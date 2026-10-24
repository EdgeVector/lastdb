//! Private, bounded V2 checkpoints. V1 state and temporary grants stay separate.
use super::{model::Saved, BackupError, Result};
use cloud_backup_protocol::Id;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

const MAX_JSON: u64 = cloud_backup_protocol::v2::limits::CONTROL_JSON_MAX_BYTES;
static WRITE: Mutex<()> = Mutex::new(());

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Active {
    request_id: Id,
}

#[derive(Clone)]
pub(super) struct Store {
    base: PathBuf,
    root: PathBuf,
    id: Id,
    bytes: Arc<AtomicU64>,
}
impl Store {
    pub fn base(home: &Path) -> PathBuf {
        home.join("data/cloud-byte-backup-v2")
    }
    pub fn active(home: &Path) -> Result<Option<Id>> {
        Self::active_at(&Self::base(home))
    }
    pub fn active_at(base: &Path) -> Result<Option<Id>> {
        Ok(read::<Active>(&base.join("active.json"))?.map(|a| a.request_id))
    }
    pub fn open(home: &Path, id: &Id) -> Result<Self> {
        Self::open_at(&Self::base(home), id)
    }
    pub fn open_at(base: &Path, id: &Id) -> Result<Self> {
        directory(base)?;
        directory(&base.join("jobs"))?;
        let root = base.join("jobs").join(id.as_str());
        directory(&root)?;
        let bytes = Arc::new(AtomicU64::new(count(base, 0, &mut 0)?));
        Ok(Self {
            base: base.to_path_buf(),
            root,
            id: id.clone(),
            bytes,
        })
    }
    pub fn lock(home: &Path) -> Result<File> {
        Self::lock_at(&Self::base(home))
    }
    pub fn lock_at(base: &Path) -> Result<File> {
        directory(base)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(base.join("owner.lock"))?;
        private_file(&file.metadata()?)?;
        file.try_lock().map_err(|_| BackupError::Busy)?;
        Ok(file)
    }
    pub fn reopen(&self) -> Result<Self> {
        Self::open_at(&self.base, &self.id)
    }
    pub fn select(&self, id: &Id) -> Result<()> {
        if id != &self.id {
            return Err(BackupError::Identity);
        }
        write(
            &self.base.join("active.json"),
            &Active {
                request_id: id.clone(),
            },
            &self.bytes,
        )
    }
    pub fn read<T: DeserializeOwned>(&self, name: &str) -> Result<Option<T>> {
        read(&self.root.join(name))
    }
    pub fn write<T: Serialize>(&self, name: &str, value: &T) -> Result<()> {
        write(&self.root.join(name), value, &self.bytes)
    }
    pub fn read_base<T: DeserializeOwned>(&self, name: &str) -> Result<Option<T>> {
        read(&self.base.join(name))
    }
    pub fn write_base<T: Serialize>(&self, name: &str, value: &T) -> Result<()> {
        write(&self.base.join(name), value, &self.bytes)
    }
    pub fn state(&self) -> Result<Option<Saved>> {
        read_state_at(&self.root.join("state.json"), &self.id)
    }
    pub fn save(&self, value: &Saved) -> Result<()> {
        value.validate()?;
        if value.request_id != self.id {
            return Err(BackupError::Identity);
        }
        self.write("state.json", value)
    }
    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }
}

pub(super) fn state(home: &Path, id: &Id) -> Result<Option<Saved>> {
    state_at(&Store::base(home), id)
}

pub(super) fn state_at(base: &Path, id: &Id) -> Result<Option<Saved>> {
    read_state_at(&base.join("jobs").join(id.as_str()).join("state.json"), id)
}

fn read_state_at(path: &Path, id: &Id) -> Result<Option<Saved>> {
    let value: Option<Saved> = read(path)?;
    if let Some(saved) = &value {
        saved.validate()?;
        if &saved.request_id != id {
            return Err(BackupError::Identity);
        }
    }
    Ok(value)
}

/// A bounded close marker remains writable without a job-directory walk.
pub(super) fn close(home: &Path, id: &Id) -> Result<()> {
    close_at(&Store::base(home), id)
}

pub(super) fn close_at(base: &Path, id: &Id) -> Result<()> {
    let root = base.join("jobs").join(id.as_str());
    let info = fs::symlink_metadata(&root)?;
    if !info.is_dir() || info.uid() != unsafe { libc::geteuid() } || info.mode() & 0o077 != 0 {
        return Err(BackupError::Invalid);
    }
    let path = root.join("close.json");
    let previous = size(&path)?
        .checked_add(size(&path.with_extension("pending"))?)
        .ok_or(BackupError::Limit)?;
    write(&path, id, &AtomicU64::new(previous))
}

pub(super) fn read<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match fs::symlink_metadata(path) {
        Ok(info) => {
            private_file(&info)?;
            if info.len() > MAX_JSON {
                return Err(BackupError::Limit);
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    private_file(&file.metadata()?)?;
    let mut bytes = Vec::new();
    file.take(MAX_JSON + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_JSON {
        return Err(BackupError::Limit);
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| BackupError::Invalid)
}

fn write<T: Serialize>(path: &Path, value: &T, used: &AtomicU64) -> Result<()> {
    let bytes = cloud_backup_protocol::v2::hash::canonical_bytes(value)?;
    if bytes.len() as u64 > MAX_JSON {
        return Err(BackupError::Limit);
    }
    let _guard = WRITE.lock().map_err(|_| BackupError::Disk)?;
    if used
        .load(Ordering::Relaxed)
        .checked_add(2 * MAX_JSON + 65536)
        .is_none_or(|n| n > cloud_backup_protocol::limits::DURABLE_LOCAL_SPOOL_MAX_BYTES)
    {
        return Err(BackupError::Limit);
    }
    let parent = path.parent().ok_or(BackupError::Disk)?;
    let pending = path.with_extension("pending");
    let previous = size(path)?
        .checked_add(size(&pending)?)
        .ok_or(BackupError::Limit)?;
    for file in [path, &pending] {
        match fs::symlink_metadata(file) {
            Ok(info) => private_file(&info)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    let _space = laststore::disk_pack::maintenance::reserve_space(
        parent,
        &laststore::disk_pack::maintenance::SpacePolicy::default(),
        2 * MAX_JSON + 65536,
    )
    .map_err(|_| BackupError::Disk)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&pending)?;
    private_file(&file.metadata()?)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&pending, path)?;
    File::open(parent)?.sync_all()?;
    let next = used
        .load(Ordering::Relaxed)
        .checked_sub(previous)
        .and_then(|n| n.checked_add(bytes.len() as u64))
        .ok_or(BackupError::Invalid)?;
    used.store(next, Ordering::Relaxed);
    Ok(())
}

fn size(path: &Path) -> Result<u64> {
    match fs::symlink_metadata(path) {
        Ok(info) => {
            private_file(&info)?;
            Ok(info.len())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error.into()),
    }
}

fn private_file(info: &fs::Metadata) -> Result<()> {
    if !info.is_file()
        || info.uid() != unsafe { libc::geteuid() }
        || info.mode() & 0o077 != 0
        || info.nlink() != 1
    {
        return Err(BackupError::Invalid);
    }
    Ok(())
}

fn directory(path: &Path) -> Result<()> {
    match fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => File::open(path.parent().ok_or(BackupError::Disk)?)?.sync_all()?,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    let info = fs::symlink_metadata(path)?;
    if !info.is_dir() || info.uid() != unsafe { libc::geteuid() } || info.mode() & 0o077 != 0 {
        return Err(BackupError::Invalid);
    }
    Ok(())
}

fn count(path: &Path, depth: u8, entries: &mut u64) -> Result<u64> {
    if depth > 3 {
        return Err(BackupError::Limit);
    }
    let mut bytes = 0u64;
    for entry in fs::read_dir(path)? {
        *entries = entries.checked_add(1).ok_or(BackupError::Limit)?;
        if *entries > 524_288 {
            return Err(BackupError::Limit);
        }
        let path = entry?.path();
        let info = fs::symlink_metadata(&path)?;
        let extra = if info.is_dir() {
            if info.uid() != unsafe { libc::geteuid() } || info.mode() & 0o077 != 0 {
                return Err(BackupError::Invalid);
            }
            count(&path, depth + 1, entries)?
        } else {
            private_file(&info)?;
            info.len()
        };
        bytes = bytes.checked_add(extra).ok_or(BackupError::Limit)?;
        if bytes > cloud_backup_protocol::limits::DURABLE_LOCAL_SPOOL_MAX_BYTES {
            return Err(BackupError::Limit);
        }
    }
    Ok(bytes)
}
