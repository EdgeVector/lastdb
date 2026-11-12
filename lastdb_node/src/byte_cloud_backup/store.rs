//! Private, bounded V2 checkpoints.
use super::{model::Saved, BackupError, Result};
use crate::offline_home::{is_owned_dir, is_private_file};
use cloud_backup_protocol::{
    limits::DURABLE_LOCAL_SPOOL_MAX_BYTES, v2::hash::canonical_bytes,
    v2::limits::CONTROL_JSON_MAX_BYTES, Id,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

pub(crate) const AUTOMATIC_FILE: &str = "automatic.json";
const MAX_JSON: u64 = CONTROL_JSON_MAX_BYTES;
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
    pub fn publication_base(home: &Path) -> PathBuf {
        home.join("data/cloud-share-backup-v2")
    }
    pub fn write_configuration_at<T: Serialize>(base: &Path, name: &str, value: &T) -> Result<()> {
        directory(base)?;
        let path = base.join(name);
        write(&path, value, &AtomicU64::new(pair_size(&path)?))
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
        let jobs = base.join("jobs");
        let root = jobs.join(id.as_str());
        for dir in [base, jobs.as_path(), root.as_path()] {
            directory(dir)?;
        }
        Ok(Self {
            base: base.to_path_buf(),
            root,
            id: id.clone(),
            bytes: Arc::new(AtomicU64::new(count(base, 0, &mut 0)?)),
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
        (id == &self.id)
            .then_some(())
            .ok_or(BackupError::Identity)?;
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
    pub fn raw(&self, name: &str) -> Result<Option<Vec<u8>>> {
        read_raw(&self.root.join(name))
    }
    pub fn write_raw(&self, name: &str, bytes: &[u8]) -> Result<()> {
        write_bytes(&self.root.join(name), bytes, &self.bytes)
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
        (value.request_id == self.id)
            .then_some(())
            .ok_or(BackupError::Identity)?;
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
    if let Some(s) = &value {
        s.validate()?;
        (&s.request_id == id)
            .then_some(())
            .ok_or(BackupError::Identity)?;
    }
    Ok(value)
}

pub(super) fn close_at(base: &Path, id: &Id) -> Result<()> {
    let root = base.join("jobs").join(id.as_str());
    owned_dir(&fs::symlink_metadata(&root)?)?;
    let path = root.join("close.json");
    write(&path, id, &AtomicU64::new(pair_size(&path)?))
}

pub(super) fn read<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    read_raw(path)?
        .map(|b| serde_json::from_slice(&b).map_err(|_| BackupError::Invalid))
        .transpose()
}

fn read_raw(path: &Path) -> Result<Option<Vec<u8>>> {
    let len = size(path)?;
    if len == 0 && !path.try_exists()? {
        return Ok(None);
    }
    if len > MAX_JSON + 29 {
        return Err(BackupError::Limit);
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    private_file(&file.metadata()?)?;
    let mut bytes = Vec::new();
    file.take(MAX_JSON + 30).read_to_end(&mut bytes)?;
    (bytes.len() as u64 <= MAX_JSON + 29)
        .then_some(Some(bytes))
        .ok_or(BackupError::Limit)
}

fn write<T: Serialize>(path: &Path, value: &T, used: &AtomicU64) -> Result<()> {
    let bytes = canonical_bytes(value)?;
    (bytes.len() as u64 <= MAX_JSON)
        .then_some(())
        .ok_or(BackupError::Limit)
        .and_then(|()| write_bytes(path, &bytes, used))
}

fn write_bytes(path: &Path, bytes: &[u8], used: &AtomicU64) -> Result<()> {
    let reserve = 2 * MAX_JSON + 65536;
    (bytes.len() as u64 <= MAX_JSON + 29)
        .then_some(())
        .ok_or(BackupError::Limit)?;
    let _guard = WRITE.lock().map_err(|_| BackupError::Disk)?;
    if used
        .load(Ordering::Relaxed)
        .checked_add(reserve)
        .is_none_or(|n| n > DURABLE_LOCAL_SPOOL_MAX_BYTES)
    {
        return Err(BackupError::Limit);
    }
    let parent = path.parent().ok_or(BackupError::Disk)?;
    let pending = path.with_extension("pending");
    let previous = pair_size(path)?;
    let _space =
        laststore::space::reserve_space(parent, &laststore::space::SpacePolicy::default(), reserve)
            .map_err(|_| BackupError::Disk)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&pending)?;
    private_file(&file.metadata()?)?;
    file.write_all(bytes)?;
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

pub(super) fn size(path: &Path) -> Result<u64> {
    match fs::symlink_metadata(path) {
        Ok(info) => private_file(&info).map(|()| info.len()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(e.into()),
    }
}

fn pair_size(path: &Path) -> Result<u64> {
    size(path)?
        .checked_add(size(&path.with_extension("pending"))?)
        .ok_or(BackupError::Limit)
}

pub(super) fn remove_owned(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(info) => private_file(&info).and_then(|()| Ok(fs::remove_file(path)?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

fn owned_dir(info: &fs::Metadata) -> Result<()> {
    is_owned_dir(info).then_some(()).ok_or(BackupError::Invalid)
}

fn private_file(info: &fs::Metadata) -> Result<()> {
    is_private_file(info)
        .then_some(())
        .ok_or(BackupError::Invalid)
}

fn directory(path: &Path) -> Result<()> {
    match fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => File::open(path.parent().ok_or(BackupError::Disk)?)?.sync_all()?,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.into()),
    }
    owned_dir(&fs::symlink_metadata(path)?)
}

fn count(path: &Path, depth: u8, entries: &mut u64) -> Result<u64> {
    (depth <= 3).then_some(()).ok_or(BackupError::Limit)?;
    let mut bytes = 0u64;
    for entry in fs::read_dir(path)? {
        *entries = entries
            .checked_add(1)
            .filter(|&n| n <= 524_288)
            .ok_or(BackupError::Limit)?;
        let p = entry?.path();
        let info = fs::symlink_metadata(&p)?;
        let extra = if info.is_dir() {
            owned_dir(&info).and_then(|()| count(&p, depth + 1, entries))?
        } else {
            private_file(&info).map(|()| info.len())?
        };
        bytes = bytes
            .checked_add(extra)
            .filter(|&n| n <= DURABLE_LOCAL_SPOOL_MAX_BYTES)
            .ok_or(BackupError::Limit)?;
    }
    Ok(bytes)
}
