//! Exact owned control records. No credential or temporary grant is durable.
use super::{model::Saved, BackupError, Result};
use cloud_backup_protocol::{canonical_bytes, Id};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

const MAX_JSON: u64 = 256 * 1024;
pub(super) const CONTROL_RESERVE: u64 =
    lastdb_storage_policy::backup_decode::JSON_PAGE_RESERVE_BYTES;
static WRITE: Mutex<()> = Mutex::new(());

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Active {
    request_id: Id,
}

#[derive(Clone)]
pub(super) struct Store {
    inner: Arc<Inner>,
}
struct Inner {
    base: PathBuf,
    root: PathBuf,
    bytes: AtomicU64,
    other: AtomicU64,
}

impl Store {
    pub fn base(home: &Path) -> PathBuf {
        home.join("data/native-cloud-backup-v1")
    }
    pub fn active(home: &Path) -> Result<Option<Id>> {
        Ok(read::<Active>(&Self::base(home).join("active.json"))?.map(|value| value.request_id))
    }
    pub fn open(home: &Path, id: &Id) -> Result<Self> {
        let base = Self::base(home);
        directory(&base)?;
        directory(&base.join("jobs"))?;
        let root = base.join("jobs").join(id.as_str());
        directory(&root)?;
        let bytes = count(&base, 0, &mut 0)?;
        if bytes > cloud_backup_protocol::limits::DURABLE_LOCAL_SPOOL_MAX_BYTES {
            return Err(BackupError::Limit);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                base,
                root,
                bytes: AtomicU64::new(bytes),
                other: AtomicU64::new(0),
            }),
        })
    }
    pub fn lock(home: &Path) -> Result<File> {
        let root = Self::base(home);
        directory(&root)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(root.join("owner.lock"))?;
        file.try_lock().map_err(|_| BackupError::Busy)?;
        Ok(file)
    }
    pub fn select(&self, id: &Id) -> Result<()> {
        self.write_at(
            &self.inner.base.join("active.json"),
            &Active {
                request_id: id.clone(),
            },
        )
    }
    pub fn read<T: DeserializeOwned>(&self, name: &str) -> Result<Option<T>> {
        read(&self.inner.root.join(name))
    }
    pub fn read_base<T: DeserializeOwned>(&self, name: &str) -> Result<Option<T>> {
        read(&self.inner.base.join(name))
    }
    pub fn write<T: Serialize>(&self, name: &str, value: &T) -> Result<()> {
        self.write_at(&self.inner.root.join(name), value)
    }
    pub fn write_base<T: Serialize>(&self, name: &str, value: &T) -> Result<()> {
        self.write_at(&self.inner.base.join(name), value)
    }
    pub fn state(&self) -> Result<Option<Saved>> {
        let saved: Option<Saved> = self.read("state.json")?;
        if let Some(saved) = &saved {
            saved.validate()?;
        }
        Ok(saved)
    }
    pub fn save(&self, state: &Saved) -> Result<()> {
        state.validate()?;
        self.write("state.json", state)
    }
    pub fn bytes(&self) -> u64 {
        self.inner.bytes.load(Ordering::Relaxed)
    }
    pub fn set_other_bytes(&self, bytes: u64) {
        self.inner.other.store(bytes, Ordering::Relaxed);
    }
    pub fn admit(&self, extra: u64) -> Result<()> {
        if self
            .bytes()
            .checked_add(self.inner.other.load(Ordering::Relaxed))
            .and_then(|n| n.checked_add(extra))
            .is_none_or(|n| n > cloud_backup_protocol::limits::DURABLE_LOCAL_SPOOL_MAX_BYTES)
        {
            return Err(BackupError::Limit);
        }
        Ok(())
    }
    fn write_at<T: Serialize>(&self, path: &Path, value: &T) -> Result<()> {
        let bytes = canonical_bytes(value)?;
        if bytes.len() as u64 > MAX_JSON {
            return Err(BackupError::Limit);
        }
        let _guard = WRITE.lock().map_err(|_| BackupError::Disk)?;
        self.admit(2 * bytes.len() as u64 + CONTROL_RESERVE)?;
        let old = size(path)?;
        let pending = path.with_extension("pending");
        let prior = size(&pending)?;
        let _space = laststore::disk_pack::maintenance::reserve_space(
            &self.inner.root,
            &laststore::disk_pack::maintenance::SpacePolicy::default(),
            bytes.len() as u64 + 65536,
        )
        .map_err(|_| BackupError::Disk)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&pending)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&pending, path)?;
        File::open(path.parent().ok_or(BackupError::Disk)?)?.sync_all()?;
        let next = self
            .bytes()
            .checked_sub(old + prior)
            .and_then(|n| n.checked_add(bytes.len() as u64))
            .ok_or(BackupError::Invalid)?;
        self.inner.bytes.store(next, Ordering::Relaxed);
        Ok(())
    }
}

pub(super) fn read<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match fs::symlink_metadata(path) {
        Ok(meta)
            if meta.is_file()
                && meta.uid() == unsafe { libc::geteuid() }
                && meta.mode() & 0o077 == 0
                && meta.len() <= MAX_JSON => {}
        Ok(_) => return Err(BackupError::Invalid),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let mut bytes = Vec::new();
    file.take(MAX_JSON + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_JSON {
        return Err(BackupError::Limit);
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| BackupError::Invalid)
}

/// Fixed-size consent metadata must remain writable when a job fills its spool.
/// This path never walks job directories or changes their receipt accounting.
pub(super) fn write_automatic<T: Serialize>(home: &Path, value: &T) -> Result<()> {
    let base = Store::base(home);
    directory(&base)?;
    write_fixed(&base.join("automatic.json"), value, 8192)
}

/// Only the controller's exact accepted-head proof may add endpoint provenance
/// to an already terminal legacy job. This does not select or reopen that job.
pub(super) fn write_completed_state(home: &Path, saved: &Saved) -> Result<()> {
    saved.validate()?;
    if saved.retained_source_bytes != Some(0) {
        return Err(BackupError::Invalid);
    }
    let root = Store::base(home)
        .join("jobs")
        .join(saved.request_id.as_str());
    write_fixed(&root.join("state.json"), saved, MAX_JSON as usize)
}

fn write_fixed<T: Serialize>(path: &Path, value: &T, max: usize) -> Result<()> {
    let bytes = canonical_bytes(value)?;
    if bytes.len() > max {
        return Err(BackupError::Limit);
    }
    let _guard = WRITE.lock().map_err(|_| BackupError::Disk)?;
    let base = path.parent().ok_or(BackupError::Disk)?;
    let pending = path.with_extension("pending");
    private_control(path)?;
    private_control(&pending)?;
    let _space = laststore::disk_pack::maintenance::reserve_space(
        base,
        &laststore::disk_pack::maintenance::SpacePolicy::default(),
        2 * max as u64 + 65536,
    )
    .map_err(|_| BackupError::Disk)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&pending)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(pending, path)?;
    File::open(base)?.sync_all()?;
    File::open(base.parent().ok_or(BackupError::Disk)?)?.sync_all()?;
    Ok(())
}
fn private_control(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta)
            if meta.is_file()
                && meta.uid() == unsafe { libc::geteuid() }
                && meta.mode() & 0o077 == 0
                && meta.nlink() == 1 =>
        {
            Ok(())
        }
        Ok(_) => Err(BackupError::Invalid),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}
fn directory(path: &Path) -> Result<()> {
    match fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => {
            File::open(path.parent().ok_or(BackupError::Disk)?)?.sync_all()?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    let meta = fs::symlink_metadata(path)?;
    if !meta.is_dir() || meta.uid() != unsafe { libc::geteuid() } || meta.mode() & 0o077 != 0 {
        return Err(BackupError::Invalid);
    }
    Ok(())
}
fn size(path: &Path) -> Result<u64> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.is_file() => Ok(meta.len()),
        Ok(_) => Err(BackupError::Invalid),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error.into()),
    }
}
fn count(path: &Path, depth: u8, entries: &mut u64) -> Result<u64> {
    if depth > 3 {
        return Err(BackupError::Invalid);
    }
    let mut bytes = 0u64;
    for entry in fs::read_dir(path)? {
        *entries += 1;
        if *entries > 524_288 {
            return Err(BackupError::Limit);
        }
        let entry = entry?;
        let meta = fs::symlink_metadata(entry.path())?;
        let extra = if meta.is_dir() {
            count(&entry.path(), depth + 1, entries)?
        } else if meta.is_file() {
            meta.len()
        } else {
            return Err(BackupError::Invalid);
        };
        bytes = bytes.checked_add(extra).ok_or(BackupError::Limit)?;
        if bytes > cloud_backup_protocol::limits::DURABLE_LOCAL_SPOOL_MAX_BYTES {
            return Err(BackupError::Limit);
        }
    }
    Ok(bytes)
}
