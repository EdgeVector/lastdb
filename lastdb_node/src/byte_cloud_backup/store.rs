//! Private, bounded V2 checkpoints and automatic job retirement.
use super::{source::Saved, BackupError, Result};
use crate::cloud::{
    is_owned_dir, is_private_file, open_private_path, read_private_path, PrivatePathError,
};
use cloud_backup_protocol::{
    limits::DURABLE_LOCAL_SPOOL_MAX_BYTES, v2::hash::canonical_bytes,
    v2::limits::CONTROL_JSON_MAX_BYTES, Id,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

pub(crate) const AUTOMATIC_FILE: &str = "automatic.json";
const MAX_JSON: u64 = CONTROL_JSON_MAX_BYTES;
static WRITE: Mutex<()> = Mutex::new(());
const EXACT: &[&str] = &[
    "state",
    "terminal",
    "recovery",
    "pause",
    "log-plan",
    "inventory-index",
    "close",
    "blobs",
];
const PREFIXES: &[&str] = &[
    "log-object-",
    "log-file-",
    "log-payload-",
    "inventory-page-",
    "blob-upload-",
    "blob-uploaded-",
    "blob-release-",
];

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Active {
    request_id: Id,
}

#[derive(Clone)]
pub(super) struct Store {
    pub base: PathBuf,
    pub root: PathBuf,
    id: Id,
    bytes: Arc<AtomicU64>,
}

fn map_priv(err: PrivatePathError) -> BackupError {
    match err {
        PrivatePathError::Io(e) => e.into(),
        PrivatePathError::Invalid => BackupError::Invalid,
        PrivatePathError::Limit => BackupError::Limit,
    }
}

pub(super) fn reserved(id: &Id) -> bool {
    id.as_str().starts_with("a17dbac2-")
}

pub(super) fn request_id() -> Result<Id> {
    let mut bytes = *uuid::Uuid::new_v4().as_bytes();
    bytes[..4].copy_from_slice(&[0xa1, 0x7d, 0xba, 0xc2]);
    Ok(Id::try_from(uuid::Uuid::from_bytes(bytes).to_string())?)
}

impl Store {
    pub fn base(home: &Path) -> PathBuf {
        home.join("data/cloud-byte-backup-v2")
    }
    pub fn publication_base(home: &Path) -> PathBuf {
        home.join("data/cloud-share-backup-v2")
    }
    pub fn write_config<T: Serialize>(base: &Path, name: &str, value: &T) -> Result<()> {
        directory(base)?;
        let path = base.join(name);
        write(&path, value, &AtomicU64::new(pair_size(&path)?))
    }
    pub fn active(base: &Path) -> Result<Option<Id>> {
        Ok(read::<Active>(&base.join("active.json"))?.map(|a| a.request_id))
    }
    pub fn has_active(base: &Path) -> Result<bool> {
        let Some(id) = Self::active(base)? else {
            return Ok(false);
        };
        Ok(!state(base, &id)?.ok_or(BackupError::Invalid)?.terminal())
    }
    pub fn open(base: &Path, id: &Id) -> Result<Self> {
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
    pub fn lock(base: &Path) -> Result<File> {
        directory(base)?;
        let f =
            open_private_path(&base.join("owner.lock"), true, false, false).map_err(map_priv)?;
        f.try_lock().map(|()| f).map_err(|_| BackupError::Busy)
    }
    pub fn acquire_job(base: &Path, id: &Id) -> Result<(Self, File)> {
        let permit = Self::lock(base)?;
        if let Some(prev) = Self::active(base)?.filter(|p| p != id) {
            if !state(base, &prev)?.ok_or(BackupError::Invalid)?.terminal() {
                return Err(BackupError::Busy);
            }
        }
        Ok((Self::open(base, id)?, permit))
    }
    pub fn reopen(&self) -> Result<Self> {
        Self::open(&self.base, &self.id)
    }
    pub fn select(&self, id: &Id) -> Result<()> {
        (id == &self.id)
            .then_some(())
            .ok_or(BackupError::Identity)?;
        let act = Active {
            request_id: id.clone(),
        };
        write(&self.base.join("active.json"), &act, &self.bytes)
    }
    pub fn read<T: DeserializeOwned>(&self, name: &str) -> Result<Option<T>> {
        read(&self.root.join(name))
    }
    pub fn write<T: Serialize>(&self, name: &str, value: &T) -> Result<()> {
        write(&self.root.join(name), value, &self.bytes)
    }
    pub fn raw(&self, name: &str) -> Result<Option<Vec<u8>>> {
        read_private_path(&self.root.join(name), MAX_JSON + 29).map_err(map_priv)
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

pub(super) fn state(base: &Path, id: &Id) -> Result<Option<Saved>> {
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

pub(super) fn close(base: &Path, id: &Id) -> Result<()> {
    let root = base.join("jobs").join(id.as_str());
    is_owned_dir(&fs::symlink_metadata(&root)?)
        .then_some(())
        .ok_or(BackupError::Invalid)?;
    let path = root.join("close.json");
    write(&path, id, &AtomicU64::new(pair_size(&path)?))
}

pub(super) fn read<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    read_private_path(path, MAX_JSON + 29)
        .map_err(map_priv)?
        .map(|b| serde_json::from_slice(&b).map_err(|_| BackupError::Invalid))
        .transpose()
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
    if bytes.len() as u64 > MAX_JSON + 29 {
        return Err(BackupError::Limit);
    }
    let _guard = WRITE.lock().map_err(|_| BackupError::Disk)?;
    if used
        .load(Ordering::Relaxed)
        .checked_add(reserve)
        .is_none_or(|n| n > DURABLE_LOCAL_SPOOL_MAX_BYTES)
    {
        return Err(BackupError::Limit);
    }
    let parent = path.parent().ok_or(BackupError::Disk)?;
    let (pending, previous) = (path.with_extension("pending"), pair_size(path)?);
    let _space =
        laststore::space::reserve_space(parent, &laststore::space::SpacePolicy::default(), reserve)
            .map_err(|_| BackupError::Disk)?;
    let mut file = open_private_path(&pending, true, false, true).map_err(map_priv)?;
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

fn size(path: &Path) -> Result<u64> {
    match fs::symlink_metadata(path) {
        Ok(info) if is_private_file(&info) => Ok(info.len()),
        Ok(_) => Err(BackupError::Invalid),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(e.into()),
    }
}

fn pair_size(path: &Path) -> Result<u64> {
    size(path)?
        .checked_add(size(&path.with_extension("pending"))?)
        .ok_or(BackupError::Limit)
}

fn remove_owned(path: &Path) -> Result<()> {
    if size(path)? > 0 || fs::symlink_metadata(path).is_ok() {
        fs::remove_file(path)?;
    }
    Ok(())
}

fn directory(path: &Path) -> Result<()> {
    match fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => File::open(path.parent().ok_or(BackupError::Disk)?)?.sync_all()?,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.into()),
    }
    is_owned_dir(&fs::symlink_metadata(path)?)
        .then_some(())
        .ok_or(BackupError::Invalid)
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
            is_owned_dir(&info)
                .then_some(())
                .ok_or(BackupError::Invalid)?;
            count(&p, depth + 1, entries)?
        } else if is_private_file(&info) {
            info.len()
        } else {
            return Err(BackupError::Invalid);
        };
        bytes = bytes
            .checked_add(extra)
            .filter(|&n| n <= DURABLE_LOCAL_SPOOL_MAX_BYTES)
            .ok_or(BackupError::Limit)?;
    }
    Ok(bytes)
}

fn allowed_job_file(name: &str) -> bool {
    matches!(name.rsplit_once('.'), Some((s, "json" | "pending")) if EXACT.contains(&s))
        || PREFIXES.iter().any(|p| {
            name.strip_prefix(p)
                .and_then(|t| t.split_once('.'))
                .is_some_and(|(d, ext)| {
                    (1..=8).contains(&d.len())
                        && d.as_bytes().iter().all(u8::is_ascii_digit)
                        && matches!(ext, "json" | "pending" | "enc")
                })
        })
}

pub(super) fn retire_cleanup(base: &Path, cleanup: &mut Option<Id>) -> Result<bool> {
    let Some(id) = cleanup.clone() else {
        return Ok(false);
    };
    if reserved(&id) && Store::active(base)?.as_ref() == Some(&id) {
        return Ok(false);
    }
    if reserved(&id) {
        retire_at(base, &id)?;
    }
    *cleanup = None;
    Ok(true)
}

fn retire_at(base: &Path, id: &Id) -> Result<()> {
    let root = base.join("jobs").join(id.as_str());
    let (previous, active) = (state(base, id)?, Store::active(base)?);
    let remove_root = || -> Result<()> {
        if let Err(e) = fs::remove_dir(&root) {
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(e.into());
            }
        }
        Ok(File::open(root.parent().ok_or(BackupError::Invalid)?)?.sync_all()?)
    };
    if previous.is_none() && active.as_ref() != Some(id) {
        return remove_root();
    }
    let saved = previous.ok_or(BackupError::Missing)?;
    if saved.automatic_revision.is_none() || !saved.terminal() || active.as_ref() == Some(id) {
        return Err(BackupError::Identity);
    }
    let mut files = Vec::new();
    for entry in fs::read_dir(&root)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .filter(|s| allowed_job_file(s))
            .ok_or(BackupError::Invalid)?;
        if files.len() >= 32768 {
            return Err(BackupError::Limit);
        }
        size(&entry.path())?;
        if !matches!(name, "state.json" | "terminal.json") {
            files.push(entry.path());
        }
    }
    for path in files {
        remove_owned(&path)?;
    }
    for name in ["terminal.json", "state.json"] {
        File::open(&root)?.sync_all()?;
        remove_owned(&root.join(name))?;
    }
    File::open(&root)?.sync_all()?;
    remove_root()
}
