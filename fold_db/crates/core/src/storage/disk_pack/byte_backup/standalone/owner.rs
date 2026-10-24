//! External coordinator ownership survives all active capture/read handles.
use super::{invalid, StorageResult};
use std::fs::{self, File, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

pub(super) struct Owner {
    source: PathBuf,
    state: PathBuf,
    source_identity: (u64, u64),
    state_identity: (u64, u64),
    lock: File,
}
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Binding {
    version: u16,
    source: PathBuf,
    source_device: u64,
    source_inode: u64,
    home_id: uuid::Uuid,
}
impl Owner {
    pub fn open(source: &Path, state: &Path) -> StorageResult<Self> {
        let source_identity = require_source(source)?;
        if !state.is_absolute() || state.starts_with(source) || source.starts_with(state) {
            return Err(invalid("byte standalone requires disjoint absolute state"));
        }
        let parent = state
            .parent()
            .ok_or_else(|| invalid("byte standalone state parent"))?;
        directory(parent, true)?;
        if parent.canonicalize()? != parent {
            return Err(invalid("byte standalone state parent alias"));
        }
        match fs::DirBuilder::new().mode(0o700).create(state) {
            Ok(()) => File::open(parent)?.sync_all()?,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        let state_identity = directory(state, true)?;
        if state.canonicalize()? != state {
            return Err(invalid("byte standalone state alias"));
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(state.join("owner.lock"))?;
        require_lock(&lock)?;
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        lock.sync_all()?;
        File::open(state)?.sync_all()?;
        Ok(Self {
            source: source.to_path_buf(),
            state: state.to_path_buf(),
            source_identity,
            state_identity,
            lock,
        })
    }
    pub fn state_root(&self) -> &Path {
        &self.state
    }
    pub fn binding(&self, home_id: uuid::Uuid) -> Binding {
        Binding {
            version: 1,
            source: self.source.clone(),
            home_id,
            source_device: self.source_identity.0,
            source_inode: self.source_identity.1,
        }
    }
    pub fn validate(&self) -> StorageResult<()> {
        if require_source(&self.source)? != self.source_identity
            || directory(&self.state, true)? != self.state_identity
        {
            return Err(invalid("byte standalone directory identity changed"));
        }
        require_lock(&self.lock)?;
        let actual = fs::symlink_metadata(self.state.join("owner.lock"))?;
        let held = self.lock.metadata()?;
        if actual.dev() != held.dev() || actual.ino() != held.ino() {
            return Err(invalid("byte standalone owner lock changed"));
        }
        Ok(())
    }
}
pub(super) fn require_source(path: &Path) -> StorageResult<(u64, u64)> {
    let identity = directory(path, false)?;
    if !path.is_absolute() || path.canonicalize()? != path {
        return Err(invalid("byte standalone native root alias"));
    }
    let lock = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path.join("LIFETIME_LOCK"))?;
    if !lock.metadata()?.is_file() {
        return Err(invalid("byte standalone existing lifetime lock required"));
    }
    Ok(identity)
}
fn directory(path: &Path, private: bool) -> StorageResult<(u64, u64)> {
    let meta = fs::symlink_metadata(path)?;
    let forbidden = if private { 0o077 } else { 0o022 };
    if !meta.is_dir() || meta.uid() != unsafe { libc::geteuid() } || meta.mode() & forbidden != 0 {
        return Err(invalid("byte standalone directory ownership"));
    }
    Ok((meta.dev(), meta.ino()))
}
fn require_lock(file: &File) -> StorageResult<()> {
    let meta = file.metadata()?;
    if !meta.is_file()
        || meta.uid() != unsafe { libc::geteuid() }
        || meta.nlink() != 1
        || meta.mode() & 0o077 != 0
    {
        return Err(invalid("byte standalone owner lock ownership"));
    }
    Ok(())
}
