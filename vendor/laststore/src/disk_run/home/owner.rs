//! Existing owner locks for backup; normal opens retain their creation path.
use crate::{Error, Result};
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;

pub(super) fn open(root: &Path, create: bool) -> Result<File> {
    let mut options = OpenOptions::new();
    options
        .create(create)
        .truncate(false)
        .write(true)
        .mode(0o600)
        .custom_flags(
            rustix::fs::OFlags::NOFOLLOW.bits() as i32 | rustix::fs::OFlags::NONBLOCK.bits() as i32,
        );
    let lock = options.open(root.join(super::LOCK))?;
    if !create {
        let info = lock.metadata()?;
        if !info.is_file()
            || info.uid() != rustix::process::geteuid().as_raw()
            || info.mode() & 0o077 != 0
            || info.len() != 0
            || info.nlink() != 1
        {
            return Err(Error::Config(
                "backup owner lock is not an existing private file".into(),
            ));
        }
    }
    match rustix::fs::flock(&lock, rustix::fs::FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => {
            require_path(root, &lock)?;
            Ok(lock)
        }
        Err(error) if error == rustix::io::Errno::WOULDBLOCK => Err(Error::Config(
            "disk run home: another owner holds the home".into(),
        )),
        Err(error) => Err(std::io::Error::from(error).into()),
    }
}

pub(super) fn require_path(root: &Path, lock: &File) -> Result<()> {
    // A returning-home exchange can move an already opened lock before flock.
    // Never use that archived inode to guard CURRENT at the new path.
    let held = lock.metadata()?;
    let current = fs::symlink_metadata(root.join(super::LOCK))?;
    if !current.is_file() || (held.dev(), held.ino()) != (current.dev(), current.ino()) {
        return Err(Error::Config("disk run home: owner lock moved".into()));
    }
    Ok(())
}

impl super::RunHome {
    /// Keep the owner lock after the run home closes and its merger stops.
    pub fn retain_owner_lock(&self) -> Result<std::sync::Arc<File>> {
        require_path(&self.root, &self.lock)?;
        Ok(std::sync::Arc::clone(&self.lock))
    }
}
