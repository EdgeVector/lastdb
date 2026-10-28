//! Existing owner locks for backup; normal opens retain their creation path.
use crate::{Error, Result};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;

pub(super) fn open(root: &Path, create: bool) -> Result<File> {
    let mut options = OpenOptions::new();
    options
        .create(create)
        .truncate(false)
        .write(true)
        .mode(0o600);
    if !create {
        options.custom_flags(
            rustix::fs::OFlags::NOFOLLOW.bits() as i32 | rustix::fs::OFlags::NONBLOCK.bits() as i32,
        );
    }
    let lock = options.open(root.join("LOCK"))?;
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
        Ok(()) => Ok(lock),
        Err(error) if error == rustix::io::Errno::WOULDBLOCK => Err(Error::Config(
            "disk run home: another owner holds the home".into(),
        )),
        Err(error) => Err(std::io::Error::from(error).into()),
    }
}
