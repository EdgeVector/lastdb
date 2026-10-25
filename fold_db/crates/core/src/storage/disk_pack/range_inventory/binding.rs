use super::{invalid, StorageResult};
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

const MAX_CURRENT: u64 = 64 * 1024 * 1024;

pub(super) struct Binding {
    path: PathBuf,
    held: File,
    pub(super) digest: String,
}

impl Binding {
    pub(super) fn check_owner(&self, owner: &File) -> StorageResult<()> {
        let held = self.held.metadata()?;
        let owner = owner.metadata()?;
        if held.dev() != owner.dev() || held.ino() != owner.ino() {
            return Err(invalid("source owner lock changed during capture"));
        }
        Ok(())
    }

    pub(super) fn capture(path: &Path) -> StorageResult<Self> {
        let mut held = open_file(path)?;
        let bytes = read(&mut held, MAX_CURRENT)?;
        Ok(Self {
            path: path.into(),
            held,
            digest: format!("{:x}", Sha256::digest(bytes)),
        })
    }

    pub(super) fn check(&self) -> StorageResult<()> {
        let mut current = open_file(&self.path)?;
        let old = self.held.metadata()?;
        let new = current.metadata()?;
        if old.dev() != new.dev()
            || old.ino() != new.ino()
            || format!("{:x}", Sha256::digest(read(&mut current, MAX_CURRENT)?)) != self.digest
        {
            return Err(invalid("source publication changed during the inventory"));
        }
        Ok(())
    }
}

pub(super) fn open_file(path: &Path) -> StorageResult<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(invalid("source publication is not a regular file"));
    }
    Ok(file)
}

pub(super) fn read(file: &mut File, maximum: u64) -> StorageResult<Vec<u8>> {
    if file.metadata()?.len() > maximum {
        return Err(invalid("source publication exceeds its bound"));
    }
    let mut bytes = Vec::new();
    file.take(maximum + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > maximum {
        return Err(invalid("source publication exceeds its bound"));
    }
    Ok(bytes)
}
