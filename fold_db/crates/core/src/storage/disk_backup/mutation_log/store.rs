//! Exact private metadata paths; every enumeration is a bounded explicit sequence.
use super::{invalid, StorageResult};
use cloud_backup_protocol::Id;
use serde::{de::DeserializeOwned, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

pub(super) const MAX_JSON: u64 = cloud_backup_protocol::v2::limits::CONTROL_JSON_MAX_BYTES;
pub(super) const MAX_EPOCHS: usize = 32;
pub(super) const MAX_ENTRIES: u64 = 131_072;

pub(super) fn epoch(root: &Path, id: &Id) -> PathBuf {
    root.join(id.as_str())
}
pub(super) fn entry(root: &Path, id: &Id, sequence: u64) -> StorageResult<PathBuf> {
    if sequence == 0 || sequence > MAX_ENTRIES {
        return Err(invalid("mutation log sequence resource bound"));
    }
    Ok(epoch(root, id).join(format!("{sequence:020}")))
}

pub(super) fn directory(path: &Path) -> StorageResult<()> {
    let info = fs::symlink_metadata(path)?;
    if !info.is_dir()
        || info.uid() != unsafe { libc::geteuid() }
        || info.mode() & 0o077 != 0
        || path.canonicalize()? != path
    {
        return Err(invalid("mutation log private directory"));
    }
    Ok(())
}

pub(super) fn create(path: &Path) -> StorageResult<()> {
    let parent = path
        .parent()
        .ok_or_else(|| invalid("mutation log directory parent"))?;
    directory(parent)?;
    fs::DirBuilder::new().mode(0o700).create(path)?;
    directory(path)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

pub(super) fn read<T: DeserializeOwned>(path: &Path) -> StorageResult<Option<T>> {
    match fs::symlink_metadata(path) {
        Ok(info) => private(&info)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    directory(
        path.parent()
            .ok_or_else(|| invalid("mutation log metadata parent"))?,
    )?;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let before = file.metadata()?;
    private(&before)?;
    let mut bytes = Vec::new();
    file.take(MAX_JSON + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 != before.len() {
        return Err(invalid("mutation log metadata length"));
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| invalid("mutation log metadata format"))
}

pub(super) fn write<T: Serialize>(path: &Path, value: &T) -> StorageResult<()> {
    let parent = path
        .parent()
        .ok_or_else(|| invalid("mutation log metadata parent"))?;
    directory(parent)?;
    let bytes = cloud_backup_protocol::v2::hash::canonical_bytes(value)
        .map_err(|_| invalid("mutation log metadata encoding"))?;
    if bytes.len() as u64 > MAX_JSON {
        return Err(invalid("mutation log metadata resource bound"));
    }
    for path in [path.to_owned(), path.with_extension("pending")] {
        match fs::symlink_metadata(path) {
            Ok(info) => private(&info)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    let pending = path.with_extension("pending");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(&pending)?;
    private(&file.metadata()?)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(pending, path)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn private(info: &fs::Metadata) -> StorageResult<()> {
    if !info.is_file()
        || info.uid() != unsafe { libc::geteuid() }
        || info.mode() & 0o077 != 0
        || info.nlink() != 1
        || info.len() > MAX_JSON
    {
        return Err(invalid("mutation log private metadata"));
    }
    Ok(())
}

/// Exact owned control file only; absent means an earlier cleanup completed this step.
pub(super) fn remove_metadata(path: &Path) -> StorageResult<()> {
    match fs::symlink_metadata(path) {
        Ok(info) => private(&info)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    }
    directory(
        path.parent()
            .ok_or_else(|| invalid("mutation log cleanup parent"))?,
    )?;
    fs::remove_file(path)?;
    Ok(())
}

pub(super) fn remove_part(path: &Path, bytes: u64) -> StorageResult<()> {
    match fs::symlink_metadata(path) {
        Ok(info) => {
            if !info.is_file()
                || info.uid() != unsafe { libc::geteuid() }
                || info.mode() & 0o077 != 0
                || info.len() != bytes
            {
                return Err(invalid("mutation log cleanup file identity"));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    }
    directory(
        path.parent()
            .ok_or_else(|| invalid("mutation log cleanup parent"))?,
    )?;
    fs::remove_file(path)?;
    Ok(())
}

pub(super) fn remove_empty(path: &Path) -> StorageResult<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => directory(path)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    }
    fs::remove_dir(path)?;
    Ok(())
}
