use super::{RestoreError, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;

pub(super) fn directory(path: &Path) -> Result<()> {
    let meta = fs::symlink_metadata(path)?;
    if !meta.is_dir() || meta.uid() != unsafe { libc::geteuid() } || meta.mode() & 0o077 != 0 {
        return Err(RestoreError::Invalid("owned session directory"));
    }
    Ok(())
}

pub(super) fn owner_file(path: &Path, new: bool) -> Result<File> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW);
    if new {
        options.create_new(true);
    }
    let file = options.open(path)?;
    validate_file(&file)?;
    Ok(file)
}

pub(super) fn read_json<T: DeserializeOwned>(path: &Path, maximum: u64) -> Result<T> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    validate_file(&file)?;
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(maximum + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > maximum {
        return Err(RestoreError::Limit("metadata bytes"));
    }
    serde_json::from_slice(&bytes).map_err(|_| RestoreError::Invalid("metadata JSON"))
}

pub(super) fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = cloud_backup_protocol::canonical_bytes(value)?;
    if bytes.len() as u64 > cloud_backup_protocol::v2::limits::CONTROL_JSON_MAX_BYTES {
        return Err(RestoreError::Limit("saved metadata"));
    }
    let pending = path.with_extension("pending");
    let mut file = writable_file(&pending, true)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(pending, path)?;
    File::open(
        path.parent()
            .ok_or(RestoreError::Invalid("metadata parent"))?,
    )?
    .sync_all()?;
    Ok(())
}

fn validate_file(file: &File) -> Result<()> {
    let meta = file.metadata()?;
    if !meta.is_file()
        || meta.uid() != unsafe { libc::geteuid() }
        || meta.mode() & 0o077 != 0
        || meta.nlink() != 1
    {
        return Err(RestoreError::Invalid("owned session file"));
    }
    Ok(())
}

pub(super) fn writable_file(path: &Path, truncate: bool) -> Result<File> {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    validate_file(&file)?;
    if truncate {
        file.set_len(0)?;
    }
    Ok(file)
}
