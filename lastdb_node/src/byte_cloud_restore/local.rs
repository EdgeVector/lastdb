use super::{RestoreError, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::Path;

pub(crate) fn directory(path: &Path) -> Result<()> {
    let meta = fs::symlink_metadata(path)?;
    if !meta.is_dir() || meta.uid() != unsafe { libc::geteuid() } || meta.mode() & 0o077 != 0 {
        return Err(RestoreError::Invalid("owned session directory"));
    }
    Ok(())
}

pub(crate) fn owner_file(path: &Path, new: bool) -> Result<File> {
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

pub(crate) fn read_json<T: DeserializeOwned>(path: &Path, maximum: u64) -> Result<T> {
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

pub(crate) fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    atomic_json_bounded(
        path,
        value,
        cloud_backup_protocol::v2::limits::CONTROL_JSON_MAX_BYTES,
    )
}

pub(crate) fn atomic_json_bounded(path: &Path, value: &impl Serialize, maximum: u64) -> Result<()> {
    let bytes = cloud_backup_protocol::canonical_bytes(value)?;
    if bytes.len() as u64 > maximum {
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

pub(crate) fn writable_file(path: &Path, truncate: bool) -> Result<File> {
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

/// Recreate private runtime members that the native file inventory excludes.
pub(crate) fn prepare_native_ephemera(data: &Path) -> Result<()> {
    for name in [
        fold_db::storage::disk_pack::RUN_DIR,
        fold_db::storage::disk_pack::CAS_DIR,
    ] {
        let home = data.join(name);
        directory(&home)?;
        let runs = home.join(laststore::disk_run::home::RUNS);
        match fs::DirBuilder::new().mode(0o700).create(&runs) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        directory(&runs)?;
        let path = home.join(laststore::disk_run::home::LOCK);
        let lock = match owner_file(&path, true) {
            Ok(lock) => lock,
            Err(RestoreError::Io(error)) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                owner_file(&path, false)?
            }
            Err(error) => return Err(error),
        };
        if lock.metadata()?.len() != 0 {
            return Err(RestoreError::Invalid("run ephemeral lock"));
        }
        lock.sync_all()?;
        File::open(&home)?.sync_all()?;
    }
    Ok(())
}
