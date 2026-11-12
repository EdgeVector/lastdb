//! Private files for one accepted stream. Bodies are not control JSON.

use super::Error;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::Path;

const CONTROL_MAX: u64 = 65_536;
const NOFOLLOW: i32 = libc::O_NOFOLLOW | libc::O_NONBLOCK;

pub(super) fn ensure_dir(path: &Path) -> Result<(), Error> {
    if path.exists() {
        return require_dir(path);
    }
    let parent = path.parent().ok_or(Error::Unavailable)?;
    require_dir(parent)?;
    fs::DirBuilder::new()
        .mode(0o700)
        .create(path)
        .map_err(|_| Error::Unavailable)?;
    require_dir(path)?;
    sync_dir(parent)
}

pub(super) fn require_dir(path: &Path) -> Result<(), Error> {
    let info = fs::symlink_metadata(path).map_err(|_| Error::Unavailable)?;
    if !info.is_dir() || info.uid() != euid() || info.mode() & 0o077 != 0 {
        return Err(Error::Unavailable);
    }
    Ok(())
}

pub(super) fn read_json<T: DeserializeOwned>(path: &Path) -> Result<Option<T>, Error> {
    let Some(bytes) = read_limited(path, CONTROL_MAX)? else {
        return Ok(None);
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| Error::Gap)
}

pub(super) fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), Error> {
    let bytes = cloud_backup_protocol::v2::hash::canonical_bytes(value).map_err(|_| Error::Gap)?;
    if bytes.len() as u64 > CONTROL_MAX {
        return Err(Error::Gap);
    }
    write_bytes(path, &bytes)
}

pub(super) fn write_bytes(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    let parent = path.parent().ok_or(Error::Unavailable)?;
    require_dir(parent)?;
    let pending = path.with_extension("partial");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .custom_flags(NOFOLLOW)
        .open(&pending)
        .map_err(|_| Error::Unavailable)?;
    file.write_all(bytes).map_err(|_| Error::Unavailable)?;
    file.sync_all().map_err(|_| Error::Unavailable)?;
    fs::rename(&pending, path).map_err(|_| Error::Unavailable)?;
    sync_dir(parent)
}

pub(super) fn read_exact(path: &Path, max: u64) -> Result<Vec<u8>, Error> {
    read_limited(path, max)?.ok_or(Error::Gap)
}

pub(super) fn read_limited(path: &Path, max: u64) -> Result<Option<Vec<u8>>, Error> {
    let info = match fs::symlink_metadata(path) {
        Ok(info) => info,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(Error::Unavailable),
    };
    if !info.is_file()
        || info.uid() != euid()
        || info.mode() & 0o077 != 0
        || info.nlink() != 1
        || info.len() > max
    {
        return Err(Error::Gap);
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(NOFOLLOW)
        .open(path)
        .map_err(|_| Error::Unavailable)?;
    let mut bytes = Vec::new();
    file.take(max + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::Unavailable)?;
    if bytes.len() as u64 != info.len() {
        return Err(Error::Gap);
    }
    Ok(Some(bytes))
}

pub(super) fn sync_dir(path: &Path) -> Result<(), Error> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|_| Error::Unavailable)
}

pub(super) fn remove_file(path: &Path) -> Result<(), Error> {
    match fs::symlink_metadata(path) {
        Ok(info) if info.is_file() => fs::remove_file(path).map_err(|_| Error::Unavailable),
        Ok(_) => Err(Error::Gap),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(Error::Unavailable),
    }
}

pub(super) fn remove_dir(path: &Path) -> Result<(), Error> {
    match fs::symlink_metadata(path) {
        Ok(info) if info.is_dir() => fs::remove_dir_all(path).map_err(|_| Error::Unavailable),
        Ok(_) => Err(Error::Gap),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(Error::Unavailable),
    }
}

fn euid() -> u32 {
    unsafe { libc::geteuid() }
}
