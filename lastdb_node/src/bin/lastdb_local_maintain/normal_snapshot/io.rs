//! Bounded regular-file inputs and durable owner-only output artifacts.

use super::err;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::Path;

pub(super) fn digest(bytes: &[u8]) -> String {
    fold_db::hex::hex_lower(Sha256::digest(bytes))
}

pub(super) fn check_digest(value: &str) -> Result<(), String> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("a bound artifact SHA must be lowercase SHA-256".into());
    }
    Ok(())
}

pub(super) fn read(path: &Path, limit: u64, private: bool) -> Result<Vec<u8>, String> {
    let meta = std::fs::symlink_metadata(path).map_err(err)?;
    if !meta.is_file()
        || meta.len() > limit
        || (private
            && (meta.uid() != unsafe { libc::geteuid() } || meta.permissions().mode() & 0o077 != 0))
    {
        return Err("a snapshot input is not a bounded regular owner-only file".into());
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(err)?;
    let opened = file.metadata().map_err(err)?;
    if (meta.dev(), meta.ino()) != (opened.dev(), opened.ino()) {
        return Err("a snapshot input changed during open".into());
    }
    let mut bytes = Vec::new();
    file.take(limit + 1).read_to_end(&mut bytes).map_err(err)?;
    if bytes.len() as u64 > limit {
        return Err("a snapshot input exceeded its read limit".into());
    }
    Ok(bytes)
}

pub(super) fn bound(path: &Path, expected: &str, limit: u64) -> Result<Vec<u8>, String> {
    check_digest(expected)?;
    let bytes = read(path, limit, true)?;
    if digest(&bytes) != expected {
        return Err("a bound snapshot input changed".into());
    }
    Ok(bytes)
}

pub(super) fn absent(path: &Path) -> Result<(), String> {
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        _ => Err("a forbidden snapshot state path exists or cannot be checked".into()),
    }
}

pub(super) fn create_dir(path: &Path, home: &Path, root: &Path) -> Result<(), String> {
    absent(path)?;
    let parent = std::fs::canonicalize(path.parent().ok_or("report directory has no parent")?)
        .map_err(err)?;
    if !path.is_absolute() || parent.starts_with(home) || parent.starts_with(root) {
        return Err("snapshot artifacts must be outside the home".into());
    }
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(path)
        .map_err(err)?;
    File::open(parent)
        .and_then(|file| file.sync_all())
        .map_err(err)
}

pub(super) fn write<T: Serialize>(dir: &Path, name: &str, value: &T) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(value).map_err(err)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(dir.join(name))
        .map_err(err)?;
    file.write_all(&bytes)
        .and_then(|_| file.sync_all())
        .map_err(err)?;
    File::open(dir)
        .and_then(|file| file.sync_all())
        .map_err(err)
}
