//! Private files under the owner home. Callers choose the directory name.

use crate::profile::private_fs;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

pub(super) fn ensure_dir(path: &Path) -> Result<(), String> {
    fs::create_dir_all(path).map_err(|_| "cannot create a private directory")?;
    let metadata = fs::symlink_metadata(path).map_err(|_| "cannot inspect a private directory")?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err("private directory must be a real directory".into());
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|_| "cannot protect a private directory")?;
    Ok(())
}

pub(super) fn replace(directory: &Path, name: &str, bytes: &[u8]) -> Result<(), String> {
    ensure_dir(directory)?;
    if name.is_empty() || name.contains('/') || name.contains('\0') {
        return Err("cannot publish a private file".into());
    }
    private_fs::write_private(&directory.join(name), bytes)
        .map_err(|_| "cannot publish a private file".into())
}

pub(super) fn read_at(path: &Path, limit: u64) -> Result<Option<Vec<u8>>, String> {
    private_fs::read_file_limited(path, limit as usize, false)
        .map_err(|_| "cannot read a private file".into())
}

pub(super) fn remove_file(path: &Path) -> Result<(), String> {
    private_fs::remove_private(path).map_err(|_| "cannot remove a private file".into())
}
