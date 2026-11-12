//! Private files under the owner home. Callers choose the directory name.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
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
    let temporary = directory.join(format!(".{}", uuid::Uuid::new_v4()));
    let write = write_new(&temporary, bytes);
    if write.is_err() {
        let _ = fs::remove_file(&temporary);
        return write;
    }
    let target = directory.join(name);
    if fs::rename(&temporary, &target).is_err() {
        let _ = fs::remove_file(&temporary);
        return Err("cannot publish a private file".into());
    }
    File::open(directory)
        .and_then(|dir| dir.sync_all())
        .map_err(|_| "cannot sync a private directory".to_owned())
}

pub(super) fn read_at(path: &Path, limit: u64) -> Result<Option<Vec<u8>>, String> {
    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("cannot read a private file".into()),
    };
    let metadata = file
        .metadata()
        .map_err(|_| "cannot inspect a private file")?;
    if !metadata.is_file() || metadata.len() > limit {
        return Err("cannot read a private file".into());
    }
    let mut bytes = Vec::new();
    file.take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read a private file")?;
    if bytes.len() as u64 > limit {
        return Err("cannot read a private file".into());
    }
    Ok(Some(bytes))
}

pub(super) fn remove_file(path: &Path) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
            fs::remove_file(path).map_err(|_| "cannot remove a private file".to_owned())
        }
        Ok(_) => Err("cannot remove a private file".into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err("cannot remove a private file".into()),
    }
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|_| "cannot create a private file")?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|_| "cannot write a private file".to_owned())
}
