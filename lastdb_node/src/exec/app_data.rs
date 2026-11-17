//! The folder an app grant may receive a held file into.
//!
//! The launcher names it when it registers the app. A grant without one
//! writes nothing. The path is part of the grant and is stored with it.

use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use super::reply::HostError;

const MAX_DATA_LEN: usize = 4096;

/// The canonical data folder for `app`, or why `data` cannot be used.
pub(in crate::exec) fn accept(home: &Path, app: &str, data: &str) -> Result<PathBuf, HostError> {
    let path = Path::new(data);
    if data.is_empty() || data.len() > MAX_DATA_LEN || !path.is_absolute() {
        return Err(HostError::new(
            400,
            "the app data folder must be an absolute path",
        ));
    }
    if !real_dir(path) {
        return Err(HostError::new(
            400,
            "the app data folder must be a directory and not a symlink",
        ));
    }
    let canonical = fs::canonicalize(path).map_err(|_| {
        HostError::new(
            400,
            "the app data folder must be a directory and not a symlink",
        )
    })?;
    if !is_app_data(&canonical, app) {
        return Err(HostError::new(
            400,
            "the app data folder must be that app's data directory",
        ));
    }
    outside_home(&canonical, home)?;
    Ok(canonical)
}

/// The export path when a regular file of `size` is already there. Creates nothing.
pub(in crate::exec) fn existing_export(
    data: &Path,
    home: &Path,
    hash: &str,
    size: u64,
) -> Option<PathBuf> {
    let data = checked_data(data, home).ok()?;
    let dest = export_path(&data, hash).ok()?;
    let files = dest.parent()?;
    if !real_dir(files) {
        return None;
    }
    let meta = regular_file(&dest)?;
    (meta.len() == size).then_some(dest)
}

/// Write `bytes` to `<data>/files/<hash>`. A regular file of the same size is reused.
pub(in crate::exec) fn write_export(
    data: &Path,
    home: &Path,
    hash: &str,
    bytes: &[u8],
) -> Result<PathBuf, HostError> {
    let data = checked_data(data, home)?;
    let dest = export_path(&data, hash)?;
    let files = ensure_files(&data)?;
    if dest.parent() != Some(files.as_path()) {
        return Err(HostError::internal("file export left the app data folder"));
    }
    if regular_file(&dest).is_some_and(|meta| meta.len() == bytes.len() as u64) {
        return Ok(dest);
    }
    write_new(&files, &dest, hash, bytes)
}

fn real_dir(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok_and(|meta| meta.is_dir() && !meta.file_type().is_symlink())
}

fn is_app_data(path: &Path, app: &str) -> bool {
    path.file_name().and_then(|name| name.to_str()) == Some("data")
        && path
            .parent()
            .and_then(|parent| parent.file_name())
            .and_then(|name| name.to_str())
            == Some(app)
        && crate::profile::valid_label_64(app)
}

fn outside_home(data: &Path, home: &Path) -> Result<(), HostError> {
    let home = fs::canonicalize(home)
        .map_err(|_| HostError::internal("the LastDB home is not a directory"))?;
    if data == home || data.starts_with(&home) {
        return Err(HostError::new(
            400,
            "the app data folder must stay outside the LastDB home",
        ));
    }
    Ok(())
}

fn checked_data(data: &Path, home: &Path) -> Result<PathBuf, HostError> {
    if !real_dir(data) {
        return Err(HostError::internal(
            "the app data folder must be a directory and not a symlink",
        ));
    }
    let canonical = fs::canonicalize(data).map_err(|_| {
        HostError::internal("the app data folder must be a directory and not a symlink")
    })?;
    if canonical != data {
        return Err(HostError::internal(
            "the app data folder must be a directory and not a symlink",
        ));
    }
    outside_home(&canonical, home).map_err(|error| match error.status {
        400 => HostError::internal(error.message),
        _ => error,
    })?;
    Ok(canonical)
}

fn export_path(data: &Path, hash: &str) -> Result<PathBuf, HostError> {
    if !valid_hash(hash) {
        return Err(HostError::internal("record file hash is not 64 hex digits"));
    }
    let dest = data.join("files").join(hash);
    let rest = Path::new("files").join(hash);
    if dest.strip_prefix(data).ok() != Some(rest.as_path()) {
        return Err(HostError::internal("file export left the app data folder"));
    }
    Ok(dest)
}

fn valid_hash(hash: &str) -> bool {
    hash.len() == 64
        && hash
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn ensure_files(data: &Path) -> Result<PathBuf, HostError> {
    let files = data.join("files");
    match fs::symlink_metadata(&files) {
        Ok(meta) if meta.is_dir() && !meta.file_type().is_symlink() => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let mut builder = fs::DirBuilder::new();
            builder.mode(0o700);
            builder.create(&files).map_err(|error| {
                HostError::internal(format!(
                    "could not create the app files folder: {}",
                    error.kind()
                ))
            })?;
        }
        _ => {
            return Err(HostError::internal(
                "the app files folder must be a directory and not a symlink",
            ));
        }
    }
    if !real_dir(&files) {
        return Err(HostError::internal(
            "the app files folder must be a directory and not a symlink",
        ));
    }
    open_dir(&files)?
        .set_permissions(fs::Permissions::from_mode(0o700))
        .map_err(|error| {
            HostError::internal(format!(
                "could not protect the app files folder: {}",
                error.kind()
            ))
        })?;
    if !real_dir(&files) {
        return Err(HostError::internal(
            "the app files folder must be a directory and not a symlink",
        ));
    }
    Ok(files)
}

fn write_new(files: &Path, dest: &Path, hash: &str, bytes: &[u8]) -> Result<PathBuf, HostError> {
    let temporary = files.join(format!(".{hash}.{}.tmp", uuid::Uuid::new_v4()));
    if temporary.parent() != Some(files) {
        return Err(HostError::internal("file export left the app data folder"));
    }
    let write = (|| -> io::Result<()> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&temporary)?;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
        file.write_all(bytes)?;
        file.sync_all()
    })();
    if let Err(error) = write {
        let _ = fs::remove_file(&temporary);
        return Err(HostError::internal(format!(
            "could not write the record file: {}",
            error.kind()
        )));
    }
    if regular_file(dest).is_some_and(|meta| meta.len() == bytes.len() as u64) {
        let _ = fs::remove_file(&temporary);
        return Ok(dest.to_path_buf());
    }
    if let Err(error) = fs::rename(&temporary, dest) {
        let _ = fs::remove_file(&temporary);
        return Err(HostError::internal(format!(
            "could not place the record file: {}",
            error.kind()
        )));
    }
    let placed = regular_file(dest).is_some_and(|meta| meta.len() == bytes.len() as u64);
    if !placed {
        let _ = fs::remove_file(dest);
        return Err(HostError::internal(
            "the record file was not a regular file after it was placed",
        ));
    }
    sync_dir(files)?;
    Ok(dest.to_path_buf())
}

fn regular_file(path: &Path) -> Option<fs::Metadata> {
    let meta = fs::symlink_metadata(path).ok()?;
    (meta.is_file() && !meta.file_type().is_symlink()).then_some(meta)
}

fn open_dir(path: &Path) -> Result<fs::File, HostError> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| {
            HostError::internal(format!(
                "could not open the app files folder: {}",
                error.kind()
            ))
        })
}

fn sync_dir(path: &Path) -> Result<(), HostError> {
    open_dir(path)?.sync_all().map_err(|error| {
        HostError::internal(format!(
            "could not finish the record file: {}",
            error.kind()
        ))
    })
}
