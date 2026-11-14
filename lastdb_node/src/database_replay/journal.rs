//! Durable applied-entry markers and page-attempt helpers under `<home>/log-sync/<database>/`.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use cloud_backup_protocol::Id;

use super::decode;
use super::RestoreError;

const MARKER_LIMIT: usize = 64;

pub(super) enum Marker {
    Absent,
    Applied,
}

pub(super) fn read_marker(
    home: &Path,
    database_id: &str,
    mutation_id: &str,
    hash: &str,
) -> Result<Marker, RestoreError> {
    let path = marker_path(home, database_id, mutation_id)?;
    // A body of `1` is E's existing applied marker and counts as this payload.
    match read_bytes(&path, MARKER_LIMIT, "applied marker")? {
        None => Ok(Marker::Absent),
        Some(bytes) if bytes.as_slice() == b"1" || bytes == hash.as_bytes() => Ok(Marker::Applied),
        Some(_) => Err(RestoreError::Conflict("applied marker")),
    }
}

pub(super) fn write_marker(
    home: &Path,
    database_id: &str,
    mutation_id: &str,
    hash: &str,
) -> Result<(), RestoreError> {
    let path = marker_path(home, database_id, mutation_id)?;
    write_private(&path, hash.as_bytes(), "applied marker")
}

fn marker_path(home: &Path, database_id: &str, mutation_id: &str) -> Result<PathBuf, RestoreError> {
    if !decode::mutation_label(mutation_id) {
        return Err(RestoreError::Entry("mutation id"));
    }
    Ok(database_dir(home, database_id)?
        .join("applied")
        .join(mutation_id))
}

pub(super) fn database_dir(home: &Path, database_id: &str) -> Result<PathBuf, RestoreError> {
    let id =
        Id::try_from(database_id.to_owned()).map_err(|_| RestoreError::Scope("database id"))?;
    if id.as_str() != database_id {
        return Err(RestoreError::Scope("database id"));
    }
    Ok(home.join("log-sync").join(database_id))
}

pub(super) fn read_bytes(
    path: &Path,
    limit: usize,
    detail: &'static str,
) -> Result<Option<Vec<u8>>, RestoreError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(RestoreError::Durable(detail)),
    };
    if !metadata.is_file()
        || metadata.permissions().mode() & 0o022 != 0
        || metadata.len() > limit as u64
    {
        return Err(class_for(detail));
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|_| RestoreError::Durable(detail))?;
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| RestoreError::Durable(detail))?;
    if bytes.len() > limit {
        return Err(class_for(detail));
    }
    Ok(Some(bytes))
}

fn class_for(detail: &'static str) -> RestoreError {
    if detail == "applied marker" {
        RestoreError::Conflict(detail)
    } else {
        RestoreError::Durable(detail)
    }
}

pub(super) fn write_private(
    path: &Path,
    bytes: &[u8],
    detail: &'static str,
) -> Result<(), RestoreError> {
    let parent = path.parent().ok_or(RestoreError::Durable(detail))?;
    fs::create_dir_all(parent).map_err(|_| RestoreError::Durable(detail))?;
    let temporary = path.with_extension("tmp");
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&temporary)
            .map_err(|_| RestoreError::Durable(detail))?;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|_| RestoreError::Durable(detail))?;
        file.write_all(bytes)
            .map_err(|_| RestoreError::Durable(detail))?;
        file.sync_all().map_err(|_| RestoreError::Durable(detail))?;
    }
    fs::rename(&temporary, path).map_err(|_| RestoreError::Durable(detail))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| RestoreError::Durable(detail))?;
    Ok(())
}

pub(super) fn remove_private(path: &Path, detail: &'static str) -> Result<(), RestoreError> {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err(RestoreError::Durable(detail)),
    }
    let parent = path.parent().ok_or(RestoreError::Durable(detail))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| RestoreError::Durable(detail))
}
