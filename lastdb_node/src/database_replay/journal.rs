//! Durable applied markers and private replay state under `<home>/log-sync/<database>/`.

use std::io;
use std::path::{Path, PathBuf};

use cloud_backup_protocol::Id;

use crate::profile::private_fs;

use super::chain as decode;
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
    // A body of `1` is the existing local applied marker and counts as this payload.
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
    private_fs::read_limited(path, limit).map_err(|error| {
        if error.kind() == io::ErrorKind::InvalidData && detail == "applied marker" {
            RestoreError::Conflict(detail)
        } else {
            RestoreError::Durable(detail)
        }
    })
}

pub(super) fn write_private(
    path: &Path,
    bytes: &[u8],
    detail: &'static str,
) -> Result<(), RestoreError> {
    private_fs::write_private(path, bytes).map_err(|_| RestoreError::Durable(detail))
}

pub(super) fn remove_private(path: &Path, detail: &'static str) -> Result<(), RestoreError> {
    private_fs::remove_private(path).map_err(|_| RestoreError::Durable(detail))
}
