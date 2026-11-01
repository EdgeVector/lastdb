//! Stable local restore report and durable owned metadata helpers.
use super::{RestoreError, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoreReport {
    pub commit_sha256: String,
    pub native_files: u64,
    pub native_bytes: u64,
    pub verified_rows: u64,
    pub file_dependencies_verified: bool,
    #[serde(default)]
    pub verified_external_files: u64,
    pub destination: PathBuf,
}

pub(super) fn owner_file(path: &Path, new: bool) -> Result<File> {
    if !new && !fs::symlink_metadata(path)?.is_file() {
        return Err(RestoreError::Invalid("owned file"));
    }
    let mut options = OpenOptions::new();
    options.read(true).write(true).mode(0o600);
    if new {
        options.create_new(true);
    }
    Ok(options.open(path)?)
}
pub(super) fn read_json<T: serde::de::DeserializeOwned>(path: &Path, max: u64) -> Result<T> {
    if !fs::symlink_metadata(path)?.is_file() {
        return Err(RestoreError::Invalid("metadata file"));
    }
    let mut bytes = Vec::new();
    File::open(path)?.take(max + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max {
        return Err(RestoreError::Limit("metadata bytes"));
    }
    serde_json::from_slice(&bytes).map_err(|_| RestoreError::Invalid("metadata JSON"))
}
pub(super) fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = cloud_backup_protocol::canonical_bytes(value)?;
    if bytes.len() > 256 * 1024 {
        return Err(RestoreError::Limit("saved metadata"));
    }
    let pending = path.with_extension("pending");
    if pending.try_exists()? && !fs::symlink_metadata(&pending)?.is_file() {
        return Err(RestoreError::Invalid("metadata temporary"));
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&pending)?;
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
