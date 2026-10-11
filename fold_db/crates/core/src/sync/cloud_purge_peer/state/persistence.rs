//! Monotonic control validation and durable private peer journals.

use super::*;
use std::fs::{File, OpenOptions};
use std::io::Write;

pub(super) fn validate_successor(
    previous: &CloudPurgeControl,
    next: &CloudPurgeControl,
) -> SyncResult<()> {
    if next.generation < previous.generation
        || (next.generation == previous.generation && next.revision < previous.revision)
        || (next.generation == previous.generation && next.operation_id != previous.operation_id)
    {
        return Err(held(
            "cloud control moved backward or changed an operation identity",
        ));
    }
    if next.generation == previous.generation
        && previous.replacement_manifest.is_some()
        && previous.replacement_manifest != next.replacement_manifest
    {
        return Err(held(
            "cloud replacement identity changed within one generation",
        ));
    }
    if next.generation == previous.generation
        && previous.continuation.is_some()
        && previous.continuation != next.continuation
    {
        return Err(held(
            "cloud continuation identity changed within one generation",
        ));
    }
    Ok(())
}

pub(in crate::sync::cloud_purge_peer) fn account_key(account: &CloudAccount) -> SyncResult<String> {
    if account.id.is_empty() {
        return Err(held("cloud account identity is empty"));
    }
    String::from_utf8(serde_json::to_vec(account)?)
        .map_err(|e| SyncError::Serialization(e.to_string()))
}

pub(in crate::sync::cloud_purge_peer) fn private_directory(path: &Path) -> SyncResult<()> {
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

pub(in crate::sync::cloud_purge_peer) fn write_atomic(path: &Path, bytes: &[u8]) -> SyncResult<()> {
    let parent = path
        .parent()
        .ok_or_else(|| held("cloud peer journal has no parent"))?;
    private_directory(parent)?;
    let temporary = parent.join(format!(".write-{}.tmp", uuid::Uuid::new_v4()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}
