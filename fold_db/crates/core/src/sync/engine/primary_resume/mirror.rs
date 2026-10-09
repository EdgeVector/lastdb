use super::*;
use std::fs;

impl SyncEngine {
    pub(super) fn require_durable_primary_resume_mirror(
        &self,
        manifest: &BackupManifest,
    ) -> SyncResult<()> {
        let path = self.backup_manifest_cache_path().ok_or_else(|| {
            SyncError::Storage("primary resume requires a backup keep-set path".into())
        })?;
        write_exact_mirror(&path, manifest)
    }
}

fn write_exact_mirror(path: &std::path::Path, manifest: &BackupManifest) -> SyncResult<()> {
    SyncEngine::write_backup_keep_set_file(path, manifest)
        .and_then(|()| fs::File::open(path)?.sync_all())
        .and_then(|()| fs::File::open(path.parent().unwrap())?.sync_all())
        .map_err(|error| SyncError::Storage(format!("persist primary resume keep set: {error}")))?;
    let bytes = fs::read(path)
        .map_err(|error| SyncError::Storage(format!("read primary resume keep set: {error}")))?;
    let stored: BackupManifest = serde_json::from_slice(&bytes)
        .map_err(|error| SyncError::Storage(format!("decode primary resume keep set: {error}")))?;
    if manifest_sha256_hex(&stored).map_err(|error| SyncError::Storage(error.to_string()))?
        != manifest_sha256_hex(manifest).map_err(|error| SyncError::Storage(error.to_string()))?
    {
        return Err(SyncError::Storage(
            "primary resume keep set differs from the cloud root".into(),
        ));
    }
    Ok(())
}
