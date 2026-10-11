use super::*;
use crate::sync::engine::backup_restore::download_manifest_chain;

impl SyncEngine {
    /// Caller holds the normal publication turn. A recovered body is an
    /// authenticated cloud predecessor, not a new local commit or GC grant.
    pub(super) async fn recover_missing_normal_backup_predecessor(
        &self,
        previous: Option<BackupManifest>,
    ) -> SyncResult<Option<BackupManifest>> {
        if previous.is_some()
            || self.backup_only_mode.load(Ordering::Acquire)
            || !self.cloud_plane_allows_upload().await
        {
            return Ok(previous);
        }
        let store = self.laststore_backup_source.as_ref().ok_or_else(|| {
            SyncError::Storage("backup predecessor recovery requires a LastStore source".into())
        })?;
        let (store_uuid, epoch) = store.backup_source_identity().map_err(|_| {
            SyncError::Storage(
                "backup predecessor source identity is unavailable or invalid".into(),
            )
        })?;
        let db_hash = cloud_db_hash_for_store_uuid(&store_uuid);
        if self.auth.db_hash_scope() != Some(db_hash.as_str()) {
            return Err(SyncError::Storage(
                "backup predecessor database scope does not match the local source".into(),
            ));
        }
        let Some(latest) = self.auth.backup_latest_get_optional().await? else {
            return Ok(None);
        };
        require_predecessor_pointer(&latest.latest, &store_uuid, epoch)?;
        let manifests =
            download_manifest_chain(&self.auth, &self.s3, &latest.latest.manifest_sha256)
                .await
                .map_err(|failure| failure.source)?;
        let tip = manifests
            .first()
            .ok_or_else(|| SyncError::Storage("backup predecessor chain is empty".into()))?;
        require_predecessor_chain_source(&manifests, &store_uuid, epoch, &db_hash)?;
        if latest.latest.format_version() != tip.version
            || latest.latest.counter != tip.counter
            || manifest_sha256_hex(tip)? != latest.latest.manifest_sha256
        {
            return Err(SyncError::Storage(
                "backup predecessor pointer does not match its verified body".into(),
            ));
        }
        let current = self.auth.backup_latest_get_optional().await?;
        if !current.is_some_and(|current| same_predecessor_pointer(&latest.latest, &current.latest))
        {
            return Err(SyncError::Storage(
                "backup predecessor latest changed during recovery".into(),
            ));
        }
        let path = self.backup_manifest_cache_path().ok_or_else(|| {
            SyncError::Storage("backup predecessor recovery requires a local mirror".into())
        })?;
        Self::write_backup_keep_set_file(&path, tip)
            .map_err(|_| SyncError::Storage("backup predecessor mirror write failed".into()))?;
        Ok(Some(tip.clone()))
    }
}

fn require_predecessor_pointer(
    pointer: &BackupLatestPointer,
    store_uuid: &str,
    epoch: u64,
) -> SyncResult<()> {
    pointer.require_supported_format()?;
    if pointer.store_uuid != store_uuid
        || pointer.epoch != epoch
        || pointer.counter == 0
        || pointer.manifest_sha256.len() != 64
        || !pointer
            .manifest_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(SyncError::Storage(
            "backup predecessor pointer is invalid or belongs to another source".into(),
        ));
    }
    Ok(())
}

fn require_predecessor_chain_source(
    manifests: &[BackupManifest],
    store_uuid: &str,
    epoch: u64,
    db_hash: &str,
) -> SyncResult<()> {
    if manifests.iter().any(|manifest| {
        manifest.store_uuid != store_uuid
            || manifest.epoch != epoch
            || cloud_db_hash_for_store_uuid(&manifest.store_uuid) != db_hash
    }) {
        return Err(SyncError::Storage(
            "backup predecessor chain belongs to another source".into(),
        ));
    }
    Ok(())
}

fn same_predecessor_pointer(left: &BackupLatestPointer, right: &BackupLatestPointer) -> bool {
    left.format_version() == right.format_version()
        && left.store_uuid == right.store_uuid
        && left.epoch == right.epoch
        && left.counter == right.counter
        && left.manifest_sha256 == right.manifest_sha256
}
