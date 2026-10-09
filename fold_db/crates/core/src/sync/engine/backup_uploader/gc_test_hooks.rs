use super::*;

impl SyncEngine {
    /// Snapshot of last computed cloud backup footprint for status (cheap).
    pub fn backup_storage_footprint_snapshot(&self) -> Option<BackupStorageFootprint> {
        *self
            .backup_storage_footprint
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn store_backup_storage_footprint(&self, footprint: BackupStorageFootprint) {
        *self
            .backup_storage_footprint
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(footprint);
    }

    /// Refresh referenced/billed/reclaimable from a cloud list + keep-set.
    ///
    /// Used by status background refresh and admin tooling. Caches the result
    /// so subsequent `/api/status` reads stay free of list_objects latency.
    pub async fn refresh_backup_storage_footprint(
        &self,
        live_manifests: &[BackupManifest],
    ) -> SyncResult<BackupStorageFootprint> {
        let listed = self.auth.list_objects("backup/chunks/").await?;
        let mut keep = std::collections::BTreeSet::new();
        for m in live_manifests {
            keep.extend(manifest_referenced_chunk_shas(m));
        }
        let footprint = backup_storage_footprint_from_listing(&listed, &keep);
        self.store_backup_storage_footprint(footprint);
        Ok(footprint)
    }
}
