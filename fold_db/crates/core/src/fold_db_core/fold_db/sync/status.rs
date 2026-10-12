use super::super::FoldDB;
use std::sync::Arc;

impl FoldDB {
    /// Start the background sync timer. Delegates to the coordinator.
    ///
    /// Automatic gc-atoms is not started here. FoldDB init starts that
    /// local cadence even when no sync engine is configured.
    pub fn start_sync(&self, interval_ms: u64) {
        self.sync_coordinator.start_background_sync(interval_ms);
    }

    /// Force an immediate sync (e.g. on shutdown).
    pub async fn force_sync(&self) -> Result<(), crate::sync::SyncError> {
        self.sync_coordinator.force_sync().await
    }

    /// **Live** cloud snapshot + upload-staging heal (never stops Mini).
    ///
    /// Uploads `latest.enc` from the open store, resets capture watermark, and
    /// clears durable upload staging **only if** the snapshot succeeds.
    /// Concurrent local R/W continues; this is the product path for operators
    /// (sled-era offline exclusive snapshot is gone with the sled engine).
    pub async fn heal_cloud_staging(
        &self,
    ) -> Result<crate::sync::engine::HealStagingReport, crate::error::FoldDbError> {
        let engine = self.sync_engine().ok_or_else(|| {
            crate::error::FoldDbError::Database(
                "cloud sync is not configured (no cloud_sync.json / sync engine)".into(),
            )
        })?;
        engine
            .heal_staging_via_snapshot()
            .await
            .map_err(crate::error::FoldDbError::Database)
    }

    pub async fn laststore_cloud_snapshot(
        &self,
        previous_manifest: Option<&crate::storage::laststore::BackupManifest>,
    ) -> Result<
        (
            crate::storage::laststore::BackupManifest,
            crate::sync::engine::LastStoreCloudSnapshotReport,
        ),
        crate::error::FoldDbError,
    > {
        let engine = self.sync_engine().ok_or_else(|| {
            crate::error::FoldDbError::Database(
                "cloud sync is not configured (no cloud_sync.json / sync engine)".into(),
            )
        })?;
        engine
            .laststore_cloud_snapshot(previous_manifest)
            .await
            .map_err(|e| {
                crate::error::fold_db_error_from_sync("laststore cloud snapshot failed", e)
            })
    }

    /// Product path: GC unreferenced cloud backup chunks (orphan sweep).
    ///
    /// Keeps digests referenced by `live_manifests` (pass current + retained
    /// cuts). `dry_run: true` only reports orphans; `false` presign-DELETEs them.
    pub async fn gc_orphan_backup_chunks(
        &self,
        live_manifests: &[crate::storage::laststore::BackupManifest],
        dry_run: bool,
    ) -> Result<crate::sync::engine::BackupOrphanGcReport, crate::error::FoldDbError> {
        let engine = self.sync_engine().ok_or_else(|| {
            crate::error::FoldDbError::Database(
                "cloud sync is not configured (no cloud_sync.json / sync engine)".into(),
            )
        })?;
        engine
            .gc_orphan_backup_chunks(live_manifests, dry_run)
            .await
            .map_err(|e| crate::error::FoldDbError::Database(e.to_string()))
    }

    /// Product path: read-only R2 prefix/category size inventory for the
    /// connected cloud account. List-only — never deletes, never fetches an
    /// object body. See [`crate::sync::engine::PrefixInventoryReport`].
    pub async fn prefix_inventory(
        &self,
    ) -> Result<crate::sync::engine::PrefixInventoryReport, crate::error::FoldDbError> {
        let engine = self.sync_engine().ok_or_else(|| {
            crate::error::FoldDbError::Database(
                "cloud sync is not configured (no cloud_sync.json / sync engine)".into(),
            )
        })?;
        engine
            .prefix_inventory()
            .await
            .map_err(|e| crate::error::FoldDbError::Database(e.to_string()))
    }

    /// Get the sync engine state, if sync is configured.
    pub async fn sync_state(&self) -> Option<crate::sync::SyncState> {
        self.sync_coordinator.state().await
    }

    /// Get a full sync status snapshot, if sync is configured.
    pub async fn sync_status(&self) -> Option<crate::sync::SyncStatus> {
        self.sync_coordinator.status().await
    }

    /// Intentionally pause cloud mutation staging on the live engine (grace clock).
    ///
    /// Does **not** remove credentials; pair with renaming `cloud_sync.json` for
    /// durable reboot pause (`lastdb cloud off`). No-op-ish if no engine.
    pub async fn set_cloud_sync_disabled_live(&self, disabled: bool) -> Result<(), String> {
        let engine = self.sync_engine().ok_or_else(|| {
            "cloud sync engine not running (daemon booted without cloud_sync.json)".to_string()
        })?;
        engine.set_cloud_sync_disabled(disabled).await;
        Ok(())
    }

    /// Re-enable after intentional pause: pull→snapshot when past grace, then resume staging.
    pub async fn reenable_cloud_sync_live(
        &self,
    ) -> Result<crate::sync::CloudSyncReenableOutcome, String> {
        let engine = self.sync_engine().ok_or_else(|| {
            "cloud sync engine not running (daemon booted without cloud_sync.json)".to_string()
        })?;
        Ok(engine.reenable_cloud_sync().await)
    }

    /// Last-fire automatic plane-compaction reports, on any node shape.
    ///
    /// `sync_status()` is `None` without `cloud_sync.json`, so reading these
    /// off the sync snapshot hid the whole cadence on exactly the node that
    /// most needs to prove it runs. This reads the compactor the local cadence
    /// steps, or the engine's when cloud sync is on — the same object either
    /// way. `None` means no cadence started in this process.
    pub async fn automatic_compaction_status(
        &self,
    ) -> Option<std::collections::BTreeMap<String, crate::sync::engine::AutomaticCompactionStatus>>
    {
        self.sync_coordinator.automatic_compaction_status().await
    }

    /// Returns true if the sync engine is configured.
    pub fn is_sync_enabled(&self) -> bool {
        self.sync_coordinator.is_enabled()
    }

    /// Returns a clone of the sync engine Arc, if configured.
    ///
    /// Used by callers to configure scoped sync targets after startup.
    pub fn sync_engine(&self) -> Option<Arc<crate::sync::SyncEngine>> {
        self.sync_coordinator.engine()
    }

    /// True while a backup cut is held in-process and must not be deleted out
    /// from under. Local-only boots with no sync engine are never held.
    pub async fn backup_publish_target_is_held(&self) -> bool {
        match self.sync_engine() {
            Some(engine) => engine.has_backup_publish_target().await,
            None => false,
        }
    }

    /// Run `op` only when no backup cut is held, keeping the photograph packing
    /// lock across `op` so a cut cannot start mid-rewrite.
    ///
    /// Returns `None` when a cut is already in the slot (caller must defer).
    /// Local-only boots with no sync engine always run `op`.
    pub async fn run_unless_backup_publish_target_held<T, F, Fut>(&self, op: F) -> Option<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        match self.sync_engine() {
            Some(engine) => {
                let guard = engine.lock_backup_publish_target().await;
                if guard.is_some() {
                    None
                } else {
                    let result = op().await;
                    drop(guard);
                    Some(result)
                }
            }
            None => Some(op().await),
        }
    }
}
