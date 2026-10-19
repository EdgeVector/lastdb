//! Reclaim-keep-small snapshots.

use super::*;

impl FoldDB {
    /// Drop the legacy `metadata` hash group that held the keep-small
    /// snapshot, without loading it (`lastdb db reclaim-keep-small-legacy`).
    ///
    /// The snapshot moved to its own `keep_small` plane on 2026-09-21 after a
    /// per-write flush (fold #2127) filled its `metadata` group with 39 GB of
    /// superseded copies — 6,782 segments on the primary — which the first
    /// write after boot then loaded whole, crossing the 16 GiB memory guard
    /// and restarting the daemon every 7-17 minutes. Nothing reads the old key
    /// now, `compact` cannot touch a group it cannot load, and the cold-load
    /// cap refuses the load. Dropping the directory is the only byte-return
    /// path. The store proves from the group's id sidecar that it holds
    /// nothing but `keep_small:meters` and refuses otherwise. Dry-run by
    /// default.
    pub fn reclaim_keep_small_legacy(
        &self,
        dry_run: bool,
    ) -> Result<crate::storage::laststore::DeadHashGroupDropReport, FoldDbError> {
        self.db_ops()
            .namespaced_store()
            .drop_dead_hash_group(crate::storage::laststore::DeadHashGroupDropOptions {
                collection: "metadata".to_string(),
                expected_only_id: crate::db_operations::KEEP_SMALL_SNAPSHOT_KEY.to_string(),
                dry_run,
            })
            .map_err(|e| FoldDbError::Database(format!("reclaim_keep_small_legacy: {e}")))
    }

    /// Drop the current `keep_small` hash group that holds only the local
    /// storage-meter snapshot, without loading it.
    ///
    /// The snapshot is node-local, capture-skipped bookkeeping. A missing row
    /// starts the normal bootstrap path, so this is safe only after the store
    /// proves that the group sidecar names exactly `keep_small:meters`.
    /// This provides a byte-return path when the group's superseded snapshots
    /// exceed the cold-group load cap and generic compact must refuse it.
    pub fn reclaim_keep_small_snapshot(
        &self,
        dry_run: bool,
    ) -> Result<crate::storage::laststore::DeadHashGroupDropReport, FoldDbError> {
        self.db_ops()
            .namespaced_store()
            .drop_dead_hash_group(crate::storage::laststore::DeadHashGroupDropOptions {
                collection: crate::db_operations::KEEP_SMALL_SNAPSHOT_COLLECTION.to_string(),
                expected_only_id: crate::db_operations::KEEP_SMALL_SNAPSHOT_KEY.to_string(),
                dry_run,
            })
            .map_err(|e| FoldDbError::Database(format!("reclaim_keep_small_snapshot: {e}")))
    }
}
