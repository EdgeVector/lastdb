//! Current local storage-meter snapshot reclaim.

use super::*;

impl FoldDB {
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
