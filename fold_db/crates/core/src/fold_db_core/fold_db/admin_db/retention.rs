//! Tip-history drain, superseded-version retention and reclaim-keep-small snapshots.

use super::*;

impl FoldDB {
    /// One bounded, resumable pass that drains existing live tip-version
    /// chains without collection compaction. See
    /// [`crate::db_operations::AtomStore::drain_tip_history_chains`].
    pub async fn drain_tip_history_chains(
        &self,
        options: TipHistoryDrainOptions,
    ) -> Result<TipHistoryDrainReport, FoldDbError> {
        self.db_ops()
            .atoms()
            .drain_tip_history_chains(options)
            .await
            .map_err(|e| admin_op_error("drain_tip_history_chains", e))
    }

    /// Drain from the durable automatic-reclaim checkpoint and advance it.
    pub async fn drain_tip_history_chains_from_checkpoint(
        &self,
        options: TipHistoryDrainOptions,
    ) -> Result<(TipHistoryDrainReport, TipHistoryDrainCheckpoint), FoldDbError> {
        self.db_ops()
            .atoms()
            .drain_tip_history_chains_from_checkpoint(options)
            .await
            .map_err(|e| {
                FoldDbError::Database(format!("drain_tip_history_chains_from_checkpoint: {e}"))
            })
    }

    /// Read the durable tip-history drain checkpoint.
    pub async fn tip_history_drain_checkpoint(
        &self,
    ) -> Result<TipHistoryDrainCheckpoint, FoldDbError> {
        self.db_ops()
            .atoms()
            .tip_history_drain_checkpoint(None)
            .await
            .map_err(|e| FoldDbError::Database(format!("tip_history_drain_checkpoint: {e}")))
    }

    /// One bounded pass that drops expired `tv:` nodes on live heads and keeps
    /// versions inside the 7-day window. Tombstoned heads are skipped.
    pub async fn retain_superseded_versions(
        &self,
        options: SupersededVersionRetentionOptions,
    ) -> Result<SupersededVersionRetentionReport, FoldDbError> {
        self.db_ops()
            .atoms()
            .retain_superseded_versions(options)
            .await
            .map_err(|e| admin_op_error("retain_superseded_versions", e))
    }

    /// Retention pass from the durable checkpoint (resume / settle).
    pub async fn retain_superseded_versions_from_checkpoint(
        &self,
        options: SupersededVersionRetentionOptions,
    ) -> Result<
        (
            SupersededVersionRetentionReport,
            SupersededVersionRetentionCheckpoint,
        ),
        FoldDbError,
    > {
        self.db_ops()
            .atoms()
            .retain_superseded_versions_from_checkpoint(options)
            .await
            .map_err(|e| {
                FoldDbError::Database(format!("retain_superseded_versions_from_checkpoint: {e}"))
            })
    }

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

    /// Measure (and optionally delete) legacy `ref:` whole-molecule blobs —
    /// pre-per-key layout residue the live read path never dual-reads.
    /// Dry-run by default.
    pub async fn purge_ref_blobs(
        &self,
        dry_run: bool,
    ) -> Result<LegacyRefBlobPurgeReport, FoldDbError> {
        self.db_ops()
            .atoms()
            .purge_ref_blobs(dry_run, None)
            .await
            .map_err(|e| FoldDbError::Database(format!("purge_ref_blobs: {e}")))
    }
}
