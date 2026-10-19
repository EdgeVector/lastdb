//! Schema purge barriers and purge statistics.

use std::collections::HashMap;
use std::sync::Arc;

use super::MutationManager;

impl MutationManager {
    /// Resolve the shared `RwLock` for guarded purges of `schema_name`.
    ///
    /// See the [`Self::purge_barrier`] field docs for the per-schema scope.
    pub(crate) fn schema_purge_barrier(&self, schema_name: &str) -> Arc<tokio::sync::RwLock<()>> {
        let mut map = self.purge_barrier.lock().expect("purge_barrier poisoned");
        Arc::clone(map.entry(schema_name.to_string()).or_default())
    }

    /// Record one completed purge pass against `schema_name`.
    ///
    /// `exclusive_hold` is the guarded critical-section time after acquisition.
    /// Ordinary writes do not take this barrier.
    pub(super) fn record_purge(
        &self,
        schema_name: &str,
        records_purged: u64,
        exclusive_hold: std::time::Duration,
    ) {
        self.purge_stats.record(
            schema_name,
            records_purged,
            exclusive_hold,
            crate::clock::unix_millis(),
        );
    }

    pub(super) fn record_schema_barrier_acquisition(&self, schema_name: &str) {
        self.purge_stats
            .record_schema_barrier_acquisition(schema_name);
    }

    pub(super) fn record_purge_path(
        &self,
        schema_name: &str,
        target_slots: u64,
        candidate_atoms: u64,
        reverse_edge_reads: u64,
    ) {
        self.purge_stats.record_path(
            schema_name,
            target_slots,
            candidate_atoms,
            reverse_edge_reads,
        );
    }

    /// Snapshot per-schema purge accounting for the status/ops surfaces.
    /// Cumulative since process start, so consumers delta it themselves —
    /// matching how the request-ops aggregates are consumed.
    #[must_use]
    pub fn purge_stats_snapshot(&self) -> HashMap<String, super::PurgeStats> {
        self.purge_stats.snapshot()
    }

    /// One schema's cumulative purge totals.
    /// All-zero means no purge has run on this schema since process start.
    #[must_use]
    pub fn purge_stats_for_schema(&self, schema_name: &str) -> super::PurgeStats {
        self.purge_stats.for_schema(schema_name)
    }
}
