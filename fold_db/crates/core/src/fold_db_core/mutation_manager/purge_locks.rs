//! Purge statistics.

use std::collections::HashMap;

use super::MutationManager;

impl MutationManager {
    /// Record one completed erase pass against `schema_name`.
    pub(super) fn record_purge(&self, schema_name: &str, records_purged: u64) {
        self.purge_stats
            .record(schema_name, records_purged, crate::clock::unix_millis());
    }

    pub(super) fn record_purge_path(
        &self,
        schema_name: &str,
        target_slots: u64,
        atoms_deleted: u64,
    ) {
        self.purge_stats
            .record_path(schema_name, target_slots, atoms_deleted);
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
