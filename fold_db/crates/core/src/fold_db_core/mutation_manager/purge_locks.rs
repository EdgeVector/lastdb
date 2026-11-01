//! Purge statistics.

use std::collections::HashMap;

use super::MutationManager;

impl MutationManager {
    /// Snapshot per-schema purge accounting for the status/ops surfaces.
    /// Cumulative since process start, so consumers delta it themselves —
    /// matching how the request-ops aggregates are consumed.
    #[must_use]
    pub fn purge_stats_snapshot(&self) -> HashMap<String, super::PurgeStats> {
        self.purge_stats.snapshot()
    }
}
