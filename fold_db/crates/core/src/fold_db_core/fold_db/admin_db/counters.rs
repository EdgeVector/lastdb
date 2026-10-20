//! The atom counter planner report.

use super::*;

impl FoldDB {
    /// Totals of the atom counter planner since this process started, and the
    /// recent counter faults. A read of in-memory counters; no store access.
    #[must_use]
    pub fn atom_counter_report(&self) -> crate::db_operations::AtomCounterReport {
        self.db_ops().atoms().counter_report().snapshot()
    }

    /// The retention window of kept conflict records, the janitor's last run,
    /// and the queue depth at that run. A read of in-memory counters.
    #[must_use]
    pub fn conflict_retention_report(&self) -> crate::db_operations::ConflictRetentionReport {
        self.db_ops().atoms().retention_state().snapshot()
    }
}
