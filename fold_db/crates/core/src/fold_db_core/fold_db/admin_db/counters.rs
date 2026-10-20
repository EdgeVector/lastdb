//! The atom counter and blob counter reports.

use super::*;

impl FoldDB {
    /// Totals of the atom counter planner since this process started, and the
    /// recent counter faults. A read of in-memory counters; no store access.
    #[must_use]
    pub fn atom_counter_report(&self) -> crate::db_operations::AtomCounterReport {
        self.db_ops().atoms().counter_report().snapshot()
    }

    /// Totals of the blob counters since this process started, and what the last
    /// dwell pass saw in the queue. A read of in-memory counters; no store access.
    #[must_use]
    pub fn blob_counter_report(&self) -> crate::db_operations::BlobCounterReport {
        self.db_ops().atoms().blob_counter_report()
    }
}
