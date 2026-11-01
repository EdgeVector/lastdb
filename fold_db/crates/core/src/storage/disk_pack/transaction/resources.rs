//! Request leases stay owned until durable publication and delivery.

use super::*;

pub(super) type Resource = Arc<dyn Send + Sync>;

impl PendingBatch {
    /// Old read-prefix snapshots may retain this batch after completion. They
    /// must not prolong a completed critical section's queue-owned guard.
    pub(in crate::storage::disk_pack) fn release_resources(&self) {
        self.resources
            .lock()
            .expect("native completed scoped resources")
            .clear();
    }
}
