//! The repair of the retention queue (offline recount).
//!
//! The survey (`time_queue/survey.rs`) finds a bucket that holds entries and has
//! no directory row. The repair writes the row again
//! (`TimeQueue::register_buckets`), so the janitor finds the entries. A repair
//! is safe to run again: a bucket that has its row is not a finding.

use crate::db_operations::record_store::time_queue::TimeQueue;
use crate::db_operations::record_store::RecordStore;
use crate::schema::SchemaError;

/// The queue layout and the window that a repair writes with. The recount takes
/// them from the record of the node when the node recorded them.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RepairWindow {
    pub(crate) queue: TimeQueue,
    pub(crate) window_nanos: u64,
}

impl RecordStore {
    /// Write the directory row of each bucket in `buckets`. A stopped home, or a
    /// holder of a write scope. Returns the number of buckets.
    pub(crate) async fn repair_queue_buckets(
        &self,
        buckets: &[u64],
        window: &RepairWindow,
    ) -> Result<u64, SchemaError> {
        window
            .queue
            .register_buckets(self, buckets, self.retention_state().clock_nanos())
            .await?;
        Ok(buckets.len() as u64)
    }
}
