//! The size of the dwell queue, for `lastdb status`.
//!
//! The report counts the buckets by the directory and the entries by the buckets,
//! with bounded range walks (`time_queue_stats`), so deleted keys cannot make it
//! fail and it stays cheap when the queue is large.

use super::{blob_dwell_millis, BlobCounterReport, BlobQueue};
use super::{BlobCounters, BlobPorts};
use crate::db_operations::atom_store::RangeWalk;
use crate::schema::SchemaError;

/// The queue depth that the status counts up to.
const QUEUE_DEPTH_CAP: u64 = 1_000;

impl<S: BlobPorts> BlobCounters<'_, S> {
    /// The blob counter report with the size of the dwell queue: the buckets by
    /// the directory, and the entries counted up to 1,000 with bounded reads. A
    /// bucket that cannot be read makes the entry count a lower bound
    /// (`queue_depth_capped`). Only a failure to read the directory leaves
    /// `queue_buckets` empty. `next_due_unix_ms` is the earliest time at which
    /// the oldest bucket can hold a due entry: its start plus the dwell.
    pub async fn blob_counter_report_full(&self) -> BlobCounterReport {
        let mut report = self.store.blob_events().snapshot();
        let queue = self.store.blob_events().queue();
        if let Ok(stats) = self
            .store
            .time_queue_stats(&queue.layout(), QUEUE_DEPTH_CAP)
            .await
        {
            report.queue_depth = stats.entries;
            report.queue_depth_capped = stats.capped;
            report.queue_buckets = Some(stats.buckets);
        }
        if let Ok(Some(bucket)) = self.blob_queue_first_bucket(&queue).await {
            report.next_due_unix_ms =
                Some((bucket / 1_000_000).saturating_add(blob_dwell_millis()));
        }
        report
    }

    /// The start of the oldest bucket in the directory. A bounded walk of one row.
    async fn blob_queue_first_bucket(&self, queue: &BlobQueue) -> Result<Option<u64>, SchemaError> {
        let (start, end) = queue.layout().directory_range(0, u64::MAX);
        let mut walk = RangeWalk::new(start.as_bytes(), end.as_bytes(), 1);
        while !walk.done() {
            let step = self.store.walk_step(&mut walk).await?;
            for (key, _) in step.rows {
                let bucket = String::from_utf8(key)
                    .ok()
                    .and_then(|key| queue.layout().directory_bucket(&key));
                if bucket.is_some() {
                    return Ok(bucket);
                }
            }
        }
        Ok(None)
    }
}
