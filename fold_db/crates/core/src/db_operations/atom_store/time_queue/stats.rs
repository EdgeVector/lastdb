//! The size of a time-bucketed queue, counted by the directory and the buckets.

use super::keys::TimeQueue;
use crate::db_operations::atom_store::{AtomStore, RangeWalk};
use crate::schema::SchemaError;

/// Directory rows that one read asks for.
const DIRECTORY_PAGE: usize = 1_000;
/// Entries that one read of a bucket asks for.
const BUCKET_PAGE: usize = 256;
/// Buckets the count lists, at most.
const MAX_BUCKETS: usize = 10_000;
/// Reads of buckets that one count makes, at most. The count is a gauge for a
/// status line, and it must stay cheap when the queue is large.
const MAX_BUCKET_READS: usize = 1_024;

/// What the directory and the buckets say about a queue.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QueueStats {
    /// Rows in the directory: buckets that hold, or held, an entry.
    pub(crate) buckets: u64,
    /// Entries, counted up to the cap.
    pub(crate) entries: u64,
    /// True when the queue holds more entries than `entries`, or when the count
    /// stopped before it read every bucket.
    pub(crate) capped: bool,
}

impl AtomStore {
    /// Count the buckets of a queue by its directory, and its entries up to
    /// `cap`. Every read is a bounded range walk, so deleted keys cannot make
    /// the count fail. A bucket that cannot be read ends the count with
    /// `capped` set: the entries are a lower bound. Only a failure to read the
    /// directory is an error.
    pub(crate) async fn time_queue_stats(
        &self,
        queue: &TimeQueue,
        cap: u64,
    ) -> Result<QueueStats, SchemaError> {
        let (start, end) = queue.directory_range(0, u64::MAX);
        let mut walk = RangeWalk::new(start.as_bytes(), end.as_bytes(), DIRECTORY_PAGE);
        let mut buckets: Vec<u64> = Vec::new();
        let mut stats = QueueStats::default();
        while !walk.done() {
            let step = self.walk_step(&mut walk).await?;
            for (key, _) in step.rows {
                let bucket = String::from_utf8(key)
                    .ok()
                    .and_then(|key| queue.directory_bucket(&key));
                if let Some(bucket) = bucket {
                    buckets.push(bucket);
                }
            }
            if buckets.len() > MAX_BUCKETS {
                stats.capped = true;
                break;
            }
        }
        stats.buckets = buckets.len() as u64;
        let mut reads = 0usize;
        'buckets: for bucket in buckets {
            let (start, end) = queue.bucket_range(bucket);
            let mut walk = RangeWalk::new(start.as_bytes(), end.as_bytes(), BUCKET_PAGE);
            while !walk.done() {
                reads += 1;
                if reads > MAX_BUCKET_READS {
                    stats.capped = true;
                    break 'buckets;
                }
                let Ok(step) = self.walk_step(&mut walk).await else {
                    stats.capped = true;
                    break 'buckets;
                };
                stats.entries += step.rows.len() as u64;
                if stats.entries > cap {
                    stats.entries = cap;
                    stats.capped = true;
                    break 'buckets;
                }
            }
        }
        Ok(stats)
    }
}
