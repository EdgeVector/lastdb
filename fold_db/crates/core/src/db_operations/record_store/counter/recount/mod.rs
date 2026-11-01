//! The offline recount of a stopped home: the blob counters, the buckets of the
//! retention queue, and the replay cutoff.
//!
//! - The blob step (`blobs.rs`) counts the file blob references of the records
//!   and of the field history of every listed scope, and lets
//!   `blob_counter::recount` repair the blob counters and the blob queue.
//! - The retention step (`retention.rs`) surveys the retention queue and gives a
//!   bucket that holds entries its directory row back.
//! - With a cutoff, the replay cutoff of the home moves up (`delete_replay.rs`).
//!
//! **Offline only.** It takes no write gate. The caller opens a stopped home
//! (`lastdb db recount` refuses a home that a daemon serves). No gate or write
//! path calls it. It is safe to run again: a run that stopped leaves every
//! batch whole, and the next run changes only what is still wrong.

mod blobs;
mod report;
mod retention;

pub use report::{RecountOptions, RecountReport};

use crate::db_operations::record_store::RecordStore;
use crate::schema::SchemaError;
use std::time::Instant;

/// Rows asked for in each read of the recount.
const PAGE_ROWS: usize = 2_000;

impl RecordStore {
    /// Recount the blob counters of the given scopes and repair the retention
    /// queue. Offline only. See the module documentation.
    pub async fn recount_counters(
        &self,
        options: &RecountOptions,
    ) -> Result<RecountReport, SchemaError> {
        let started = Instant::now();
        let mut report = RecountReport {
            executed: options.execute,
            scopes: options.scopes.clone(),
            ..RecountReport::default()
        };
        let before = self.range_walks().snapshot();
        self.recount_retention(options.execute, &mut report).await?;
        self.recount_blobs(options, &mut report).await?;
        if let Some(cutoff) = options.delete_cutoff_nanos {
            let raised = self.raise_replay_cutoff(cutoff, options.execute).await?;
            report.operations += raised.operations;
            report.batches += raised.batches;
            report.replay_cutoff = Some(raised);
        }
        report.range_reads = self.range_walks().snapshot().since(before);
        report.elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        Ok(report)
    }
}
