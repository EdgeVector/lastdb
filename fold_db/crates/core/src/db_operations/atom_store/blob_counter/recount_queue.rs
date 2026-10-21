//! The queue repairs of the blob step of the recount, written through the planner.
//!
//! Two things make a blob invisible to the dwell pass, and the recount finds both
//! on a stopped home:
//!
//! - **A counter in its dwell with no queue entry.** The pass reads entries, so it
//!   never reads the blob. The entry (and the directory row of its bucket) is
//!   written again at the `zero_since` of the counter.
//! - **A queue entry whose bucket has no directory row.** The pass reads the
//!   directory, so it never lists the bucket. The row is written again
//!   (`TimeQueue::register_buckets`).
//!
//! Both are batches of the planner (`commit_tip_changes`, rows only), so they add
//! no writer to the writer lint. A second run finds nothing: the rows are there.

use super::{BlobQueue, BlobRecountReport};
use crate::db_operations::atom_store::counter::TipChangeSet;
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;
use std::collections::BTreeSet;

/// Entries whose rows one repair batch writes, at most.
const REPAIR_BATCH_ENTRIES: usize = 100;
/// Directory rows that one read asks for.
const DIRECTORY_READ: usize = 1_000;

impl AtomStore {
    /// Write the queue entry and the directory row of each `(blob, zero_since)`
    /// again. Does nothing in a dry run.
    pub(super) async fn write_blob_queue_rows(
        &self,
        queue: &BlobQueue,
        entries: &[(String, u64)],
        execute: bool,
    ) -> Result<(), SchemaError> {
        if !execute {
            return Ok(());
        }
        for chunk in entries.chunks(REPAIR_BATCH_ENTRIES) {
            let rows = chunk
                .iter()
                .flat_map(|(blob, at)| queue.put_rows(*at, blob))
                .collect();
            self.in_background_scope(async {
                self.commit_tip_changes(TipChangeSet {
                    rows,
                    ..TipChangeSet::default()
                })
                .await
                .map(|_| ())
            })
            .await?;
        }
        Ok(())
    }

    /// Check that each of `buckets` has its directory row, and with `execute`
    /// write the rows that are missing. `buckets` are the buckets of the entries
    /// that stay.
    pub(super) async fn check_blob_directory(
        &self,
        queue: &BlobQueue,
        buckets: &BTreeSet<u64>,
        execute: bool,
        report: &mut BlobRecountReport,
    ) -> Result<(), SchemaError> {
        let all: Vec<u64> = buckets.iter().copied().collect();
        let mut missing: Vec<u64> = Vec::new();
        for chunk in all.chunks(DIRECTORY_READ) {
            let keys = chunk
                .iter()
                .map(|bucket| queue.layout().directory_key(*bucket).into_bytes())
                .collect();
            let found = self
                .raw()
                .inner()
                .exists_many(keys)
                .await
                .map_err(SchemaError::from)?;
            missing.extend(
                chunk
                    .iter()
                    .zip(found)
                    .filter(|(_, present)| !present)
                    .map(|(bucket, _)| *bucket),
            );
        }
        report.directory_rows_restored += missing.len() as u64;
        if execute && !missing.is_empty() {
            let stamp = BlobQueue::nanos(self.blob_clock_millis());
            queue
                .layout()
                .register_buckets(self, &missing, stamp)
                .await?;
        }
        Ok(())
    }
}
