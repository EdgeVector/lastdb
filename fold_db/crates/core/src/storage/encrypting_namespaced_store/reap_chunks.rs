//! `reap-unsealed` in chunks, with a durable checkpoint after each chunk.

use super::*;
use crate::storage::reap_unsealed::{
    ReapUnsealedCheckpoint, ReapUnsealedReport, REAP_DEFAULT_MAX_ROWS, REAP_DEFAULT_MAX_SECS,
};
use crate::storage::traits::PhysicalScanCursor;

/// Rows one chunk scans before the checkpoint is written. A kill during a call
/// then loses at most one chunk, and the next call goes on from the checkpoint.
const REAP_CHUNK_ROWS: usize = 1024;

impl EncryptingNamespacedStore {
    /// Run the passes of one call: chunks of `REAP_CHUNK_ROWS` rows, until the
    /// plane is done or the row or time budget of the call is spent. Outside a
    /// dry run each chunk stores the checkpoint, so a kill loses at most one.
    pub(super) async fn reap_in_chunks(
        &self,
        store: &EncryptingKvStore,
        options: &ReapUnsealedOptions,
        mut cursor: Option<PhysicalScanCursor>,
        stored: Option<ReapUnsealedCheckpoint>,
    ) -> StorageResult<ReapUnsealedReport> {
        let collection = options.collection.trim().to_string();
        let max_rows = options.max_rows.unwrap_or(REAP_DEFAULT_MAX_ROWS).max(1);
        let max_secs = options.max_secs.unwrap_or(REAP_DEFAULT_MAX_SECS).max(1);
        let started = std::time::Instant::now();
        let mut total = ReapUnsealedReport::empty(collection.clone(), options.dry_run);
        let mut checkpoint = stored.unwrap_or_else(|| ReapUnsealedCheckpoint {
            collection: collection.clone(),
            cursor: None,
            rows_scanned_total: 0,
            rows_removed_total: 0,
            bytes_reclaimed_total: 0,
            completed: false,
            updated_at: chrono::Utc::now(),
        });
        // A dry run keeps no checkpoint: its totals start at zero.
        if options.dry_run {
            checkpoint.rows_scanned_total = 0;
            checkpoint.rows_removed_total = 0;
            checkpoint.bytes_reclaimed_total = 0;
        }
        loop {
            let left_rows = max_rows.saturating_sub(total.rows_scanned as usize);
            let left_secs = max_secs.saturating_sub(started.elapsed().as_secs()).max(1);
            let chunk = ReapUnsealedOptions {
                max_rows: Some(left_rows.min(REAP_CHUNK_ROWS)),
                max_secs: Some(left_secs),
                ..options.clone()
            };
            let part = store
                .reap_unsealed_pass(&collection, &chunk, cursor.clone())
                .await?;
            checkpoint.rows_scanned_total = checkpoint
                .rows_scanned_total
                .saturating_add(part.rows_scanned);
            checkpoint.rows_removed_total = checkpoint
                .rows_removed_total
                .saturating_add(part.rows_removed);
            checkpoint.bytes_reclaimed_total = checkpoint
                .bytes_reclaimed_total
                .saturating_add(part.bytes_reclaimed);
            checkpoint.cursor = part.next_cursor.clone();
            checkpoint.completed = !part.more_remaining;
            checkpoint.updated_at = chrono::Utc::now();
            if !options.dry_run {
                self.store_reap_checkpoint(&checkpoint).await?;
            }
            cursor = part.next_cursor.clone();
            let more = part.more_remaining;
            total.absorb(part);
            let spent =
                total.rows_scanned as usize >= max_rows || started.elapsed().as_secs() >= max_secs;
            if !more || spent {
                break;
            }
        }
        total.checkpoint = Some(checkpoint);
        Ok(total)
    }
}
