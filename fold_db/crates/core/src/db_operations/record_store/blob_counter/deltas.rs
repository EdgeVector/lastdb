//! The net change of blob references in one batch.
//!
//! A blob gains a reference when a record or a field history entry that names
//! it is written, and loses one when such a record or entry is replaced or
//! removed. The deltas are summed per blob before any counter is read, so a
//! batch that removes and adds the same blob changes its counter once.

use crate::records::field_history::{RecordHistoryEntry, RECORD_HISTORY_PREFIX};
use lastdb_model::record_space::RECORD_PREFIX;
use lastdb_model::record_value::StoredRecord;
use std::collections::BTreeMap;

/// Net change of references for each blob of one batch.
#[derive(Debug, Default)]
pub(crate) struct BlobDeltas {
    by_blob: BTreeMap<String, i64>,
}

impl BlobDeltas {
    /// Rows of one storage scope that a drop of the scope deletes. A record
    /// lowers the blobs it names, and a field history entry lowers the blobs of
    /// its copied value, as the write path and the recount count them. Every
    /// other row holds no blob reference. A row that does not decode lowers
    /// nothing: the blob counters stay too high (a leak, which `lastdb db
    /// recount` repairs), and the drop is not blocked. Returns the number of
    /// rows that did not decode.
    pub(crate) fn scope_rows_dropped<'a>(
        &mut self,
        rows: impl IntoIterator<Item = (&'a [u8], &'a [u8])>,
    ) -> usize {
        let mut unreadable = 0;
        for (key, value) in rows {
            let Ok(key) = std::str::from_utf8(key) else {
                continue;
            };
            let Ok(scope) = crate::storage::current_keys::exact_scope(key) else {
                continue;
            };
            let bare = scope.map_or(key, |scope| key.get(scope.len() + 1..).unwrap_or(key));
            let blobs = if bare.starts_with(RECORD_PREFIX) {
                StoredRecord::decode(value)
                    .map(|record| record.blob_refs())
                    .ok()
            } else if bare.starts_with(RECORD_HISTORY_PREFIX) {
                serde_json::from_slice::<RecordHistoryEntry>(value)
                    .map(|entry| entry.blobs())
                    .ok()
            } else {
                continue;
            };
            match blobs {
                Some(blobs) => self.add(&blobs, -1),
                None => unreadable += 1,
            }
        }
        unreadable
    }

    pub(crate) fn add(&mut self, blobs: &[String], step: i64) {
        for blob in blobs {
            *self.by_blob.entry(blob.clone()).or_default() += step;
        }
    }

    /// The blobs with a net change, in blob order. Zero entries stay out.
    pub(super) fn into_changes(self) -> Vec<(String, i64)> {
        self.by_blob
            .into_iter()
            .filter(|(_, delta)| *delta != 0)
            .collect()
    }
}
