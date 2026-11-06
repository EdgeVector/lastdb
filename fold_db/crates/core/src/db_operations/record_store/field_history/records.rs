//! Release field history that was written by an earlier LastDB version.

use crate::db_operations::record_store::{RangeWalk, RecordStore};
use crate::records::field_history::{record_history_suffix, RecordHistoryEntry};
use crate::schema::SchemaError;
pub(super) use crate::storage::format::history::QueueEntry;
use crate::storage::format::history::{self as format, record_prefix, HistoryChange};
use std::collections::BTreeMap;

/// The field history rows of one record group.
#[derive(Default)]
struct RecordHistoryRows {
    changes: Vec<HistoryChange>,
    released: u64,
    counts: BTreeMap<String, i64>,
}

fn invalid(error: impl std::fmt::Display) -> SchemaError {
    SchemaError::InvalidData(format!("record history: {error}"))
}

/// True for the key of a field history entry of a home.
pub(super) fn is_record_history(key: &str) -> bool {
    record_history_suffix(key).is_some()
}

/// Delete one entry and its queue entry, and lower the
/// entry count of its record.
fn release(
    key: &str,
    entry: &RecordHistoryEntry,
    out: &mut RecordHistoryRows,
) -> Result<(), SchemaError> {
    out.changes.push(format::release(key, entry)?);
    *out.counts.entry(entry.record.clone()).or_default() -= 1;
    out.released += 1;
    Ok(())
}

impl RecordStore {
    /// Stage legacy history deletes in pages in the same request as the record.
    /// The native transaction bounds all staged operations and discards them
    /// together if admission or any later part of the purge fails.
    pub(crate) async fn stage_purged_record_history(
        &self,
        records: &[&str],
    ) -> Result<u64, SchemaError> {
        if records.is_empty() {
            return Ok(0);
        }
        let counts = self.record_history_counts(records).await?;
        let mut released = 0;
        for (record, count) in records.iter().zip(counts) {
            if count > 0 {
                self.stage_purged_history_pages(record, count).await?;
                released += count;
            }
        }
        Ok(released)
    }

    async fn stage_purged_history_pages(
        &self,
        record: &str,
        count: u64,
    ) -> Result<(), SchemaError> {
        let prefix = record_prefix(record, None)?;
        let (start, end) = lastdb_model::kind_partition::exact_prefix_bounds(&prefix);
        let mut walk = RangeWalk::new(start.as_bytes(), end.as_bytes(), 100);
        let mut seen = 0;
        while !walk.done() {
            let rows = self.walk_step(&mut walk).await?.rows;
            if rows.is_empty() {
                continue;
            }
            let mut releases = Vec::with_capacity(rows.len());
            for (key, bytes) in rows {
                let (key, entry) = decode_row(key, &bytes)?;
                releases.push(format::release(&key, &entry)?);
            }
            seen += releases.len() as u64;
            self.format.stage_history(format::encode(releases)?).await?;
        }
        if seen != count {
            return Err(invalid(
                "the history count does not match the record's entries",
            ));
        }
        self.format
            .stage_history(format::encode(vec![HistoryChange::DeleteCount(
                record.to_owned(),
            )])?)
            .await
    }

    /// Release entries in one batch. The caller holds a write scope.
    async fn commit_history_release(
        &self,
        entries: &[(String, RecordHistoryEntry)],
        orphan_queue_keys: Vec<QueueEntry>,
    ) -> Result<u64, SchemaError> {
        let mut out = RecordHistoryRows::default();
        for (key, entry) in entries {
            release(key, entry, &mut out)?;
        }
        let mut changes = out.changes;
        changes.extend(
            orphan_queue_keys
                .into_iter()
                .map(HistoryChange::OrphanQueue),
        );
        if changes.is_empty() {
            return Ok(0);
        }
        changes.extend(self.record_history_count_changes(out.counts).await?);
        self.format.stage_history(format::encode(changes)?).await?;
        Ok(out.released)
    }

    /// Release the due entries of one expiry page. A queue entry whose history
    /// entry is gone is deleted with the batch.
    pub(super) async fn expire_record_history(
        &self,
        due: Vec<QueueEntry>,
        now: u64,
    ) -> Result<u64, SchemaError> {
        if due.is_empty() {
            return Ok(0);
        }
        let keys: Vec<String> = due.iter().map(|entry| entry.history.clone()).collect();
        let stored: Vec<Option<RecordHistoryEntry>> = self.main_store.get_items(&keys).await?;
        let mut entries = Vec::new();
        let mut orphans = Vec::new();
        for (queued, stored) in due.into_iter().zip(stored) {
            let Some(entry) = stored else {
                orphans.push(queued);
                continue;
            };
            if !entry.expired(now) || entry.queue_key.as_deref() != Some(queued.queue.as_str()) {
                return Err(invalid("the history queue does not match the entry expiry"));
            }
            entries.push((queued.history, entry));
        }
        self.commit_history_release(&entries, orphans).await
    }
}

fn decode_row(key: Vec<u8>, bytes: &[u8]) -> Result<(String, RecordHistoryEntry), SchemaError> {
    let key = String::from_utf8(key).map_err(invalid)?;
    let entry = serde_json::from_slice(bytes).map_err(invalid)?;
    Ok((key, entry))
}
