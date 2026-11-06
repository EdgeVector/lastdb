//! Release field history that was written by an earlier LastDB version.

use crate::db_operations::record_store::{RangeWalk, RecordStore};
use crate::records::field_history::{record_history_suffix, RecordHistoryEntry};
use crate::schema::SchemaError;
pub(super) use crate::storage::format::history::QueueEntry;
use crate::storage::format::history::{self as format, record_prefix, HistoryChange};
use std::collections::BTreeMap;

/// Bound the old-value rows that can join a single record purge.
const PURGE_RELEASE_CAP: u64 = 1_024;

/// The field history rows of one record group.
#[derive(Default)]
pub(crate) struct RecordHistoryRows {
    pub(crate) changes: Vec<HistoryChange>,
    /// The entries that a purge released.
    pub(crate) released: u64,
    /// The change of the entry count of each record key.
    pub(super) counts: BTreeMap<String, i64>,
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
pub(super) fn release(
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
    /// Join legacy history cleanup to the same commit as each record purge.
    pub(crate) async fn record_history_releases(
        &self,
        records: &[&str],
    ) -> Result<RecordHistoryRows, SchemaError> {
        let mut out = RecordHistoryRows::default();
        if !records.is_empty() {
            self.release_purged_history(records, &mut out).await?;
            let counts = std::mem::take(&mut out.counts);
            out.changes
                .extend(self.record_history_count_changes(counts).await?);
        }
        Ok(out)
    }

    /// Release every history entry of each purged record in the purge batch.
    async fn release_purged_history(
        &self,
        records: &[&str],
        out: &mut RecordHistoryRows,
    ) -> Result<(), SchemaError> {
        let counts = self.record_history_counts(records).await?;
        for (record, count) in records.iter().zip(counts) {
            if count == 0 {
                continue;
            }
            if count > PURGE_RELEASE_CAP {
                return Err(invalid(format!(
                    "the record has {count} legacy history entries, above the bounded purge limit"
                )));
            }
            for (key, entry) in self.history_entries(&record_prefix(record, None)?).await? {
                release(&key, &entry, out)?;
            }
        }
        Ok(())
    }

    /// Every entry under one prefix, read in bounded steps.
    async fn history_entries(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, RecordHistoryEntry)>, SchemaError> {
        let (start, end) = lastdb_model::kind_partition::exact_prefix_bounds(prefix);
        let mut walk = RangeWalk::new(start.as_bytes(), end.as_bytes(), 100);
        let mut entries = Vec::new();
        while !walk.done() {
            for (key, bytes) in self.walk_step(&mut walk).await?.rows {
                entries.push(decode_row(key, &bytes)?);
            }
        }
        Ok(entries)
    }

    /// Release entries in one batch. The caller holds a write scope.
    pub(super) async fn commit_history_release(
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
