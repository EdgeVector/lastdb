//! Field history of a home (decision D5).
//!
//! An entry keeps its own copy of one old field value and its stamp, at
//! `rh:{S}:{sha(record key)}:{field identity}:{id}`. `S` is the record
//! space of the schema, and `id` is the SHA-256 of the record key, the field
//! identity and the old stamp. The entry reuses the field policy (`ahp:`), the
//! entry count of each record (`ahk:`) and the expiry queue (`ahq:`/`ahb\0`) of
//! the field key history.

use super::*;
use crate::db_operations::record_store::{RecordChange, RecordStore};
use crate::records::field_history::{record_history_suffix, RecordHistoryEntry};

use crate::schema::{Schema, SchemaError};
use crate::storage::format::history::{self as format, HistoryCapture, HistoryChange};
pub(super) use crate::storage::format::history::{record_prefix, QueueEntry};
use lastdb_model::record_value::StoredRecord;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// A purge releases at most this many history entries of one record in its
/// own batch. A record with more refuses the purge until its field history is
/// purged (`POST /api/field-history`, `op: purge`).
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

/// One old field value that a write replaced or a delete removed.
struct Capture<'a> {
    change: &'a RecordChange,
    field: &'a str,
    identity: &'a str,
    source: &'a Arc<StoredRecord>,
    policy: FieldHistoryPolicy,
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
    /// The history rows of the changed records of one group, in the request's
    /// write scope: a capture of each overwritten or deleted field value with
    /// a history policy, and the release of every entry of a purged record.
    pub(crate) async fn record_history_rows(
        &self,
        schema: &Schema,
        changes: &[RecordChange],
        purges: &[bool],
    ) -> Result<RecordHistoryRows, SchemaError> {
        let mut out = RecordHistoryRows::default();
        let purged: Vec<&str> = changes
            .iter()
            .zip(purges)
            .filter(|(_, purge)| **purge)
            .map(|(change, _)| change.key.as_str())
            .collect();
        if !purged.is_empty() {
            self.release_purged_history(&purged, &mut out).await?;
        }
        let captures = self.history_captures(schema, changes, purges).await?;
        let now = self.queue_clock().now_nanos();
        for capture in &captures {
            capture_value(&schema.name, capture, now, &mut out);
        }
        let counts = std::mem::take(&mut out.counts);
        out.changes
            .extend(self.record_history_count_changes(counts).await?);
        Ok(out)
    }

    /// The old field values of the group whose field has a history policy. A
    /// rewrite of the same value and a purge capture nothing.
    async fn history_captures<'a>(
        &self,
        schema: &'a Schema,
        changes: &'a [RecordChange],
        purges: &[bool],
    ) -> Result<Vec<Capture<'a>>, SchemaError> {
        if !self.field_history_enabled().await? {
            return Ok(Vec::new());
        }
        let mut found = Vec::new();
        for (change, purge) in changes.iter().zip(purges) {
            let Some(before) = change.before.as_ref().filter(|_| !purge) else {
                continue;
            };
            for (field, old) in &before.fields {
                if change.after.fields.get(field) == Some(old) {
                    continue;
                }
                let Some(identity) = schema.field_hashes.get(field) else {
                    continue;
                };
                found.push(Capture {
                    change,
                    field,
                    identity,
                    source: before,
                    policy: FieldHistoryPolicy::Off,
                });
            }
        }
        if found.is_empty() {
            return Ok(found);
        }
        let mut identities: Vec<&str> = found.iter().map(|capture| capture.identity).collect();
        identities.sort_unstable();
        identities.dedup();
        let keys: Vec<String> = identities
            .iter()
            .map(|identity| format::policy_key(identity))
            .collect();
        let policies: Vec<Option<FieldHistoryPolicy>> = self.main_store.get_items(&keys).await?;
        let policies: HashMap<&str, FieldHistoryPolicy> = identities
            .into_iter()
            .zip(policies)
            .map(|(identity, policy)| (identity, policy.unwrap_or_default()))
            .collect();
        for capture in &mut found {
            capture.policy = policies[capture.identity];
        }
        found.retain(|capture| capture.policy != FieldHistoryPolicy::Off);
        Ok(found)
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
                    "the record has {count} field history entries; purge its field history \
                     (field-history purge) before the record"
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
        let mut walk = prefix_walk(prefix, None, 100)?;
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

/// Add one entry, its queue entry when it expires, and one to the entry count
/// of its record.
fn capture_value(schema: &str, capture: &Capture<'_>, now: u64, out: &mut RecordHistoryRows) {
    let record = capture.change.key.as_str();
    out.changes.push(HistoryChange::Capture(HistoryCapture {
        schema: schema.to_string(),
        record: record.to_string(),
        field: capture.field.to_string(),
        identity: capture.identity.to_string(),
        source: Arc::clone(capture.source),
        policy: capture.policy,
        now,
        replaced: capture.change.after.fields.contains_key(capture.field),
    }));
    *out.counts.entry(record.to_string()).or_default() += 1;
}

pub(super) fn decode_row(
    key: Vec<u8>,
    bytes: &[u8],
) -> Result<(String, RecordHistoryEntry), SchemaError> {
    let key = String::from_utf8(key).map_err(invalid)?;
    let entry = serde_json::from_slice(bytes).map_err(invalid)?;
    Ok((key, entry))
}
