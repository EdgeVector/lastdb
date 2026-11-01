//! Field history of a format 7 home (decision D5).
//!
//! An entry keeps its own copy of one old field value and its stamp, at
//! `{scope:}rh:{S}:{sha(record key)}:{field identity}:{id}`. `S` is the record
//! space of the schema, and `id` is the SHA-256 of the record key, the field
//! identity and the old stamp. The entry reuses the field policy (`ahp:`), the
//! entry count of each record (`ahk:`) and the expiry queue (`ahq:`/`ahb:`) of
//! the molecule history. A file blob pointer in the copied value holds one
//! blob reference: the capture and the release change the blob counter in the
//! batch of the entry.

use super::*;
use crate::db_operations::atom_store::blob_counter::BlobDeltas;
use crate::db_operations::atom_store::retention::RetentionEntry;
use crate::db_operations::atom_store::{AtomStore, RecordChange};
use crate::records::atom_history::{RecordHistoryEntry, RECORD_HISTORY_PREFIX};
use crate::schema::types::field::build_storage_key;
use crate::schema::{Schema, SchemaError};
use crate::storage::KvMutation;
use lastdb_model::record_space::RECORD_PREFIX;
use lastdb_model::record_value::FieldStamp;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

/// A purge releases at most this many history entries of one record in its
/// own batch. A record with more refuses the purge until its field history is
/// purged (`POST /api/atom-history`, `op: purge`).
const PURGE_RELEASE_CAP: u64 = 1_024;

/// The field history rows of one record group and their blob changes.
#[derive(Default)]
pub(crate) struct RecordHistoryRows {
    pub(crate) rows: Vec<KvMutation>,
    pub(crate) blobs: BlobDeltas,
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
    old: &'a Value,
    stamp: &'a FieldStamp,
    policy: FieldHistoryPolicy,
}

fn invalid(error: impl std::fmt::Display) -> SchemaError {
    SchemaError::InvalidData(format!("record history: {error}"))
}

/// `{scope:}rh:{S}:{sha(record key)}:`, and the field identity when given.
pub(super) fn record_prefix(record: &str, field: Option<&str>) -> Result<String, SchemaError> {
    let scope = crate::storage::current_keys::exact_scope(record)?;
    let bare = scope.map_or(Some(record), |scope| record.get(scope.len() + 1..));
    let space = bare
        .and_then(|bare| bare.strip_prefix(RECORD_PREFIX))
        .and_then(|rest| rest.split(':').next())
        .filter(|space| !space.is_empty())
        .ok_or_else(|| invalid("the location is not a record key"))?;
    let mut base = format!(
        "{RECORD_HISTORY_PREFIX}{space}:{}:",
        digest(record.as_bytes())
    );
    if let Some(field) = field {
        base.push_str(field);
        base.push(':');
    }
    Ok(build_storage_key(scope, &base))
}

/// True for the key of a field history entry of a format 7 home.
pub(super) fn is_record_history(key: &str) -> bool {
    let Ok(scope) = crate::storage::current_keys::exact_scope(key) else {
        return false;
    };
    let bare = scope.map_or(Some(key), |scope| key.get(scope.len() + 1..));
    bare.is_some_and(|bare| bare.starts_with(RECORD_HISTORY_PREFIX))
}

/// Delete one entry, its queue entry and its blob references, and lower the
/// entry count of its record.
pub(super) fn release(key: &str, entry: &RecordHistoryEntry, out: &mut RecordHistoryRows) {
    out.rows.push(KvMutation::delete(key.as_bytes().to_vec()));
    if let Some(queue) = &entry.queue_key {
        out.rows.push(KvMutation::delete(queue.as_bytes().to_vec()));
    }
    out.blobs.add(&entry.blobs(), -1);
    *out.counts.entry(entry.record.clone()).or_default() -= 1;
    out.released += 1;
}

impl AtomStore {
    /// The history rows of the changed records of one group, in the request's
    /// write scope: a capture of each overwritten or deleted field value with
    /// a history policy, and the release of every entry of a purged record.
    pub(crate) async fn record_history_rows(
        &self,
        schema: &Schema,
        scope: Option<&str>,
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
        let captures = self
            .history_captures(schema, scope, changes, purges)
            .await?;
        let now = self.retention_state().clock_nanos();
        for capture in &captures {
            capture_value(&schema.name, capture, now, &mut out)?;
        }
        let counts = std::mem::take(&mut out.counts);
        out.rows
            .extend(self.record_history_count_mutations(counts).await?);
        Ok(out)
    }

    /// The old field values of the group whose field has a history policy. A
    /// rewrite of the same value and a purge capture nothing.
    async fn history_captures<'a>(
        &self,
        schema: &'a Schema,
        scope: Option<&str>,
        changes: &'a [RecordChange],
        purges: &[bool],
    ) -> Result<Vec<Capture<'a>>, SchemaError> {
        if !self.atom_history_enabled().await? {
            return Ok(Vec::new());
        }
        let mut found = Vec::new();
        for (change, purge) in changes.iter().zip(purges) {
            let Some(before) = change.before.as_ref().filter(|_| !purge) else {
                continue;
            };
            for (field, old) in &before.fields {
                let stamp = before.stamps.get(field);
                if change.after.fields.get(field) == Some(old)
                    || change.after.stamps.get(field) == stamp
                {
                    continue;
                }
                let (Some(identity), Some(stamp)) = (schema.field_hashes.get(field), stamp) else {
                    continue;
                };
                found.push(Capture {
                    change,
                    field,
                    identity,
                    old,
                    stamp,
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
            .map(|identity| build_storage_key(scope, &format!("ahp:v1:{identity}")))
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
                     (atom-history purge) before the record"
                )));
            }
            for (key, entry) in self.history_entries(&record_prefix(record, None)?).await? {
                release(&key, &entry, out);
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
        orphan_queue_keys: Vec<String>,
    ) -> Result<u64, SchemaError> {
        let mut out = RecordHistoryRows::default();
        for (key, entry) in entries {
            release(key, entry, &mut out);
        }
        let mut batch = out.rows;
        batch.extend(
            orphan_queue_keys
                .into_iter()
                .map(|key| KvMutation::delete(key.into_bytes())),
        );
        if batch.is_empty() {
            return Ok(0);
        }
        batch.extend(self.record_history_count_mutations(out.counts).await?);
        batch.extend(self.blob_counter_mutations(out.blobs).await?);
        self.main_store.inner().batch_mutate(batch).await?;
        Ok(out.released)
    }

    /// Release the due entries of one expiry page. A queue entry whose history
    /// entry is gone is deleted with the batch.
    pub(super) async fn expire_record_history(
        &self,
        due: Vec<RetentionEntry>,
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
                orphans.push(queued.queue);
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

/// Add one entry, its queue entry when it expires, its blob references and
/// one to the entry count of its record.
fn capture_value(
    schema: &str,
    capture: &Capture<'_>,
    now: u64,
    out: &mut RecordHistoryRows,
) -> Result<(), SchemaError> {
    let record = capture.change.key.as_str();
    let identity =
        serde_json::to_vec(&(record, capture.identity, capture.stamp)).map_err(invalid)?;
    let key = format!(
        "{}{}",
        record_prefix(record, Some(capture.identity))?,
        digest(&identity)
    );
    let expires_at = history_expiry(capture.policy, now)?;
    let queue_key = match expires_at {
        Some(at) => {
            let id = digest(key.as_bytes());
            let queue_key = queue().entry_key(at, &id);
            let queued = serde_json::to_value(RetentionEntry {
                version: 1,
                history: key.clone(),
                conflict: None,
                dir: String::new(),
                queue: queue_key.clone(),
                slot: record.to_string(),
                created: now,
            })
            .map_err(invalid)?;
            for (row, value) in queue().rows(at, &id, &queued) {
                let bytes = serde_json::to_vec(&value).map_err(invalid)?;
                out.rows.push(KvMutation::put(row.into_bytes(), bytes));
            }
            Some(queue_key)
        }
        None => None,
    };
    let reason = if capture.change.after.fields.contains_key(capture.field) {
        "replace"
    } else {
        "delete"
    };
    let entry = RecordHistoryEntry {
        v: RecordHistoryEntry::VERSION,
        schema: schema.to_string(),
        field: capture.field.to_string(),
        field_identity: capture.identity.to_string(),
        record: record.to_string(),
        value: capture.old.clone(),
        stamp: capture.stamp.clone(),
        archived_at: now,
        expires_at,
        reason: reason.into(),
        queue_key,
    };
    out.blobs.add(&entry.blobs(), 1);
    let bytes = serde_json::to_vec(&entry).map_err(invalid)?;
    out.rows.push(KvMutation::put(key.into_bytes(), bytes));
    *out.counts.entry(record.to_string()).or_default() += 1;
    Ok(())
}

pub(super) fn decode_row(
    key: Vec<u8>,
    bytes: &[u8],
) -> Result<(String, RecordHistoryEntry), SchemaError> {
    let key = String::from_utf8(key).map_err(invalid)?;
    let entry = serde_json::from_slice(bytes).map_err(invalid)?;
    Ok((key, entry))
}

/// The expiry time of a history entry under `policy`, or `None` to keep it.
fn history_expiry(policy: FieldHistoryPolicy, now: u64) -> Result<Option<u64>, SchemaError> {
    Ok(match policy {
        FieldHistoryPolicy::Ttl { seconds } => Some(
            now.checked_add(
                seconds
                    .checked_mul(1_000_000_000)
                    .ok_or_else(|| SchemaError::InvalidData("history TTL overflow".into()))?,
            )
            .ok_or_else(|| SchemaError::InvalidData("history expiry overflow".into()))?,
        ),
        _ => None,
    })
}
