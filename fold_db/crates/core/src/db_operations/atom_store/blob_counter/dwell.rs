//! The dwell pass: delete a blob whose count is zero and whose dwell has passed.
//!
//! A pass has two steps. [`AtomStore::blob_dwell_scan`] reads the oldest page of
//! the queue, outside the write gate, and names the entries whose dwell is over.
//! [`AtomStore::blob_dwell_delete`] takes a small chunk of them, enters the
//! native write gate, reads each counter again, and in one batch deletes the
//! blob row, the counter and the queue entry of every blob that is still at
//! exactly zero. The scan decides nothing. A reference that arrived during the
//! dwell raised the counter, so the chunk finds a count above zero and deletes
//! only the stale queue entry.
//!
//! The gate is held for one chunk and released between chunks, so a writer waits
//! for at most the reads and the staging of a chunk.

use super::commit::{counter_put, queue_put};
use super::{blob_counter_key_of_id, blob_dwell_millis, parse_blob_queue_key, BlobCounter};
use crate::db_operations::atom_store::{AtomStore, RangeWalk};
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use crate::storage::KvMutation;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};

/// Entries one scan reads from the head of the queue.
pub const DWELL_PAGE: usize = 256;

/// Blobs one chunk deletes inside one hold of the write gate.
pub const DWELL_CHUNK: usize = 64;

/// One queue entry whose dwell is over, as the scan read it.
#[derive(Debug, Clone)]
pub struct DwellEntry {
    key: Vec<u8>,
    zero_since: u64,
    id: String,
    /// The blob reference. `None` for an entry whose key or value is malformed.
    blob: Option<String>,
}

/// What one scan read from the queue.
#[derive(Debug, Default)]
pub struct DwellScan {
    /// Entries whose dwell is over, oldest first.
    pub due: Vec<DwellEntry>,
    /// Entries in the page that the scan read.
    pub depth: usize,
    /// The page was full: the queue may hold more.
    pub capped: bool,
    /// When the oldest entry that is not due becomes due, in unix milliseconds.
    pub next_due_unix_ms: Option<u64>,
}

/// What one chunk did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DwellChunk {
    /// Blobs deleted: row, counter and queue entry.
    pub deleted: u64,
    /// Queue entries removed that no counter at zero backed.
    pub stale: u64,
    /// Blobs that a retired-intent root keeps. Their dwell starts again.
    pub renewed: u64,
}

#[derive(Deserialize)]
struct QueueValue {
    blob: String,
}

enum Fate {
    Delete(BlobCounter),
    Renew(BlobCounter),
    Stale,
}

impl AtomStore {
    /// Start the one dwell pass of this store, or `None` when a pass is running.
    pub fn begin_blob_dwell_pass(&self) -> Option<super::report::DwellGuard> {
        self.blob_events.begin_pass()
    }

    /// Read the oldest page of the queue and name the entries that are due.
    /// One range read under one hash. It writes nothing.
    pub async fn blob_dwell_scan(&self, now: u64) -> Result<DwellScan, SchemaError> {
        let dwell = blob_dwell_millis();
        // A delete leaves a tombstone, and one read call may examine at most
        // 10,000 keys, deleted keys included. The oldest entries are the ones
        // that go, so the head of the range fills with tombstones. A bounded walk
        // cuts the range when a read hits the limit, so the scan still finds the
        // oldest live entries.
        let (start, end) = super::queue_range();
        let mut walk = RangeWalk::new(&start, &end, DWELL_PAGE);
        let mut rows = Vec::new();
        while rows.is_empty() && !walk.done() {
            rows = self.walk_step(&mut walk).await?.rows;
        }
        let depth = rows.len();
        let mut scan = DwellScan {
            depth,
            capped: depth == DWELL_PAGE,
            ..DwellScan::default()
        };
        for (key, value) in rows {
            let Some((zero_since, id)) = parse_blob_queue_key(&key) else {
                scan.due.push(DwellEntry {
                    key,
                    zero_since: 0,
                    id: String::new(),
                    blob: None,
                });
                continue;
            };
            let due_at = zero_since.saturating_add(dwell);
            if due_at > now {
                scan.next_due_unix_ms = Some(due_at);
                break;
            }
            let blob = serde_json::from_slice::<QueueValue>(&value)
                .ok()
                .map(|value| value.blob);
            scan.due.push(DwellEntry {
                key,
                zero_since,
                id,
                blob,
            });
        }
        self.blob_events
            .note_pass(depth as u64, scan.capped, scan.next_due_unix_ms, now);
        Ok(scan)
    }

    /// Delete the blobs of one chunk of due entries. Takes the native write gate
    /// for this chunk only. Reads each counter again inside the gate.
    pub async fn blob_dwell_delete(
        &self,
        entries: &[DwellEntry],
        now: u64,
    ) -> Result<DwellChunk, SchemaError> {
        if entries.is_empty() {
            return Ok(DwellChunk::default());
        }
        let refs: Vec<String> = entries
            .iter()
            .filter_map(|entry| entry.blob.clone())
            .collect();
        let chunk = self
            .in_write_scope(async {
                // The native gate first, then the blob gates: the upload path
                // and the retired-intent roots use the same order.
                let _blobs = self.lock_liveness_blobs(&refs).await;
                self.dwell_chunk_in_gate(entries, now).await
            })
            .await?;
        self.blob_events
            .note_deleted(chunk.deleted, chunk.stale + chunk.renewed);
        tracing::info!(
            target: "fold_db::blob_dwell",
            deleted = chunk.deleted,
            stale = chunk.stale,
            renewed = chunk.renewed,
            "blob dwell chunk"
        );
        Ok(chunk)
    }

    async fn dwell_chunk_in_gate(
        &self,
        entries: &[DwellEntry],
        now: u64,
    ) -> Result<DwellChunk, SchemaError> {
        let named: Vec<&DwellEntry> = entries.iter().filter(|e| e.blob.is_some()).collect();
        let keys: Vec<String> = named
            .iter()
            .map(|entry| blob_counter_key_of_id(&entry.id))
            .collect();
        let counters = self.read_blob_counters_at(&keys).await?;
        let refs: Vec<String> = named.iter().filter_map(|e| e.blob.clone()).collect();
        let retired = self.retired_intent_blob_roots(&refs).await?;
        let mut state: HashMap<&str, Option<BlobCounter>> = HashMap::new();
        for (entry, counter) in named.iter().zip(counters) {
            state.insert(entry.id.as_str(), counter);
        }
        let mut main = Vec::new();
        let mut rows = Vec::new();
        let mut chunk = DwellChunk::default();
        for entry in entries {
            let counter = state.get(entry.id.as_str()).and_then(Option::as_ref);
            match fate(entry, counter, &retired) {
                Fate::Delete(counter) => {
                    main.extend(delete_mutations(entry, &counter));
                    rows.push(counter.blob.into_bytes());
                    chunk.deleted += 1;
                }
                Fate::Renew(counter) => {
                    main.extend(renew_mutations(entry, &counter, now)?);
                    chunk.renewed += 1;
                }
                Fate::Stale => {
                    main.push(KvMutation::delete(entry.key.clone()));
                    chunk.stale += 1;
                }
            }
        }
        self.stage_dwell_writes(main, rows).await?;
        Ok(chunk)
    }

    async fn stage_dwell_writes(
        &self,
        main: Vec<KvMutation>,
        rows: Vec<Vec<u8>>,
    ) -> Result<(), SchemaError> {
        if !rows.is_empty() {
            let store = self.namespaced_store.as_ref().ok_or_else(|| {
                SchemaError::InvalidData("blob dwell requires a native store".into())
            })?;
            store
                .open_namespace("cas_blobs")
                .await
                .map_err(|error| SchemaError::InvalidData(format!("open cas_blobs: {error}")))?
                .batch_delete(rows)
                .await
                .map_err(|error| SchemaError::InvalidData(format!("delete blob rows: {error}")))?;
        }
        if !main.is_empty() {
            self.main_store
                .inner()
                .batch_mutate(main)
                .await
                .map_err(|error| SchemaError::InvalidData(format!("blob dwell batch: {error}")))?;
        }
        Ok(())
    }
}

/// The decision for one due entry, from the counter read inside the gate.
fn fate(entry: &DwellEntry, counter: Option<&BlobCounter>, retired: &HashSet<String>) -> Fate {
    let Some(counter) = counter else {
        return Fate::Stale;
    };
    if counter.refs != 0 || counter.suspect || counter.zero_since != Some(entry.zero_since) {
        return Fate::Stale;
    }
    if retired.contains(&counter.blob) {
        return Fate::Renew(counter.clone());
    }
    Fate::Delete(counter.clone())
}

/// Counter, queue entry and the resident copy of the blob row. The caller
/// deletes the `cas_blobs` row.
fn delete_mutations(entry: &DwellEntry, counter: &BlobCounter) -> Vec<KvMutation> {
    vec![
        KvMutation::delete(blob_counter_key_of_id(&entry.id).into_bytes()),
        KvMutation::delete(entry.key.clone()),
        KvMutation::delete(
            build_storage_key(None, &format!("cas_blob:{}", counter.blob)).into_bytes(),
        ),
    ]
}

/// A retired-intent root keeps the blob. Move it to a new dwell.
fn renew_mutations(
    entry: &DwellEntry,
    counter: &BlobCounter,
    now: u64,
) -> Result<Vec<KvMutation>, SchemaError> {
    let renewed = BlobCounter {
        zero_since: Some(now),
        ..counter.clone()
    };
    Ok(vec![
        KvMutation::delete(entry.key.clone()),
        counter_put(&renewed)?,
        queue_put(&renewed.blob, now)?,
    ])
}
