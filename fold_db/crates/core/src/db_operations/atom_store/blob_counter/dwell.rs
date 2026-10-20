//! The dwell pass: delete a blob whose count is zero and whose dwell has passed.
//!
//! The queue is time-bucketed (`queue.rs`, `time_queue`). A pass keeps a
//! [`DwellCursor`], the reader of the queue: it lists the directory, then walks
//! the buckets that have started, oldest first, with one bounded range walk for
//! each bucket. The walk is the cursor. It moves over deleted keys and it never
//! goes back to the head of a hash, so the deleted entries of a long history cost
//! nothing to pass, and a bucket that is read to its end leaves the directory.
//!
//! One step ([`AtomStore::blob_dwell_step`]) does one of two things. It reads a
//! page of due entries of the oldest bucket that has some, outside the write
//! gate, and deletes them in one chunk ([`AtomStore::blob_dwell_delete`]): the
//! chunk enters the native write gate, reads each counter again, and in one batch
//! deletes the blob row, the counter and the queue entry of every blob that is
//! still at exactly zero. Or it retires a bucket whose walk has read every key,
//! with a guarded delete of the directory row. The read decides nothing. A
//! reference that arrived during the dwell raised the counter, so the chunk finds
//! a count above zero and deletes only the stale queue entry.
//!
//! The cursor moves after the work. A step that fails leaves it where it was, and
//! the next step reads the same keys. The gate is held for one chunk and released
//! between chunks, so a writer waits for at most the reads and the staging of a
//! chunk. The node paces the steps (`lastdb_node/src/ops/blob_dwell`).

use super::plan::BlobEvent;
use super::queue::BlobQueue;
use super::{blob_counter_key_of_id, blob_dwell_millis, blob_id, BlobCounter, BlobRead};
use crate::db_operations::atom_store::time_queue::QueueReader;
use crate::db_operations::atom_store::AtomStore;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use crate::storage::KvMutation;
use serde::Deserialize;
use std::collections::{BTreeSet, HashMap, HashSet};

/// Blobs one step deletes inside one hold of the write gate, at most. It is also
/// the page of the reader.
pub const DWELL_CHUNK: usize = 64;

/// The cursors of the live buckets start again from the start of their buckets,
/// once in this many passes. A clock that stepped back can put an entry below a
/// cursor, and the restart finds it. A bucket that the directory dropped is not
/// read again.
const FULL_PASS_EVERY: u64 = 60;

/// The reader of the dwell queue, with the state it keeps between steps and
/// passes. A restart of the node loses it, and the next pass then walks each live
/// bucket once from its start.
#[derive(Debug)]
pub struct DwellCursor {
    reader: QueueReader,
    passes: u64,
    /// Buckets in which a step handled the head of a page and left the rest of
    /// it for the next step. A bucket in this set is not retired.
    partial: BTreeSet<u64>,
}

impl DwellCursor {
    pub(super) fn new(queue: BlobQueue) -> Self {
        Self {
            reader: QueueReader::new(queue.layout(), DWELL_CHUNK),
            passes: 0,
            partial: BTreeSet::new(),
        }
    }

    /// Buckets the reader knows.
    #[must_use]
    pub fn buckets(&self) -> usize {
        self.reader.len()
    }

    /// A pass begins. Every 60th pass starts the live buckets from their start.
    fn begin_pass(&mut self) {
        if self.passes.is_multiple_of(FULL_PASS_EVERY) {
            self.reader.restart();
            self.partial.clear();
        }
        self.passes += 1;
    }
}

/// One queue entry whose dwell is over, as a step read it.
#[derive(Debug, Clone)]
struct DwellEntry {
    key: Vec<u8>,
    zero_since: u64,
    id: String,
    /// The blob reference. `None` for an entry whose key or value is malformed.
    blob: Option<String>,
}

/// What one step did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DwellStep {
    /// Blobs deleted: row, counter and queue entry.
    pub deleted: u64,
    /// Queue entries removed that no counter at zero backed.
    pub stale: u64,
    /// Blobs that a retired-intent root keeps. Their dwell starts again.
    pub renewed: u64,
    /// Queue entries whose counter does not decode. The entry is dropped and the
    /// blob stays: nothing can say that its count is zero.
    pub unreadable: u64,
    /// The step deleted the directory row of a bucket whose walk was complete.
    pub retired: bool,
}

impl DwellStep {
    /// Entries the step handled.
    #[must_use]
    pub fn handled(&self) -> u64 {
        self.deleted + self.stale + self.renewed + self.unreadable
    }
}

#[derive(Deserialize)]
struct QueueValue {
    blob: String,
}

enum Fate {
    Delete(BlobCounter),
    Renew(BlobCounter),
    Stale,
    Unreadable,
}

impl AtomStore {
    /// Start the one dwell pass of this store, or `None` when a pass is running or
    /// the last passes failed and the wait is not over.
    pub fn begin_blob_dwell_pass(&self) -> Option<super::report::DwellGuard> {
        self.blob_events.begin_pass()
    }

    /// The time that a reader of the queue passes: the clock of the blob counters
    /// minus the dwell, in nanoseconds. An entry is due when its time, the
    /// `zero_since` of its counter, is at or below it. The writers and the reader
    /// read one clock.
    fn blob_due_nanos(&self) -> u64 {
        BlobQueue::nanos(self.blob_clock_millis().saturating_sub(blob_dwell_millis()))
    }

    /// Begin a pass: count it, and read the directory of the queue.
    pub async fn blob_dwell_begin(&self, cursor: &mut DwellCursor) -> Result<(), SchemaError> {
        cursor.begin_pass();
        self.blob_events.note_pass(self.blob_clock_millis());
        self.blob_dwell_refresh(cursor).await
    }

    /// Read the directory of the queue: the buckets that have started. A pass
    /// calls it now and then, so a bucket that starts during a long pass gets its
    /// turn. It is a bounded range walk.
    pub async fn blob_dwell_refresh(&self, cursor: &mut DwellCursor) -> Result<(), SchemaError> {
        cursor.reader.refresh(self, self.blob_due_nanos()).await
    }

    /// One step: delete one chunk of due blobs, or retire one finished bucket.
    /// `max` is the size of the chunk, at most [`DWELL_CHUNK`]. `None` means no
    /// bucket has a due entry.
    ///
    /// A bucket is retired only when its walk has read every key and no step left
    /// the rest of a page of it for later. The retirement is a guarded delete of
    /// the directory row: it leaves the row if a writer added an entry after the
    /// reader read the row.
    pub async fn blob_dwell_step(
        &self,
        cursor: &mut DwellCursor,
        max: usize,
    ) -> Result<Option<DwellStep>, SchemaError> {
        let max = max.clamp(1, DWELL_CHUNK);
        let now = self.blob_clock_millis();
        let due = BlobQueue::nanos(now.saturating_sub(blob_dwell_millis()));
        let queue = self.blob_events.queue();
        for bucket in cursor.reader.candidates(due) {
            loop {
                if cursor.reader.exhausted(bucket) {
                    if cursor.partial.contains(&bucket) {
                        break;
                    }
                    let Some((key, token)) = cursor.reader.retire_token(bucket) else {
                        break;
                    };
                    let retired = self.retire_bucket(key, token).await?;
                    // The reader leaves the bucket in both cases. When the row
                    // stayed, a new entry changed it, and the next refresh adds
                    // the bucket again with a new walk.
                    cursor.reader.forget(bucket);
                    return Ok(Some(DwellStep {
                        retired,
                        ..DwellStep::default()
                    }));
                }
                let page = cursor.reader.read_due(self, bucket, due).await?;
                if page.rows.is_empty() {
                    // The walk ended, or it met a key that is not due.
                    let waits = page.waits();
                    cursor.reader.finish(page);
                    if waits {
                        break;
                    }
                    continue;
                }
                let take = page.rows.len().min(max);
                let entries: Vec<DwellEntry> = page.rows[..take]
                    .iter()
                    .map(|(key, value)| decode_entry(&queue, key, value))
                    .collect();
                let step = self.blob_dwell_delete(&entries, now).await?;
                if page.rows.len() > take {
                    // The chunk took the head of the page. The cursor goes to the
                    // first key it did not take, and the bucket is read again at
                    // once.
                    let next = page.rows[take].0.clone();
                    cursor.reader.finish_before(&page, &next, due);
                    cursor.partial.insert(bucket);
                } else {
                    cursor.reader.finish(page);
                    cursor.partial.remove(&bucket);
                }
                return Ok(Some(step));
            }
        }
        Ok(None)
    }

    /// Delete the blobs of one chunk of due entries. Takes the native write gate
    /// for this chunk only. Reads each counter again inside the gate.
    async fn blob_dwell_delete(
        &self,
        entries: &[DwellEntry],
        now: u64,
    ) -> Result<DwellStep, SchemaError> {
        if entries.is_empty() {
            return Ok(DwellStep::default());
        }
        let refs: Vec<String> = entries
            .iter()
            .filter_map(|entry| entry.blob.clone())
            .collect();
        let step = self
            .in_write_scope(async {
                // The native gate first, then the blob gates: the upload path
                // and the retired-intent roots use the same order.
                let _blobs = self.lock_liveness_blobs(&refs).await;
                self.dwell_chunk_in_gate(entries, now).await
            })
            .await?;
        self.blob_events.note_deleted(step.deleted, step.stale);
        tracing::info!(
            target: "fold_db::blob_dwell",
            deleted = step.deleted,
            stale = step.stale,
            renewed = step.renewed,
            unreadable = step.unreadable,
            "blob dwell chunk"
        );
        Ok(step)
    }

    async fn dwell_chunk_in_gate(
        &self,
        entries: &[DwellEntry],
        now: u64,
    ) -> Result<DwellStep, SchemaError> {
        let named: Vec<&DwellEntry> = entries.iter().filter(|e| e.blob.is_some()).collect();
        let keys: Vec<String> = named
            .iter()
            .map(|entry| blob_counter_key_of_id(&entry.id))
            .collect();
        let counters = self.read_blob_counters_at(&keys).await?;
        let refs: Vec<String> = named.iter().filter_map(|e| e.blob.clone()).collect();
        let retired = self.retired_intent_blob_roots(&refs).await?;
        let mut state: HashMap<&str, BlobRead> = HashMap::new();
        for (entry, counter) in named.iter().zip(counters) {
            state.insert(entry.id.as_str(), counter);
        }
        let absent = BlobRead::Absent;
        let mut main = Vec::new();
        let mut rows = Vec::new();
        let mut step = DwellStep::default();
        for entry in entries {
            let read = state.get(entry.id.as_str()).unwrap_or(&absent);
            match fate(entry, read, &retired) {
                Fate::Delete(counter) => {
                    main.extend(delete_mutations(entry, &counter));
                    rows.push(counter.blob.into_bytes());
                    step.deleted += 1;
                }
                Fate::Renew(counter) => {
                    main.extend(self.renew_mutations(entry, &counter, now)?);
                    step.renewed += 1;
                }
                Fate::Stale => {
                    main.push(KvMutation::delete(entry.key.clone()));
                    step.stale += 1;
                }
                Fate::Unreadable => {
                    // Keep the blob and its counter. Drop the entry, so the same
                    // entry does not come back at every pass.
                    self.blob_events.note(&entry.id, BlobEvent::Unreadable);
                    main.push(KvMutation::delete(entry.key.clone()));
                    step.unreadable += 1;
                }
            }
        }
        self.stage_dwell_writes(BlobQueue::keep_last_directory_rows(main), rows)
            .await?;
        Ok(step)
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

    /// A retired-intent root keeps the blob. Move it to a new dwell: the old
    /// entry goes, the counter gets the new `zero_since`, and the new entry comes
    /// with the directory row of its bucket.
    fn renew_mutations(
        &self,
        entry: &DwellEntry,
        counter: &BlobCounter,
        now: u64,
    ) -> Result<Vec<KvMutation>, SchemaError> {
        let renewed = BlobCounter {
            zero_since: Some(now),
            ..counter.clone()
        };
        let mut out = vec![
            KvMutation::delete(entry.key.clone()),
            super::commit::counter_put(&renewed)?,
        ];
        out.extend(self.blob_events.queue().put_mutations(now, &renewed.blob)?);
        Ok(out)
    }
}

/// The entry of a due row. A key or a value that does not read gives an entry
/// with no blob: the chunk deletes it as stale.
fn decode_entry(queue: &BlobQueue, key: &str, value: &[u8]) -> DwellEntry {
    let Some((zero_since, id)) = queue.parse_entry(key.as_bytes()) else {
        return DwellEntry {
            key: key.as_bytes().to_vec(),
            zero_since: 0,
            id: String::new(),
            blob: None,
        };
    };
    let blob = serde_json::from_slice::<QueueValue>(value)
        .ok()
        .map(|value| value.blob);
    DwellEntry {
        key: key.as_bytes().to_vec(),
        zero_since,
        id,
        blob,
    }
}

/// The decision for one due entry, from the counter read inside the gate.
fn fate(entry: &DwellEntry, read: &BlobRead, retired: &HashSet<String>) -> Fate {
    let counter = match read {
        BlobRead::Absent => return Fate::Stale,
        BlobRead::Unreadable => return Fate::Unreadable,
        BlobRead::Present(counter) => counter,
    };
    // A counter that names another blob than its key does would make the pass
    // delete the wrong row. Treat it as stale and leave the verifier to report it.
    if counter.refs != 0
        || counter.suspect
        || counter.zero_since != Some(entry.zero_since)
        || blob_id(&counter.blob) != entry.id
    {
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
