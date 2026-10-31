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
//! chunk. The node paces the steps (`lastdb_node/src/jobs/blob_dwell`).

use super::blob_dwell_millis;
use super::dwell_chunk::{decode_entry, DwellEntry};
use super::queue::BlobQueue;
use crate::db_operations::atom_store::time_queue::{NetSchedule, QueueReader};
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;
use crate::storage::disk_pack::transaction::blocked::measure;
use std::collections::BTreeSet;

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
    /// The keys one read asks for: the chunk of the steps.
    page: usize,
    passes: u64,
    /// Buckets in which a step handled the head of a page and left the rest of
    /// it for the next step. A bucket in this set is not retired.
    partial: BTreeSet<u64>,
    /// The schedule of the safety net (`net.rs`).
    pub(super) net: NetSchedule,
}

impl DwellCursor {
    pub(super) fn new(queue: BlobQueue, page: usize) -> Self {
        let page = page.clamp(1, DWELL_CHUNK);
        Self {
            reader: QueueReader::new(queue.layout(), page),
            page,
            passes: 0,
            partial: BTreeSet::new(),
            net: NetSchedule::new(),
        }
    }

    /// The keys one read asks for.
    pub(super) fn page(&self) -> usize {
        self.page
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

/// What one step did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DwellStep {
    /// Blobs deleted: row, counter and queue entry.
    pub deleted: u64,
    /// Queue entries removed that no counter at zero backed.
    pub stale: u64,
    /// Queue entries whose counter does not decode. The entry is dropped and the
    /// blob stays: nothing can say that its count is zero.
    pub unreadable: u64,
    /// The step deleted the directory row of a bucket whose walk was complete.
    pub retired: bool,
    /// How long the step kept writers waiting: the hold of the native write gate,
    /// and for a batch that seals the queue, the wait until it is durable
    /// (`storage::disk_pack::transaction::blocked`). The node rests for a share of
    /// this time.
    pub blocked: std::time::Duration,
    /// Write scopes the step opened. The node takes them off the count of the
    /// scopes of the process to tell whether another writer was there.
    pub scopes: u64,
}

impl DwellStep {
    /// Entries the step handled.
    #[must_use]
    pub fn handled(&self) -> u64 {
        self.deleted + self.stale + self.unreadable
    }
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
        let (step, cost) = measure(self.read_and_delete(cursor, max)).await;
        Ok(step?.map(|step| DwellStep {
            blocked: cost.blocked,
            scopes: cost.scopes,
            ..step
        }))
    }

    /// The work of [`Self::blob_dwell_step`], without the measure of the time it blocks writers.
    async fn read_and_delete(
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
                let step = self.blob_dwell_delete(&entries).await?;
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
}
