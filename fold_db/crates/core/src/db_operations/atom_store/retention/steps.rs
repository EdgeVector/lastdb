//! The steps of the janitor. Each step reads outside the write gate and writes
//! at most one batch.
//!
//! The janitor keeps a [`RetentionCursor`] between steps: the reader of the
//! queue (the buckets and their cursors) and the schedule of the safety net.
//! Two kinds of work share the janitor:
//!
//! - **Ordinary work** ([`AtomStore::release_step`]): release the due records of
//!   the oldest bucket that has some, and retire a bucket that is done.
//! - **The safety net** ([`AtomStore::net_step`]): register again a bucket that
//!   holds live entries and has no directory row.

use super::page::RELEASE_BATCH_RECORDS;
use crate::db_operations::atom_store::time_queue::{NetSchedule, QueueReader, TimeQueue};
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;
use crate::storage::disk_pack::transaction::blocked::{measure, Cost};
use std::time::Duration;

/// The kind of work that one step did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Lane {
    /// A batch of due records.
    #[default]
    Ordinary,
    /// The retirement of a queue bucket.
    Retire,
    /// A chunk of a sweep of the safety net.
    Net,
}

/// What one step did.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct StepOutcome {
    pub(crate) lane: Lane,
    pub(crate) released: u64,
    pub(crate) skipped: u64,
    /// The step deleted the directory row of a bucket.
    pub(crate) retired: bool,
    /// Buckets that a net chunk registered again.
    pub(crate) repaired: u64,
    /// How long the step kept writers waiting: the hold of the native write gate,
    /// and for a batch that seals the queue, the wait until it is durable
    /// (`storage::disk_pack::transaction::blocked`). The janitor rests for a share
    /// of this time.
    pub(crate) blocked: Duration,
    /// Write scopes the step opened. The janitor takes them off the count of the
    /// scopes of the process to tell whether another writer was there.
    pub(crate) scopes: u64,
}

impl StepOutcome {
    fn with_cost(self, cost: Cost) -> Self {
        Self {
            blocked: cost.blocked,
            scopes: cost.scopes,
            ..self
        }
    }
}

/// Slices that one net chunk probes. A chunk is one batch read and, for each
/// slice without a row, one bounded range read.
const NET_CHUNK_SLICES: usize = 16;

/// What the janitor keeps between steps. A restart loses it, and the janitor
/// then walks each live bucket once from its start.
#[derive(Debug)]
pub(crate) struct RetentionCursor {
    queue: TimeQueue,
    reader: QueueReader,
    net: NetSchedule,
}

impl RetentionCursor {
    pub(crate) fn new(queue: TimeQueue) -> Self {
        Self {
            queue,
            reader: QueueReader::new(queue, RELEASE_BATCH_RECORDS),
            net: NetSchedule::new(),
        }
    }

    /// Sweep the slices of the queue for buckets without a directory row, once in
    /// `every`, and now. `None` turns the safety net off.
    pub(crate) fn set_net_every(&mut self, every: Option<Duration>) {
        self.net.set_every(every);
    }

    /// Walk the live buckets again from their start, and read the directory
    /// again from its start.
    pub(crate) fn restart(&mut self) {
        self.reader.restart();
    }

    /// Buckets the reader knows.
    pub(crate) fn buckets(&self) -> usize {
        self.reader.len()
    }
}

impl AtomStore {
    /// A new cursor for the janitor of this store.
    pub(crate) fn retention_cursor(&self) -> RetentionCursor {
        RetentionCursor::new(self.retention_state().queue())
    }

    /// Read the directory of the queue: the buckets that have started.
    pub(crate) async fn refresh_queue(
        &self,
        cursor: &mut RetentionCursor,
        now_nanos: u64,
    ) -> Result<(), SchemaError> {
        cursor.reader.refresh(self, now_nanos).await
    }

    /// One batch of ordinary work: release one page of due records, or retire
    /// one bucket whose walk is complete. `None` means nothing is due.
    ///
    /// A bucket is retired only when its walk has read every key. The retirement is a guarded delete of
    /// the directory row (`retire_bucket`): it leaves the row if a writer added
    /// an entry after the reader read the row.
    pub(crate) async fn release_step(
        &self,
        cursor: &mut RetentionCursor,
        now_nanos: u64,
    ) -> Result<Option<StepOutcome>, SchemaError> {
        let (step, cost) = measure(self.read_and_release(cursor, now_nanos)).await;
        Ok(step?.map(|outcome| outcome.with_cost(cost)))
    }

    /// The work of [`Self::release_step`], without the measure of the time it blocks writers.
    async fn read_and_release(
        &self,
        cursor: &mut RetentionCursor,
        now_nanos: u64,
    ) -> Result<Option<StepOutcome>, SchemaError> {
        for bucket in cursor.reader.candidates(now_nanos) {
            loop {
                if cursor.reader.exhausted(bucket) {
                    let Some((key, token)) = cursor.reader.retire_token(bucket) else {
                        break;
                    };
                    let retired = self.retire_bucket(key, token).await?;
                    // The reader leaves the bucket in both cases. When the row
                    // stayed, a new entry changed it, and the next refresh adds
                    // the bucket again with a new walk.
                    cursor.reader.forget(bucket);
                    return Ok(Some(StepOutcome {
                        lane: Lane::Retire,
                        retired,
                        ..StepOutcome::default()
                    }));
                }
                let page = cursor.reader.read_due(self, bucket, now_nanos).await?;
                if page.rows.is_empty() {
                    // The walk ended, or it met a key that is not due.
                    let waits = page.waits();
                    cursor.reader.finish(page);
                    if waits {
                        break;
                    }
                    continue;
                }
                let records = self.due_record_keys(&page);
                if records.is_empty() {
                    cursor.reader.finish(page);
                    continue;
                }
                // A release that fails leaves the cursor at the page, so the
                // next step reads it again.
                let outcome = self.release_due_records(&records, now_nanos).await?;
                cursor.reader.finish(page);
                return Ok(Some(StepOutcome {
                    lane: Lane::Ordinary,
                    released: outcome.released,
                    skipped: outcome.skipped,
                    ..StepOutcome::default()
                }));
            }
        }
        Ok(None)
    }

    /// One chunk of the safety net: probe the next slices of the sweep, and
    /// register again each bucket that holds a live entry and has no directory
    /// row. A new sweep starts when the net is on and its time has come. `None`
    /// means no sweep is due.
    ///
    /// The probe covers the slices that can hold a live entry, by arithmetic
    /// (`TimeQueue::net_slices`), so it needs no scan. See `time_queue/net.rs`.
    pub(crate) async fn net_step(
        &self,
        cursor: &mut RetentionCursor,
        now_nanos: u64,
    ) -> Result<Option<StepOutcome>, SchemaError> {
        let (step, cost) = measure(self.sweep_chunk(cursor, now_nanos)).await;
        Ok(step?.map(|outcome| outcome.with_cost(cost)))
    }

    /// The work of [`Self::net_step`], without the measure of the time it blocks writers.
    async fn sweep_chunk(
        &self,
        cursor: &mut RetentionCursor,
        now_nanos: u64,
    ) -> Result<Option<StepOutcome>, SchemaError> {
        let queue = cursor.queue;
        let Some(slices) = cursor.net.next_slices(
            NET_CHUNK_SLICES,
            || queue.net_slices(now_nanos, self.retention_state().window_nanos()),
            || self.retention_state().note_net_sweep(),
        ) else {
            return Ok(None);
        };
        let chunk = match queue.mend_slices(self, &slices, now_nanos).await {
            Ok(chunk) => chunk,
            Err(error) => {
                cursor.net.abandon();
                return Err(error);
            }
        };
        for bucket in &chunk.unlisted {
            tracing::warn!(
                target: "fold_node::database",
                bucket,
                "a queue bucket held live entries and had no directory row; it is registered again"
            );
        }
        let repaired = chunk.unlisted.len() as u64;
        self.retention_state().note_buckets_repaired(repaired);
        Ok(Some(StepOutcome {
            lane: Lane::Net,
            repaired,
            ..StepOutcome::default()
        }))
    }
}
