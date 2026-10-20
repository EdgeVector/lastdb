//! The steps of the janitor. Each step reads outside the write gate and writes
//! at most one batch.
//!
//! The janitor keeps a [`RetentionCursor`] between steps: the reader of the
//! queue (the buckets and their cursors) and the slot drains it has started.
//! Two kinds of work share the janitor:
//!
//! - **Ordinary work** ([`AtomStore::release_step`]): release the due records of
//!   the oldest bucket that has some, and retire a bucket that is done.
//! - **Drains** ([`AtomStore::drain_step`]): release the records of an erased
//!   slot, one batch at a time.
//!
//! The janitor alternates them, a few batches of one kind and then a few of the
//! other, so a long drain cannot hold back the due records of the queue, and a
//! long backlog cannot hold back a drain.

use super::drain::{DrainOutcome, PendingDrain, MAX_PENDING_DRAINS};
use super::entry::DrainMarker;
use super::page::RELEASE_BATCH_RECORDS;
use crate::db_operations::atom_store::time_queue::{QueueReader, TimeQueue};
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;
use std::collections::VecDeque;

/// The kind of work that one step did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Lane {
    /// A batch of due records.
    #[default]
    Ordinary,
    /// A batch of a slot drain.
    Drain,
    /// The retirement of a queue bucket.
    Retire,
}

/// What one step did.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct StepOutcome {
    pub(crate) lane: Lane,
    pub(crate) released: u64,
    pub(crate) skipped: u64,
    /// Drains that the batch finished.
    pub(crate) finished: u64,
    /// The step deleted the directory row of a bucket.
    pub(crate) retired: bool,
}

/// A bucket whose marker found no room waits this long before the next read.
const RETRY_NANOS: u64 = 1_000_000_000;

/// What the janitor keeps between steps. A restart loses it, and the janitor
/// then walks each live bucket once from its start.
#[derive(Debug)]
pub(crate) struct RetentionCursor {
    queue: TimeQueue,
    reader: QueueReader,
    pending: VecDeque<PendingDrain>,
}

impl RetentionCursor {
    pub(crate) fn new(queue: TimeQueue) -> Self {
        Self {
            queue,
            reader: QueueReader::new(queue, RELEASE_BATCH_RECORDS),
            pending: VecDeque::new(),
        }
    }

    /// Walk the live buckets again from their start, and read the directory
    /// again from its start. The slot drains keep their cursors.
    pub(crate) fn restart(&mut self) {
        self.reader.restart();
    }

    /// Slot drains that are not finished.
    pub(crate) fn pending_drains(&self) -> usize {
        self.pending.len()
    }

    /// Buckets the reader knows.
    pub(crate) fn buckets(&self) -> usize {
        self.reader.len()
    }

    /// Keep the markers of a page as pending drains, in order. A marker that is
    /// pending already is not added again. When the janitor holds
    /// [`MAX_PENDING_DRAINS`] drains, the first marker that finds no room is
    /// returned: the cursor of its bucket must stay at it.
    fn register(&mut self, markers: Vec<DrainMarker>) -> Option<String> {
        for marker in markers {
            if self
                .pending
                .iter()
                .any(|drain| drain.marker.queue == marker.queue)
            {
                continue;
            }
            if self.pending.len() >= MAX_PENDING_DRAINS {
                return Some(marker.queue);
            }
            let bucket = self.queue.entry_bucket(&marker.queue);
            self.pending.push_back(PendingDrain::new(marker, bucket));
        }
        None
    }

    /// A marker in `bucket` is not drained yet. The bucket stays until it is.
    fn holds_marker(&self, bucket: u64) -> bool {
        self.pending
            .iter()
            .any(|drain| drain.bucket == Some(bucket))
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
    /// A bucket is retired only when its walk has read every key, and no slot
    /// drain still holds a marker in it. The retirement is a guarded delete of
    /// the directory row (`retire_bucket`): it leaves the row if a writer added
    /// an entry after the reader read the row.
    pub(crate) async fn release_step(
        &self,
        cursor: &mut RetentionCursor,
        now_nanos: u64,
    ) -> Result<Option<StepOutcome>, SchemaError> {
        for bucket in cursor.reader.candidates(now_nanos) {
            loop {
                if cursor.reader.exhausted(bucket) {
                    if cursor.holds_marker(bucket) {
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
                    return Ok(Some(StepOutcome {
                        lane: Lane::Retire,
                        retired,
                        ..StepOutcome::default()
                    }));
                }
                let page = cursor.reader.read_due(self, bucket, now_nanos).await?;
                if page.keys.is_empty() {
                    // The walk ended, or it met a key that is not due.
                    let waits = page.waits();
                    cursor.reader.finish(page);
                    if waits {
                        break;
                    }
                    continue;
                }
                let outcome = self.release_due_records(&page.keys, now_nanos).await?;
                match cursor.register(outcome.markers) {
                    None => cursor.reader.finish(page),
                    Some(refused) => cursor.reader.finish_before(
                        &page,
                        &refused,
                        now_nanos.saturating_add(RETRY_NANOS),
                    ),
                }
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

    /// One batch of slot drains. `None` means the janitor holds no drain. A
    /// batch that fails sends the first drain to the back, so one bad slot cannot
    /// hold back the others.
    pub(crate) async fn drain_step(
        &self,
        cursor: &mut RetentionCursor,
    ) -> Result<Option<StepOutcome>, SchemaError> {
        if cursor.pending.is_empty() {
            return Ok(None);
        }
        match self.drain_batch(&mut cursor.pending).await {
            Ok(DrainOutcome {
                released,
                skipped,
                finished,
            }) => Ok(Some(StepOutcome {
                lane: Lane::Drain,
                released,
                skipped,
                finished,
                ..StepOutcome::default()
            })),
            Err(error) => {
                cursor.pending.rotate_left(1);
                Err(error)
            }
        }
    }
}
