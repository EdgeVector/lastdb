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
use super::entry::{DrainMarker, QueueValue};
use super::page::{ReleaseOutcome, RELEASE_BATCH_RECORDS};
use crate::db_operations::atom_store::time_queue::{DuePage, QueueReader, TimeQueue};
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;
use crate::storage::disk_pack::transaction::hold::measure;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

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
    /// A chunk of a sweep of the safety net.
    Net,
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
    /// Buckets that a net chunk registered again.
    pub(crate) repaired: u64,
    /// How long the step held the native write gate. The janitor rests for a
    /// share of this time, not of the wall time of the step, which also holds
    /// the wait for the flush (`storage::disk_pack::transaction::hold`).
    pub(crate) held: Duration,
}

/// Pages of drain markers that one ordinary step reads before it returns. A page
/// of markers needs no write gate, so it costs no batch and no rest.
const MARKER_PAGES_PER_STEP: usize = 64;
/// Slices that one net chunk probes. A chunk is one batch read and, for each
/// slice without a row, one bounded range read.
const NET_CHUNK_SLICES: usize = 16;
/// A bucket whose marker found no room waits this long before the next read.
const RETRY_NANOS: u64 = 1_000_000_000;

/// What the janitor keeps between steps. A restart loses it, and the janitor
/// then walks each live bucket once from its start.
#[derive(Debug)]
pub(crate) struct RetentionCursor {
    queue: TimeQueue,
    reader: QueueReader,
    pending: VecDeque<PendingDrain>,
    net: Net,
}

/// The schedule of the safety net, and the sweep in progress.
#[derive(Debug)]
struct Net {
    /// The time between two sweeps. `None` turns the net off.
    every: Option<Duration>,
    /// The next sweep starts at or after this time. The first one is at once.
    next_at: Instant,
    /// Slices of the sweep in progress that are not probed yet.
    todo: VecDeque<u64>,
}

impl RetentionCursor {
    pub(crate) fn new(queue: TimeQueue) -> Self {
        Self {
            queue,
            reader: QueueReader::new(queue, RELEASE_BATCH_RECORDS),
            pending: VecDeque::new(),
            net: Net {
                every: None,
                next_at: Instant::now(),
                todo: VecDeque::new(),
            },
        }
    }

    /// Sweep the slices of the queue for buckets without a directory row, once in
    /// `every`, and now. `None` turns the safety net off.
    pub(crate) fn set_net_every(&mut self, every: Option<Duration>) {
        self.net.every = every;
        self.net.next_at = Instant::now();
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
    /// A page that holds drain markers keeps them as pending drains. This needs
    /// no write gate, so a page of markers costs no batch: the step goes on to the
    /// next page, up to [`MARKER_PAGES_PER_STEP`] pages, and the batch is the
    /// release of the first page that holds records.
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
        let (step, held) = measure(self.read_and_release(cursor, now_nanos)).await;
        Ok(step?.map(|outcome| StepOutcome { held, ..outcome }))
    }

    /// The work of [`Self::release_step`], without the measure of the gate hold.
    async fn read_and_release(
        &self,
        cursor: &mut RetentionCursor,
        now_nanos: u64,
    ) -> Result<Option<StepOutcome>, SchemaError> {
        let mut marker_pages = 0usize;
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
                if page.rows.is_empty() {
                    // The walk ended, or it met a key that is not due.
                    let waits = page.waits();
                    cursor.reader.finish(page);
                    if waits {
                        break;
                    }
                    continue;
                }
                let (records, markers, expired) = self.split_due_rows(&page, now_nanos);
                let refused = cursor.register(markers);
                let outcome = if records.is_empty() && expired.is_empty() {
                    ReleaseOutcome::default()
                } else {
                    self.release_due_records(&records, &expired, now_nanos)
                        .await?
                };
                if !expired.is_empty() {
                    tracing::info!(
                        target: "fold_node::database",
                        markers = expired.len(),
                        "conflict janitor dropped drain markers older than the retention window"
                    );
                }
                let stays = refused.is_some();
                match refused {
                    None => cursor.reader.finish(page),
                    Some(refused) => cursor.reader.finish_before(
                        &page,
                        &refused,
                        now_nanos.saturating_add(RETRY_NANOS),
                    ),
                }
                if !records.is_empty()
                    || !expired.is_empty()
                    || marker_pages >= MARKER_PAGES_PER_STEP
                {
                    return Ok(Some(StepOutcome {
                        lane: Lane::Ordinary,
                        released: outcome.released,
                        skipped: outcome.skipped,
                        ..StepOutcome::default()
                    }));
                }
                marker_pages += 1;
                if stays {
                    // The bucket waits for room among the pending drains.
                    break;
                }
            }
        }
        Ok(None)
    }

    /// The keys of the record entries of a page, its drain markers, and the keys
    /// of the markers that are too old to drain. An entry that does not decode,
    /// or whose key has no readable expiry, is counted and left alone: the cursor
    /// moves past it, and the entry stays.
    ///
    /// A marker lives at most one retention window. Every record of an erased slot
    /// was written before the erase, so its own queue entry is due one window after
    /// the erase at the latest, and the ordinary lane releases it. A marker that is
    /// older than that has nothing left to speed up. The janitor deletes it without
    /// a drain, so no marker outlives the range that the safety net probes (one
    /// window and one slice before now).
    fn split_due_rows(
        &self,
        page: &DuePage,
        now_nanos: u64,
    ) -> (Vec<String>, Vec<DrainMarker>, Vec<String>) {
        let queue = self.retention_state().queue();
        let window = self.retention_state().window_nanos();
        let mut records = Vec::new();
        let mut markers = Vec::new();
        let mut expired = Vec::new();
        for (key, value) in &page.rows {
            if queue.entry_expiry(key).is_none() {
                tracing::error!(%key, "retention queue key has no readable expiry");
                self.retention_state().note_undecodable();
                continue;
            }
            match QueueValue::decode(value) {
                Ok(QueueValue::Record(_)) => records.push(key.clone()),
                Ok(QueueValue::Drain(marker))
                    if marker.erased_at.saturating_add(window) < now_nanos =>
                {
                    expired.push(key.clone());
                }
                Ok(QueueValue::Drain(marker)) => markers.push(marker),
                Err(error) => {
                    tracing::error!(%key, %error, "retention queue entry does not decode");
                    self.retention_state().note_undecodable();
                }
            }
        }
        (records, markers, expired)
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
        let (step, held) = measure(self.sweep_chunk(cursor, now_nanos)).await;
        Ok(step?.map(|outcome| StepOutcome { held, ..outcome }))
    }

    /// The work of [`Self::net_step`], without the measure of the gate hold.
    async fn sweep_chunk(
        &self,
        cursor: &mut RetentionCursor,
        now_nanos: u64,
    ) -> Result<Option<StepOutcome>, SchemaError> {
        if cursor.net.todo.is_empty() {
            let Some(every) = cursor.net.every else {
                return Ok(None);
            };
            if Instant::now() < cursor.net.next_at {
                return Ok(None);
            }
            cursor.net.next_at = Instant::now() + every;
            let state = self.retention_state();
            cursor
                .net
                .todo
                .extend(cursor.queue.net_slices(now_nanos, state.window_nanos()));
            state.note_net_sweep();
        }
        let take = cursor.net.todo.len().min(NET_CHUNK_SLICES);
        let slices: Vec<u64> = cursor.net.todo.iter().take(take).copied().collect();
        let chunk = match cursor.queue.mend_slices(self, &slices, now_nanos).await {
            Ok(chunk) => chunk,
            Err(error) => {
                // Drop the sweep. The next one starts at the next time of the
                // schedule, so a failing net does not read again at once.
                cursor.net.todo.clear();
                return Err(error);
            }
        };
        cursor.net.todo.drain(..take);
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

    /// One batch of slot drains. `None` means the janitor holds no drain. A
    /// batch that fails sends the drain at fault to the back of the line
    /// (`drain_batch`).
    pub(crate) async fn drain_step(
        &self,
        cursor: &mut RetentionCursor,
    ) -> Result<Option<StepOutcome>, SchemaError> {
        if cursor.pending.is_empty() {
            return Ok(None);
        }
        let (batch, held) = measure(self.drain_batch(&mut cursor.pending)).await;
        let DrainOutcome {
            released,
            skipped,
            finished,
        } = batch?;
        Ok(Some(StepOutcome {
            lane: Lane::Drain,
            released,
            skipped,
            finished,
            held,
            ..StepOutcome::default()
        }))
    }
}
