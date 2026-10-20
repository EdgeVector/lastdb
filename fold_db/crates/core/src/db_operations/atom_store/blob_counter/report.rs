//! What the blob planner and the dwell pass report: totals, the last queue
//! reading, and the blobs whose counter disagreed.
//!
//! The planner never hides a counter fault. It keeps the blob, marks the counter
//! `suspect`, and records the event here. `lastdb status` shows these totals.

use super::dwell::DwellCursor;
use super::plan::BlobEvent;
use super::queue::BlobQueue;
use serde::Serialize;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// How many recent fault events the report keeps.
const RECENT_FAULTS: usize = 32;
/// The wait after the first pass that failed. It doubles with each pass in a row
/// that fails.
const BACKOFF_BASE: Duration = Duration::from_secs(30);
/// The longest wait after a pass that failed.
const BACKOFF_MAX: Duration = Duration::from_secs(600);

/// Totals since the node started, the last queue reading, and the recent faults.
#[derive(Debug, Default)]
pub(crate) struct BlobEvents {
    created: AtomicU64,
    registered: AtomicU64,
    zero_reached: AtomicU64,
    cancelled: AtomicU64,
    underflows: AtomicU64,
    dangling_removals: AtomicU64,
    late_references: AtomicU64,
    unreadable_counters: AtomicU64,
    blobs_deleted: AtomicU64,
    stale_entries: AtomicU64,
    passes: AtomicU64,
    last_pass_unix_ms: AtomicU64,
    running: AtomicBool,
    recent_faults: Mutex<VecDeque<String>>,
    pub(super) clock: super::clock::BlobClock,
    /// The layout of the dwell queue, fixed for the life of the store.
    queue: BlobQueue,
    /// The reader of the dwell queue. It lives between passes, so a pass goes on
    /// where the last one stopped.
    cursor: Mutex<Option<DwellCursor>>,
    /// Passes in a row that ended with an error.
    failures: AtomicU64,
    last_error: Mutex<Option<String>>,
    retry_not_before: Mutex<Option<Instant>>,
}

/// Keeps the single dwell pass of a store. Dropping it ends the pass.
pub struct DwellGuard {
    events: Arc<BlobEvents>,
}

impl DwellGuard {
    /// The reader of the dwell queue: the one the last pass left, or a new one.
    #[must_use]
    pub fn take_cursor(&self) -> DwellCursor {
        self.events
            .cursor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .unwrap_or_else(|| DwellCursor::new(self.events.queue))
    }

    /// Keep the reader for the next pass.
    pub fn put_cursor(&self, cursor: DwellCursor) {
        *self
            .events
            .cursor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(cursor);
    }

    /// Record how the pass ended. A pass that failed makes the next ones wait,
    /// and the wait doubles with each pass in a row that fails (30 s to 10 min).
    /// A good pass clears the error.
    pub fn finish(&self, error: Option<String>) {
        self.events.note_result(error);
    }
}

impl Drop for DwellGuard {
    fn drop(&mut self) {
        self.events.running.store(false, Ordering::Release);
    }
}

impl BlobEvents {
    pub(super) fn note(&self, blob: &str, event: BlobEvent) {
        let total = match event {
            BlobEvent::Created => &self.created,
            BlobEvent::Registered => &self.registered,
            BlobEvent::Cancelled => &self.cancelled,
            BlobEvent::ZeroReached => &self.zero_reached,
            BlobEvent::Underflow => &self.underflows,
            BlobEvent::Dangling => &self.dangling_removals,
            BlobEvent::LateReference => &self.late_references,
            BlobEvent::Unreadable => &self.unreadable_counters,
        };
        total.fetch_add(1, Ordering::Relaxed);
        let name = match event {
            BlobEvent::Underflow => "underflow",
            BlobEvent::Dangling => "dangling_removal",
            BlobEvent::LateReference => "late_reference",
            BlobEvent::Unreadable => "unreadable_counter",
            _ => return,
        };
        tracing::error!(blob = %blob, event = name, "blob counter fault; the blob stays");
        let mut recent = self
            .recent_faults
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if recent.len() == RECENT_FAULTS {
            recent.pop_front();
        }
        recent.push_back(format!("{name}:{blob}"));
    }

    pub(super) fn note_deleted(&self, count: u64, stale: u64) {
        self.blobs_deleted.fetch_add(count, Ordering::Relaxed);
        self.stale_entries.fetch_add(stale, Ordering::Relaxed);
    }

    /// A dwell pass began.
    pub(super) fn note_pass(&self, now: u64) {
        self.passes.fetch_add(1, Ordering::Relaxed);
        self.last_pass_unix_ms.store(now, Ordering::Relaxed);
    }

    /// The layout of the dwell queue.
    pub(crate) fn queue(&self) -> BlobQueue {
        self.queue
    }

    fn note_result(&self, error: Option<String>) {
        let mut retry = self
            .retry_not_before
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let failed = error.is_some();
        *self
            .last_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = error;
        if failed {
            let failures = self.failures.fetch_add(1, Ordering::Relaxed) + 1;
            let shift = u32::try_from(failures.saturating_sub(1).min(10)).unwrap_or(10);
            *retry =
                Some(Instant::now() + BACKOFF_BASE.saturating_mul(1 << shift).min(BACKOFF_MAX));
        } else {
            self.failures.store(0, Ordering::Relaxed);
            *retry = None;
        }
    }

    /// Start the one pass of this store, or `None` when a pass is running or the
    /// last passes failed and the wait is not over.
    pub(super) fn begin_pass(self: &Arc<Self>) -> Option<DwellGuard> {
        let waiting = self
            .retry_not_before
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some_and(|at| Instant::now() < at);
        if waiting {
            return None;
        }
        self.running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| DwellGuard {
                events: Arc::clone(self),
            })
    }

    /// A copy of the totals for status output.
    pub(crate) fn snapshot(&self) -> BlobCounterReport {
        let nonzero = |cell: &AtomicU64| Some(cell.load(Ordering::Relaxed)).filter(|v| *v != 0);
        BlobCounterReport {
            counters_created: self.created.load(Ordering::Relaxed),
            uploads_registered: self.registered.load(Ordering::Relaxed),
            zero_reached: self.zero_reached.load(Ordering::Relaxed),
            dwell_cancelled: self.cancelled.load(Ordering::Relaxed),
            blobs_deleted: self.blobs_deleted.load(Ordering::Relaxed),
            stale_queue_entries: self.stale_entries.load(Ordering::Relaxed),
            underflows: self.underflows.load(Ordering::Relaxed),
            dangling_removals: self.dangling_removals.load(Ordering::Relaxed),
            late_references: self.late_references.load(Ordering::Relaxed),
            unreadable_counters: self.unreadable_counters.load(Ordering::Relaxed),
            dwell_passes: self.passes.load(Ordering::Relaxed),
            pass_failures: self.failures.load(Ordering::Relaxed),
            last_error: self
                .last_error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
            retry_in_secs: self
                .retry_not_before
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .map(|at| at.saturating_duration_since(Instant::now()).as_secs()),
            next_due_unix_ms: None,
            queue_depth: 0,
            queue_depth_capped: false,
            queue_buckets: None,
            last_pass_unix_ms: nonzero(&self.last_pass_unix_ms),
            dwell_secs: super::blob_dwell_millis() / 1000,
            recent_faults: self
                .recent_faults
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .cloned()
                .collect(),
        }
    }
}

/// The blob planner report in `lastdb status`.
#[derive(Debug, Clone, Default, Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct BlobCounterReport {
    pub counters_created: u64,
    pub uploads_registered: u64,
    pub zero_reached: u64,
    pub dwell_cancelled: u64,
    pub blobs_deleted: u64,
    pub stale_queue_entries: u64,
    pub underflows: u64,
    pub dangling_removals: u64,
    /// A reference raised a counter that did not exist, for a blob whose row is
    /// gone. The write went through and the atom names a blob that is not there.
    #[serde(default)]
    pub late_references: u64,
    /// A counter did not decode. The planner marked it `suspect`, or the dwell
    /// pass skipped it.
    #[serde(default)]
    pub unreadable_counters: u64,
    pub dwell_passes: u64,
    /// Passes in a row that ended with an error. The next pass waits longer.
    #[serde(default)]
    pub pass_failures: u64,
    #[serde(default)]
    pub last_error: Option<String>,
    /// Seconds until the next pass may start, after a pass that failed.
    #[serde(default)]
    pub retry_in_secs: Option<u64>,
    /// Entries in the dwell queue, counted by its buckets with bounded reads,
    /// up to a cap (`queue_depth_capped`). `lastdb status` fills it.
    #[serde(default)]
    pub queue_depth: u64,
    /// The queue holds at least `queue_depth` entries.
    #[serde(default)]
    pub queue_depth_capped: bool,
    /// Buckets in the directory of the queue. `None` when the directory could
    /// not be read.
    #[serde(default)]
    pub queue_buckets: Option<u64>,
    pub next_due_unix_ms: Option<u64>,
    pub last_pass_unix_ms: Option<u64>,
    pub dwell_secs: u64,
    pub recent_faults: Vec<String>,
}

/// What the counter store says about one blob, for operators.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BlobCounterView {
    pub blob: String,
    /// The count, or `None` when the blob has no counter.
    pub refs: Option<u64>,
    pub zero_since: Option<u64>,
    pub suspect: bool,
    /// The queue holds an entry for the blob at its `zero_since`.
    pub queued: bool,
    /// The `cas_blobs` plane holds a row for the blob.
    pub row_present: bool,
    /// The counter record exists and does not decode.
    pub unreadable: bool,
}
