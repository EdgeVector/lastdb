//! What the blob planner and the dwell pass report: totals, the last queue
//! reading, and the blobs whose counter disagreed.
//!
//! The planner never hides a counter fault. It keeps the blob, marks the counter
//! `suspect`, and records the event here. `lastdb status` shows these totals.

use super::plan::BlobEvent;
use serde::Serialize;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// How many recent fault events the report keeps.
const RECENT_FAULTS: usize = 32;

/// Totals since the node started, the last queue reading, and the recent faults.
#[derive(Debug, Default)]
pub(crate) struct BlobEvents {
    created: AtomicU64,
    registered: AtomicU64,
    zero_reached: AtomicU64,
    cancelled: AtomicU64,
    underflows: AtomicU64,
    dangling_removals: AtomicU64,
    blobs_deleted: AtomicU64,
    stale_entries: AtomicU64,
    passes: AtomicU64,
    queue_depth: AtomicU64,
    queue_depth_capped: AtomicBool,
    next_due_unix_ms: AtomicU64,
    last_pass_unix_ms: AtomicU64,
    running: AtomicBool,
    recent_faults: Mutex<VecDeque<String>>,
}

/// Keeps the single dwell pass of a store. Dropping it ends the pass.
pub struct DwellGuard {
    events: Arc<BlobEvents>,
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
        };
        total.fetch_add(1, Ordering::Relaxed);
        let name = match event {
            BlobEvent::Underflow => "underflow",
            BlobEvent::Dangling => "dangling_removal",
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

    /// What one dwell pass saw in the queue.
    pub(super) fn note_pass(&self, depth: u64, capped: bool, next_due: Option<u64>, now: u64) {
        self.passes.fetch_add(1, Ordering::Relaxed);
        self.queue_depth.store(depth, Ordering::Relaxed);
        self.queue_depth_capped.store(capped, Ordering::Relaxed);
        self.next_due_unix_ms
            .store(next_due.unwrap_or(0), Ordering::Relaxed);
        self.last_pass_unix_ms.store(now, Ordering::Relaxed);
    }

    /// Start the one pass of this store, or `None` when a pass is running.
    pub(super) fn begin_pass(self: &Arc<Self>) -> Option<DwellGuard> {
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
            dwell_passes: self.passes.load(Ordering::Relaxed),
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            queue_depth_capped: self.queue_depth_capped.load(Ordering::Relaxed),
            next_due_unix_ms: nonzero(&self.next_due_unix_ms),
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
    pub dwell_passes: u64,
    /// Entries in the first page of the queue at the last pass.
    pub queue_depth: u64,
    /// The queue holds at least `queue_depth` entries: the page was full.
    pub queue_depth_capped: bool,
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
}
