use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{debug, info, warn};

/// Tracks pending background tasks (indexing, transforms, backfills, deferred
/// resident-write persists) so a mutation can wait for convergence and so
/// shutdown can drain cleanly.
///
/// # Why waits are watermarked, not global
///
/// This used to be a bare counter whose only wait predicate was "count reached
/// zero". That made every wait a **global barrier**: a mutation's convergence
/// wait covered every *other* writer's background work, and tasks that arrived
/// after the wait began extended it further. Under sustained write load the
/// count never reached zero, so each write paid the full timeout budget —
/// measured on the primary at **2,446 ms mean per mutation, 72% of total write
/// latency**, with per-key ratios up to 11.8x the actual apply work.
///
/// It also made [`crate::resident`] write mode self-defeating: the mode acks a
/// mutation after resident install and defers the durable LastStore put, but
/// the *next* writer then blocked on that deferred put, moving the latency
/// instead of removing it.
///
/// Each task now gets a monotonic sequence number. A waiter samples the
/// sequence counter once at entry and waits only for tasks issued *below* that
/// watermark — its own work (always issued before the wait starts) plus
/// whatever was already in flight. Later arrivals cannot extend an in-progress
/// wait, which is what breaks the convoy.
#[derive(Debug)]
pub struct PendingTaskTracker {
    count: AtomicUsize,
    /// Next sequence number to hand out. Also the watermark a new waiter takes.
    issued: AtomicU64,
    /// Sequence numbers of tasks that have started and not yet finished.
    /// Ordered so the cheapest question — "is anything below the watermark
    /// still running?" — is answered by looking at the first element only.
    inflight: Mutex<BTreeSet<u64>>,
    notify: Arc<Notify>,
}

/// RAII registration for one background task. Created by
/// [`PendingTaskTracker::begin`] before the task is spawned, moved into the
/// spawned future, and completed on drop by any path — including early return
/// and panic.
///
/// Sequence identity is what makes this safe: completion removes one specific
/// sequence number, so a task cannot be counted out twice and the count cannot
/// underflow (the previous manual `decrement()` API needed a CAS loop to guard
/// exactly that).
#[derive(Debug)]
pub struct PendingTask {
    tracker: Arc<PendingTaskTracker>,
    seq: u64,
}

impl Drop for PendingTask {
    fn drop(&mut self) {
        self.tracker.finish(self.seq);
    }
}

impl PendingTaskTracker {
    /// Create a new tracker
    pub fn new() -> Self {
        Self {
            count: AtomicUsize::new(0),
            issued: AtomicU64::new(0),
            inflight: Mutex::new(BTreeSet::new()),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Register one background task and return its RAII guard.
    ///
    /// Call this **before** `tokio::spawn` and move the guard into the future,
    /// so the task is counted even if the spawn is delayed by scheduling.
    pub fn begin(self: &Arc<Self>) -> PendingTask {
        let seq = self.issued.fetch_add(1, Ordering::SeqCst);
        self.lock_inflight().insert(seq);
        let prev = self.count.fetch_add(1, Ordering::SeqCst);
        debug!(
            "Pending tasks incremented: {} -> {} (seq {})",
            prev,
            prev + 1,
            seq
        );
        PendingTask {
            tracker: Arc::clone(self),
            seq,
        }
    }

    /// Get current pending task count
    pub fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    /// Wait for the background work that existed when this call started.
    ///
    /// Returns `true` when that work has drained, `false` on timeout. Tasks
    /// issued *after* entry are deliberately not waited on — see the type-level
    /// note on why the old wait-for-global-zero behaviour was a barrier.
    pub async fn wait_for_completion(&self, timeout_duration: Duration) -> bool {
        // Sampled exactly once. `issued` is the next sequence to be handed
        // out, so everything already issued is strictly below it.
        let watermark = self.issued.load(Ordering::SeqCst);
        if !self.has_inflight_below(watermark) {
            return true;
        }

        info!(
            "Waiting for {} pending background tasks below watermark {} to complete...",
            self.inflight_below_count(watermark),
            watermark
        );

        let start = std::time::Instant::now();

        loop {
            if !self.has_inflight_below(watermark) {
                info!("All background tasks completed in {:?}", start.elapsed());
                return true;
            }

            let elapsed = start.elapsed();
            if elapsed >= timeout_duration {
                warn!(
                    "Timeout waiting for background tasks. Remaining below watermark: {}",
                    self.inflight_below_count(watermark)
                );
                return false;
            }

            // `notify_waiters` stores no permit, so a completion landing
            // between the check above and this await would be missed. The
            // 100ms cap bounds that to one poll interval and also keeps the
            // deadline check live.
            let slice = timeout_duration
                .saturating_sub(elapsed)
                .min(Duration::from_millis(100));
            let _ = tokio::time::timeout(slice, self.notify.notified()).await;
        }
    }

    /// Complete the task holding `seq`.
    fn finish(&self, seq: u64) {
        if self.lock_inflight().remove(&seq) {
            let prev = self.count.fetch_sub(1, Ordering::SeqCst);
            debug!(
                "Pending tasks decremented: {} -> {} (seq {})",
                prev,
                prev - 1,
                seq
            );
        } else {
            // Unreachable via `PendingTask`, which drops once and owns its seq.
            warn!("Pending task seq {seq} completed twice — ignoring");
        }
        // Every completion, not just the drain to zero: a watermarked waiter
        // can become satisfied while unrelated tasks are still in flight.
        self.notify.notify_waiters();
    }

    /// True when some task issued below `watermark` has not finished.
    /// `BTreeSet` is ordered, so the smallest live sequence decides it.
    fn has_inflight_below(&self, watermark: u64) -> bool {
        self.lock_inflight()
            .iter()
            .next()
            .is_some_and(|&seq| seq < watermark)
    }

    fn inflight_below_count(&self, watermark: u64) -> usize {
        self.lock_inflight().range(..watermark).count()
    }

    /// A panicking task holding this lock must not wedge convergence waits for
    /// the life of the process; the set stays usable either way.
    fn lock_inflight(&self) -> std::sync::MutexGuard<'_, BTreeSet<u64>> {
        self.inflight.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Default for PendingTaskTracker {
    fn default() -> Self {
        Self::new()
    }
}
