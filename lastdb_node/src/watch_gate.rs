//! Admission gate for blocking long-poll watchers (`GET /api/local-watch`).
//!
//! # Why this exists
//!
//! A blocked long-poll parks the UDS worker OS thread it was dispatched onto:
//! [`crate::local_outbox::LocalOutbox::poll_after`] ends in
//! `Condvar::wait_timeout`, inside `block_on`, so the thread is genuinely
//! asleep for the client's whole `timeout_ms` (up to
//! [`crate::local_outbox::MAX_POLL_TIMEOUT`]). The handler wall-clock budget
//! cannot preempt it — `tokio::time::timeout` needs the inner future to yield,
//! and a blocking syscall never does.
//!
//! Worse, the watch route takes **no QoS permit** — `acquire_op_permit` is only
//! called on the query/mutation/search paths. So N sleeping watchers read as:
//!
//! ```text
//! QoS: total=0/64 bulk=0/8 sheds_i=0 sheds_b=0   <- sees nothing
//! UDS pool: workers=28 queue_cap=256 in_flight=N <- the real occupancy
//! ```
//!
//! Measured 2026-07-28 against the live primary: 4 concurrent watchers moved
//! `in_flight` by exactly +4 and left `QoS total` at 0. At `workers`
//! concurrent watchers the pool is fully occupied by threads doing nothing,
//! real work queues behind `queue_cap=256`, and `lastdb status` still prints
//! `QoS: total=0/64 sheds_i=0` — **the node reports idle while nothing can be
//! served**, and the documented socket-safe health check (`kanban list`) hangs
//! rather than naming the cause.
//!
//! # What this gate does
//!
//! Bounds concurrent *blocking* watchers strictly below the worker pool so real
//! work always has workers, and makes the occupancy visible on `lastdb status`
//! next to `QoS` and `UDS pool`. Over the cap the node **sheds explicitly**
//! (503) rather than silently consuming a worker — same posture as
//! `uds_worker_queue_full` and the QoS lane sheds.
//!
//! Non-blocking polls (`timeout_ms=0`) are never gated: they hold a worker only
//! for the read itself, and they are the documented fallback a shed client
//! drops to.
//!
//! This is the cheap half of the fix. The complete fix is to park the waiter
//! off the worker and re-dispatch on wake, which needs an async connection
//! path end to end (`uds_http::serve_connection` is sync, thread-per-request).
//! Ship the cap first so the failure mode is bounded and legible; the park is
//! its own change.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Env override for the concurrent-watcher cap.
pub const ENV_MAX_WATCHERS: &str = "LASTDB_LOCAL_WATCH_MAX";

/// Cap used before the UDS pool size is known (and on backends without one).
/// Deliberately below [`lastdb_uds::worker_pool`]'s `MIN_WORKERS` of 8.
pub const DEFAULT_MAX_WATCHERS: usize = 4;

/// Fraction of the worker pool watchers may hold: half, so the other half is
/// always available to serve actual work. At the primary's 28 workers that is
/// 14 — 7x the ~2 concurrent watchers `lastgit forge run --all` produces today,
/// so this never fires in normal operation, only in the saturation it exists to
/// prevent.
const WORKER_SHARE_DIVISOR: usize = 2;

/// Occupancy + cumulative shed counters, for `/api/status` and `lastdb status`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WatchGateSnapshot {
    /// Concurrent blocking watchers allowed.
    pub max: usize,
    /// Blocking watchers parked on a worker right now.
    pub active: usize,
    /// High-water mark of `active` since boot — the number that says whether
    /// the cap was ever approached, which a point-in-time `active` cannot.
    pub peak: usize,
    /// Watchers refused because the cap was full.
    pub sheds: u64,
}

/// Bounds concurrent blocking `local_watch` waiters below the UDS worker pool.
#[derive(Debug)]
pub struct WatchGate {
    max: AtomicUsize,
    /// `LASTDB_LOCAL_WATCH_MAX` was set — [`WatchGate::configure_for_workers`]
    /// must not override an operator's explicit choice.
    env_pinned: bool,
    active: AtomicUsize,
    peak: AtomicUsize,
    sheds: AtomicU64,
}

impl Default for WatchGate {
    fn default() -> Self {
        Self::with_max(DEFAULT_MAX_WATCHERS, false)
    }
}

impl WatchGate {
    fn with_max(max: usize, env_pinned: bool) -> Self {
        Self {
            max: AtomicUsize::new(max.max(1)),
            env_pinned,
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            sheds: AtomicU64::new(0),
        }
    }

    /// Read the cap from [`ENV_MAX_WATCHERS`], else start at
    /// [`DEFAULT_MAX_WATCHERS`] pending [`Self::configure_for_workers`].
    #[must_use]
    pub fn from_env() -> Self {
        match env_flag::var_parsed::<usize>(ENV_MAX_WATCHERS) {
            Some(n) => Self::with_max(n, true),
            None => Self::default(),
        }
    }

    #[must_use]
    pub fn shared_from_env() -> Arc<Self> {
        Arc::new(Self::from_env())
    }

    /// Size the cap from the UDS pool once the accept loops are up. No-op when
    /// the operator pinned it via [`ENV_MAX_WATCHERS`].
    pub fn configure_for_workers(&self, workers: usize) {
        if self.env_pinned {
            return;
        }
        let max = (workers / WORKER_SHARE_DIVISOR).max(1);
        self.max.store(max, Ordering::Relaxed);
    }

    /// Concurrent blocking watchers allowed.
    #[must_use]
    pub fn max(&self) -> usize {
        self.max.load(Ordering::Relaxed)
    }

    /// Take a watcher slot, or `None` when the cap is full (counted as a shed).
    ///
    /// The returned guard releases the slot on drop, so an early return or a
    /// panic in the handler cannot leak occupancy.
    pub fn try_acquire(self: &Arc<Self>) -> Option<WatchSlot> {
        let mut cur = self.active.load(Ordering::Acquire);
        loop {
            if cur >= self.max() {
                self.sheds.fetch_add(1, Ordering::Relaxed);
                return None;
            }
            match self.active.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.peak.fetch_max(cur + 1, Ordering::Relaxed);
                    return Some(WatchSlot {
                        gate: Arc::clone(self),
                    });
                }
                Err(actual) => cur = actual,
            }
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> WatchGateSnapshot {
        WatchGateSnapshot {
            max: self.max(),
            active: self.active.load(Ordering::Acquire),
            peak: self.peak.load(Ordering::Relaxed),
            sheds: self.sheds.load(Ordering::Relaxed),
        }
    }
}

/// RAII watcher slot — releases on drop.
#[derive(Debug)]
pub struct WatchSlot {
    gate: Arc<WatchGate>,
}

impl Drop for WatchSlot {
    fn drop(&mut self) {
        self.gate.active.fetch_sub(1, Ordering::Release);
    }
}
