//! Shared counters for background persist lanes, independent of core values.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

/// Background-work counters shared by the native runtime and its callers.
/// Logical-record measurements come directly from the core warm set.
#[derive(Debug, Default)]
pub struct PersistLaneMetrics {
    depth: AtomicU64,
    failures: AtomicU64,
    oldest_enqueued_at: Mutex<Option<Instant>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PersistLaneMetricsSnapshot {
    pub depth: u64,
    pub failures: u64,
    pub oldest_age_ms: u64,
}

impl PersistLaneMetrics {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn record_enqueue(&self) {
        self.depth.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_dequeue(&self, count: u64) {
        self.depth.fetch_sub(count, Ordering::Relaxed);
    }

    pub(crate) fn set_oldest_enqueued_at(&self, enqueued_at: Option<Instant>) {
        *self.oldest_enqueued_at.lock().expect("persist lane age") = enqueued_at;
    }

    pub(crate) fn record_failure(&self) {
        self.failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> PersistLaneMetricsSnapshot {
        let oldest_age_ms = self
            .oldest_enqueued_at
            .lock()
            .expect("persist lane age")
            .map_or(0, |enqueued_at| {
                u64::try_from(enqueued_at.elapsed().as_millis()).unwrap_or(u64::MAX)
            });
        PersistLaneMetricsSnapshot {
            depth: self.depth.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            oldest_age_ms,
        }
    }
}
