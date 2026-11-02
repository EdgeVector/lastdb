//! Background persist counters for the status object.

/// Background-work counters shared by the native runtime and its callers.
/// Record measurements come directly from the core warm set.
///
/// No write queues background persist work: every verb commits in its
/// native request. The status object keeps the zero snapshot.
#[derive(Debug, Default)]
pub struct PersistLaneMetrics;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PersistLaneMetricsSnapshot {
    pub depth: u64,
    pub failures: u64,
    pub oldest_age_ms: u64,
}

impl PersistLaneMetrics {
    pub(crate) fn new() -> Self {
        Self
    }

    pub fn snapshot(&self) -> PersistLaneMetricsSnapshot {
        PersistLaneMetricsSnapshot::default()
    }
}
