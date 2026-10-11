//! Process-local lane counters and the status rows built from them.

use std::sync::atomic::{AtomicU64, Ordering};

/// Process-local lane counters (also mirrored onto [`super::ResidentMetrics`]).
#[derive(Debug, Default)]
pub struct PersistLaneCounters {
    pub(super) depth: AtomicU64,
    pub(super) failures: AtomicU64,
    pub(super) unhealthy_lanes: AtomicU64,
    /// Reserved status field for a future aggregate slot-lag gauge.
    pub(super) resident_minus_durable_revision: AtomicU64,
    pub(super) refuse_bytes: AtomicU64,
    pub(super) refuse_entries: AtomicU64,
    pub(super) refuse_unhealthy: AtomicU64,
    pub(super) write_throughs: AtomicU64,
    /// Envelopes moved to quarantine (process lifetime).
    pub(super) quarantined: AtomicU64,
    /// Quarantine-breaker trips (process lifetime). Each trip closed one
    /// lane's admission until its half-open probe.
    pub(super) breaker_trips: AtomicU64,
}

/// Occupancy of one persist lane for status / ops.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistLaneOccupancy {
    pub storage_prefix: String,
    pub schema_name: String,
    /// Bytes in the queue or in an active writer call.
    pub queued_bytes: u64,
    pub reserved_bytes: u64,
    pub queued_entries: u64,
    pub reserved_entries: u64,
    pub unhealthy: bool,
}

impl PersistLaneOccupancy {
    #[must_use]
    pub fn occupied_bytes(&self) -> u64 {
        self.queued_bytes.saturating_add(self.reserved_bytes)
    }
}

/// Cheap persist-lane pressure for `lastdb status` / `lastdb ops`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PersistLanePressure {
    pub fair_share_bytes: u64,
    pub write_through_threshold_bytes: u64,
    pub heaviest_schema: String,
    pub heaviest_bytes: u64,
    pub queued_entries: u64,
    pub reserved_entries: u64,
    pub oldest_age_ms: u64,
    pub refuse_bytes: u64,
    pub refuse_entries: u64,
    pub refuse_unhealthy: u64,
    pub write_throughs: u64,
    pub unhealthy_lanes: u64,
    /// Envelopes moved to quarantine since process start.
    pub quarantined: u64,
    /// Quarantine-breaker trips since process start.
    pub breaker_trips: u64,
}

impl PersistLaneCounters {
    pub fn depth(&self) -> u64 {
        self.depth.load(Ordering::Relaxed)
    }

    pub fn failures(&self) -> u64 {
        self.failures.load(Ordering::Relaxed)
    }

    pub fn unhealthy_lanes(&self) -> u64 {
        self.unhealthy_lanes.load(Ordering::Relaxed)
    }

    pub fn resident_minus_durable_revision(&self) -> u64 {
        self.resident_minus_durable_revision.load(Ordering::Relaxed)
    }

    pub fn refuse_bytes(&self) -> u64 {
        self.refuse_bytes.load(Ordering::Relaxed)
    }

    pub fn refuse_entries(&self) -> u64 {
        self.refuse_entries.load(Ordering::Relaxed)
    }

    pub fn refuse_unhealthy(&self) -> u64 {
        self.refuse_unhealthy.load(Ordering::Relaxed)
    }

    pub fn write_throughs(&self) -> u64 {
        self.write_throughs.load(Ordering::Relaxed)
    }

    pub fn quarantined(&self) -> u64 {
        self.quarantined.load(Ordering::Relaxed)
    }

    pub fn breaker_trips(&self) -> u64 {
        self.breaker_trips.load(Ordering::Relaxed)
    }

    pub fn record_failure(&self) {
        self.failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_refuse_bytes(&self) {
        self.refuse_bytes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_refuse_entries(&self) {
        self.refuse_entries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_refuse_unhealthy(&self) {
        self.refuse_unhealthy.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_write_through(&self) {
        self.write_throughs.fetch_add(1, Ordering::Relaxed);
    }
}
