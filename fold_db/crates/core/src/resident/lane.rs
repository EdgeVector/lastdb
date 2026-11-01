//! Persist-lane pressure counters for the status object.
//!
//! No write uses a persist lane: every verb commits in its native request.
//! `lastdb status` and `lastdb ops` keep this row, so it stays as zeros.

/// Cheap persist-lane pressure for `lastdb status` / `lastdb ops`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PersistLanePressure {
    pub fair_share_bytes: u64,
    pub heaviest_schema: String,
    pub heaviest_bytes: u64,
    pub queued_entries: u64,
    pub reserved_entries: u64,
    pub oldest_age_ms: u64,
    pub refuse_bytes: u64,
    pub refuse_entries: u64,
    pub refuse_unhealthy: u64,
    pub unhealthy_lanes: u64,
    /// Envelopes moved to quarantine since process start.
    pub quarantined: u64,
    /// Quarantine-breaker trips since process start.
    pub breaker_trips: u64,
}
