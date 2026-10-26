//! Local capture compaction status.
use serde::{Deserialize, Serialize};

/// Last successful automatic physical compaction for one captured plane.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct AutomaticCompactionStatus {
    /// Unix seconds when the last automatic compaction completed.
    pub last_compacted_at_unix_s: Option<u64>,
    /// Trigger that armed the rewrite (`overhang` after D2; `max_bytes` is retired).
    pub last_trigger: Option<String>,
    /// Status-only absolute budget alarm. Does not gate compaction.
    pub configured_max_bytes: u64,
    /// Bytes before the last successful rewrite.
    pub last_bytes_before: Option<u64>,
    /// Bytes after the last successful rewrite.
    pub last_bytes_after: Option<u64>,
    /// Last measured allocated bytes for this plane.
    #[serde(default)]
    pub allocated_bytes: Option<u64>,
    /// Last measured apparent (live) bytes for this plane.
    #[serde(default)]
    pub apparent_bytes: Option<u64>,
    /// allocated − apparent. The reclaim a compact would return.
    #[serde(default)]
    pub overhang_bytes: Option<u64>,
    /// Measured overhang in basis points of allocation.
    #[serde(default)]
    pub overhang_bps: Option<u64>,
    /// Configured ratio trigger in basis points.
    #[serde(default)]
    pub trigger_overhang_bps: u64,
    /// Configured absolute overhang floor in bytes.
    #[serde(default)]
    pub trigger_overhang_bytes: u64,
    /// True when the current measurement is above both ratio and floor.
    #[serde(default)]
    pub above_trigger: bool,
    /// True when allocated bytes exceed the status-only budget alarm.
    #[serde(default)]
    pub cap_alarm: bool,
    /// Record bytes live ids still address (store residue counters), over
    /// the groups the probe could measure.
    #[serde(default)]
    pub live_bytes: Option<u64>,
    /// Dead record bytes: superseded puts, deleted puts, delete markers.
    /// This is what a delete leaves behind; only a rewrite returns it.
    #[serde(default)]
    pub dead_bytes: Option<u64>,
    /// `dead_bytes` as basis points of measured record bytes.
    #[serde(default)]
    pub dead_bps: Option<u64>,
    /// On-disk bytes whose residue the probe could not classify.
    #[serde(default)]
    pub residue_unknown_bytes: Option<u64>,
    /// Bytes a rewrite is expected to return: filesystem overhang plus the
    /// dead-record share of the measured plane. The trigger compares this,
    /// not `overhang_bytes` alone, against the floor and ratio.
    #[serde(default)]
    pub reclaimable_estimate_bytes: Option<u64>,
    /// `reclaimable_estimate_bytes` as basis points of allocation.
    #[serde(default)]
    pub reclaimable_bps: Option<u64>,
}
