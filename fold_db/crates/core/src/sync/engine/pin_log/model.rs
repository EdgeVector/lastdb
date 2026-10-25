use super::*;

pub const PIN_LOG_NAMESPACE: &str = "sync_pin_log";

pub(super) const PIN_LOG_ENTRY_PREFIX: &str = "target:";

/// SHA-256 length prefixed onto a sealed mutation-log segment's plaintext,
/// matching the `HASH_SIZE` contract in `sync::log`.
pub(super) const MUTATION_LOG_SEGMENT_HASH_SIZE: usize = 32;

/// One durable pin-mode log record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PinLogRecord {
    pub model_version: u32,
    /// Stable id derived from the sync target prefix (`personal` for prefix "").
    pub target_id: String,
    pub target_label: String,
    pub target_prefix: String,
    /// Writer/device that minted the underlying [`LogEntry`].
    pub writer_id: String,
    /// F after applying this record in the target stream.
    pub frontier_after: u64,
    pub timestamp_ms: u64,
    pub entry: LogEntry,
}

/// Operator-visible per-target pin log counters.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PinLogTargetStatus {
    pub target_id: String,
    pub target_label: String,
    pub target_prefix: String,
    pub active: bool,
    pub base_frontier: u64,
    pub last_durable_frontier: u64,
    pub entry_count: u64,
    pub byte_count: u64,
    pub last_durable_at_ms: Option<u64>,
    /// How many sealed base members were frozen for this target at F0.
    pub sealed_base_count: u64,
    /// Seconds since pin entry when known.
    pub pin_age_secs: Option<u64>,
    /// False when pin age cannot be reported honestly.
    pub pin_age_known: bool,
    /// Instrumentation: attempts to rewrite a member of S while pinned.
    pub s_rewrite_attempts: u64,
    /// True after publish until background materialize completes.
    pub materialize_pending: bool,
    /// Degraded when pin is active but age/status is unknown.
    pub degraded: bool,
    /// Published frontier F for continuous mutation-log upload (0 if none).
    /// Scalar max across writers; see [`Self::published_f_by_writer`].
    #[serde(default)]
    pub published_frontier: u64,
    /// Cloud-confirmed published frontier (operator-facing name for F).
    #[serde(default)]
    pub published_through: u64,
    /// Age in seconds of the oldest cloud-confirmed writer recovery point.
    /// `None` until this process observes a cloud-confirmed segment.
    #[serde(default)]
    pub recovery_point_age_secs: Option<u64>,
    /// Per-writer published through_seq (vector F / HWM map).
    /// Empty before any upload; single-writer is a one-entry map.
    #[serde(default)]
    pub published_f_by_writer: BTreeMap<String, u64>,
    /// Durable records not yet sealed/uploaded (`last_durable - published`).
    #[serde(default)]
    pub upload_backlog: u64,
    /// Segments sealed+uploaded this process under the continuous log plane.
    #[serde(default)]
    pub segments_uploaded: u64,
    /// MutationIntent rows skipped because they cannot be sealed.
    #[serde(default)]
    pub records_quarantined: u64,
    /// Last unsealable-record reason (includes the missing atom id).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_quarantine_reason: Option<String>,
}

/// One sealed continuous mutation-log segment ready for (or already on) cloud.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MutationLogSegment {
    pub segment: MutationLogSegmentId,
    /// JSON-encoded [`PinLogRecord`]s covering (prev_F, through_id].
    pub payload: Vec<u8>,
}

pub(super) const PIN_LOG_PUBLISHED_F_PREFIX: &str = "published_f:";
