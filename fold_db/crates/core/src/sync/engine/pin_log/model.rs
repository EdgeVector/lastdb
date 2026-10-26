use super::*;

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

/// One sealed continuous mutation-log segment ready for (or already on) cloud.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MutationLogSegment {
    pub segment: MutationLogSegmentId,
    /// JSON-encoded [`PinLogRecord`]s covering (prev_F, through_id].
    pub payload: Vec<u8>,
}
