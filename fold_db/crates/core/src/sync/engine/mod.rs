//! Local maintenance and retained replay verification. No legacy sync engine exists.
mod compaction_status;
mod pin_log;
mod status_disk_cache;
pub use compaction_status::AutomaticCompactionStatus;
pub use pin_log::{
    audit_pin_log_plane, verify_retired_replay_source, AuthenticatedReplayObject,
    MutationLogSegment, PinLogPlaneReport, PinLogRecord, PinLogTargetStatus, PinLogWriterStat,
    RetiredReplaySource, PIN_LOG_NAMESPACE, PIN_LOG_OPERATOR_KEYS_PER_CALL,
};
pub(crate) use status_disk_cache::{StatusDiskUsageCache, DEFAULT_TTL as STATUS_DISK_USAGE_TTL};
