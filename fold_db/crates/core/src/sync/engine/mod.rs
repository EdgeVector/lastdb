//! Passive legacy models and local maintenance. No legacy sync engine exists.
mod pin_log;
mod status_disk_cache;
mod types;
pub(crate) mod upload_policy;
pub use pin_log::{
    audit_pin_log_plane, verify_retired_replay_source, AuthenticatedReplayObject,
    MutationLogSegment, PinLogPlaneReport, PinLogRecord, PinLogTargetStatus, PinLogWriterStat,
    RetiredReplaySource, PIN_LOG_NAMESPACE, PIN_LOG_OPERATOR_KEYS_PER_CALL,
};
pub(crate) use status_disk_cache::{StatusDiskUsageCache, DEFAULT_TTL as STATUS_DISK_USAGE_TTL};
pub use types::*;
pub use upload_policy::{
    ForegroundPressure, UploadPolicyMode, UploadPolicySnapshot, UploadThrottleSource,
};
