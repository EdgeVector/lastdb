//! Local maintenance and retained replay verification. No legacy sync engine exists.
mod pin_log;
pub use pin_log::{
    verify_retired_replay_source, AuthenticatedReplayObject, MutationLogSegment, PinLogRecord,
    RetiredReplaySource,
};
