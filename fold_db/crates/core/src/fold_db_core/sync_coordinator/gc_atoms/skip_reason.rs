//! Stable reasons a step deleted nothing, and why a completed lap restarts.
//!
//! A dashboard keys on these strings.

pub const DISABLED: &str = "disabled";
pub const NOT_DUE: &str = "not-due";
pub const PROBE_RUNNING: &str = "probe-running";
pub const BELOW_THRESHOLD: &str = "below-threshold";
pub const CUT_HELD: &str = "cut-held";
pub const PACKING_LOCK_BUSY: &str = "packing-lock-busy";
pub const READS_NOT_READY: &str = "atom-ref-reads-not-ready";
pub const NO_CANDIDATES: &str = "no-eligible-candidates";
pub const START_DELAY: &str = "start-delay";
pub const AUDIT_INCOMPLETE: &str = "audit-incomplete";
pub const DELETE_PASS_RUNNING: &str = "delete-pass-running";
pub const FAILED: &str = "failed";
/// Restart reasons for a completed lap that is too old to delete on.
pub const LAP_BEFORE_BOOT: &str = "lap-finished-before-boot";
pub const LAP_OLDER_THAN_RELAP: &str = "lap-older-than-relap-interval";
/// Never waived, even while a pass makes progress (the grace window bounds it).
pub const LAP_OLDER_THAN_MAX_AGE: &str = "lap-older-than-max-age";
pub const LAP_FROM_FUTURE: &str = "lap-completed-in-the-future";
/// Automatic delete is off: this node applied a peer mutation.
pub const PEER_APPLIED: &str = "peer-applied";
/// Automatic delete is off: this node cannot prove it never applied one.
pub const PEER_STATE_UNKNOWN: &str = "peer-state-unknown";
