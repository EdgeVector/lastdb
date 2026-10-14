//! Option and report types for one bounded atom reap call.

use super::*;

/// Knobs for one bounded reap call.
#[derive(Debug, Clone)]
pub struct AtomGcReapOptions {
    pub now_unix_nanos: u64,
    pub grace_window: Duration,
    pub max_candidates: usize,
    pub dry_run: bool,
    /// Rewrite the atom plane after deletes so dead segment bytes return.
    pub compact_after_delete: bool,
    /// When set, only a verdict recorded by this audit clears a delete. A
    /// verdict from an older lap is treated as no verdict.
    pub required_audit_id: Option<String>,
    /// Stop between candidates once this instant has passed.
    pub deadline: Option<tokio::time::Instant>,
    /// Accept that the atoms of a lost multi-device write can be deleted. A
    /// non-dry-run reap refuses without it on a node that has applied a peer
    /// mutation or cannot prove it never did (the daemon-wide env override
    /// also lifts the refusal).
    pub allow_after_peer_apply: bool,
}

impl Default for AtomGcReapOptions {
    fn default() -> Self {
        Self {
            now_unix_nanos: unix_nanos(),
            grace_window: DEFAULT_ATOM_GC_GRACE_WINDOW,
            max_candidates: 256,
            dry_run: true,
            compact_after_delete: false,
            required_audit_id: None,
            deadline: None,
            allow_after_peer_apply: false,
        }
    }
}

/// Result of one bounded candidate reap.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct AtomGcReapReport {
    pub dry_run: bool,
    pub candidates_examined: u64,
    /// Permanent references from an encrypted retired-intent outcome.
    #[serde(default)]
    pub retained_retired_intent_reference: u64,
    pub eligible: u64,
    pub atoms_deleted: u64,
    pub storage_keys_deleted: u64,
    pub bytes_deleted_approx: u64,
    pub retained_before_grace: u64,
    pub retained_without_audit: u64,
    /// A verdict exists but an older lap recorded it.
    #[serde(default)]
    pub retained_stale_audit: u64,
    pub retained_audit_reachable: u64,
    pub retained_pending_reference: u64,
    pub cancelled_positive_count: u64,
    /// Candidates removed because their atom has no count record. The body leaks.
    #[serde(default)]
    pub cancelled_missing_count: u64,
    /// Candidates kept because their revalidation returned an error, or because
    /// their queue record did not decode.
    #[serde(default)]
    pub retained_error: u64,
    pub stale_queue_rows_deleted: u64,
    pub truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction: Option<CollectionCompactReport>,
}
