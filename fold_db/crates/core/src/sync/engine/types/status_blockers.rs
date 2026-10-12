use crate::sync::snapshot::SnapshotScrubReport;
use serde::{Deserialize, Serialize};

/// Actionable description of a cloud replay blocker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncReplayBlocker {
    /// Stable machine-readable code for consumers.
    pub code: String,
    /// Sync target label whose replay is blocked.
    pub target: String,
    /// Cloud log sequence that could not be safely replayed.
    pub seq: u64,
    /// Redacted failure reason.
    pub reason: String,
    /// Human-actionable next step.
    pub action: String,
}

/// Actionable description of a cloud **backup** blocker — the upload side of
/// [`SyncReplayBlocker`].
///
/// Raised when the pre-upload decryptability proof is unsatisfiable in a way
/// retrying cannot change, so every further cycle is doomed. Carrying it as
/// structured status (rather than only as the cycle's last error string) is
/// what lets `lastdb status` say "backup cannot start, an operator must act"
/// instead of a generic `sync_failing` that reads as transient.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncBackupBlocker {
    /// Stable machine-readable code for consumers.
    pub code: String,
    /// Sync target label whose backup is blocked.
    pub target: String,
    /// Cloud log head sequence that could not be used as a proof, when one was
    /// found. `None` when the prefix listed no log object at all.
    pub head_seq: Option<u64>,
    /// Redacted failure reason.
    pub reason: String,
    /// Human-actionable next step.
    pub action: String,
    /// Unix timestamp (seconds) when the block was first latched.
    pub blocked_since: u64,
}

/// Outcome of explicitly quarantining one blocked cloud log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayBlockerQuarantineOutcome {
    /// Sync target label whose blocked entry was cleared.
    pub target: String,
    /// Cloud log sequence that was cleared for this device.
    pub seq: u64,
    /// Number of cloud log objects deleted (`1` for corrupt-entry delete path,
    /// `0` for apply-failed local skip).
    pub deleted_log_objects: usize,
    /// `"delete"` destroys the cloud object; `"skip_local"` only advances this
    /// device and leaves the object for peers / later builds.
    pub mode: String,
}

/// User-visible completeness summary for the last successful explicit snapshot
/// backup. Clean by default so status stays honest before any poison row has
/// been observed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SnapshotCompletionStatus {
    pub undecryptable_unsynced_count: usize,
    pub last_snapshot_complete: bool,
    pub undecryptable_namespaces: Vec<String>,
}

impl Default for SnapshotCompletionStatus {
    fn default() -> Self {
        Self {
            undecryptable_unsynced_count: 0,
            last_snapshot_complete: true,
            undecryptable_namespaces: Vec::new(),
        }
    }
}

impl From<&SnapshotScrubReport> for SnapshotCompletionStatus {
    fn from(report: &SnapshotScrubReport) -> Self {
        let mut namespaces: Vec<String> = report
            .undecryptable
            .iter()
            .map(|row| row.namespace.clone())
            .collect();
        namespaces.sort();
        namespaces.dedup();

        Self {
            undecryptable_unsynced_count: report.skipped(),
            last_snapshot_complete: report.is_clean(),
            undecryptable_namespaces: namespaces,
        }
    }
}
