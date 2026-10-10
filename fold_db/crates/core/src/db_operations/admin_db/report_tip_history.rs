//! Options, checkpoints, and reports for the tip-history drain and the
//! superseded-version retention passes.

use serde::{Deserialize, Serialize};

/// Controls one bounded pass of automatic tip-history chain drain.
///
/// See [`AtomStore::drain_tip_history_chains`]. This is the reclaim path for
/// legacy `tv:` chains left after tip history became opt-in — not a substitute
/// for orphan-atom GC and never a collection compact.
#[derive(Debug, Clone, Default)]
pub struct TipHistoryDrainOptions {
    /// When true, plan and count only — no head rewrite / `tv:` delete.
    pub dry_run: bool,
    /// Max `mk:` tips examined this pass (scan budget).
    pub max_keys: usize,
    /// Optional cap on tips actually pruned this pass. Defaults to `max_keys`.
    pub max_prunes: Option<usize>,
    /// Exclusive resume cursor: full storage key of the last tip walked.
    pub after_key: Option<String>,
    /// Optional storage-prefix scope (share/org planes). `None` = personal.
    pub storage_prefix: Option<String>,
}

/// Durable checkpoint for the background tip-history drain scheduler.
///
/// Stored under [`TIP_HISTORY_DRAIN_CHECKPOINT_KEY`] so interrupted passes
/// resume without redoing the prefix already cleared.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct TipHistoryDrainCheckpoint {
    /// Last full `mk:` key walked (exclusive start for the next pass).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_key: Option<String>,
    /// Cumulative tips whose chains were cleared across completed passes.
    #[serde(default)]
    pub tips_chain_cleared_total: u64,
    /// Cumulative `tv:` nodes removed.
    #[serde(default)]
    pub tip_versions_pruned_total: u64,
    /// How many bounded passes have completed (including zero-yield ones).
    #[serde(default)]
    pub passes_completed: u64,
    /// RFC3339 timestamp of the last pass that wrote this checkpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_pass_at: Option<String>,
    /// True after a pass reports no more remaining work (full sweep done).
    /// The next automatic pass restarts from the beginning of `mk:`.
    #[serde(default)]
    pub sweep_complete: bool,
}

/// Meta key for the automatic tip-history drain checkpoint (personal plane).
pub const TIP_HISTORY_DRAIN_CHECKPOINT_KEY: &str = "tvh-meta:drain:v1";

/// Controls one bounded pass of 7-day superseded-version retention on **live**
/// records. Tombstoned / deleted heads are skipped — the settled delete rule
/// (no timer, no retention window) is out of scope.
///
/// See [`AtomStore::retain_superseded_versions`].
#[derive(Debug, Clone)]
pub struct SupersededVersionRetentionOptions {
    /// When true, plan and count only — no head rewrite / `tv:` delete.
    pub dry_run: bool,
    /// Max `mk:` tips examined this pass (scan budget).
    pub max_keys: usize,
    /// Optional cap on tips actually truncated this pass. Defaults to `max_keys`.
    pub max_prunes: Option<usize>,
    /// Exclusive resume cursor: full storage key of the last tip walked.
    pub after_key: Option<String>,
    /// Optional storage-prefix scope (share/org planes). `None` = personal.
    pub storage_prefix: Option<String>,
    /// Retention window in seconds. Default
    /// [`molecule_key_codec::SUPERSEDED_VERSION_RETENTION_SECS`] (7 days).
    /// `0` uses the default rather than "drop everything".
    pub retention_seconds: Option<u64>,
}

impl Default for SupersededVersionRetentionOptions {
    fn default() -> Self {
        Self {
            dry_run: true,
            max_keys: 256,
            max_prunes: None,
            after_key: None,
            storage_prefix: None,
            retention_seconds: None,
        }
    }
}

/// Durable checkpoint for owner-gated superseded-version retention.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct SupersededVersionRetentionCheckpoint {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_key: Option<String>,
    #[serde(default)]
    pub tips_truncated_total: u64,
    #[serde(default)]
    pub tip_versions_pruned_total: u64,
    #[serde(default)]
    pub passes_completed: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_pass_at: Option<String>,
    /// True after a pass reports no more remaining `mk:` range. A full sweep
    /// that truncated nothing is the settle signal (zero work at steady state).
    #[serde(default)]
    pub sweep_complete: bool,
    #[serde(default)]
    pub settled: bool,
}

/// Meta key for the superseded-version retention checkpoint (personal plane).
pub const SUPERSEDED_VERSION_RETENTION_CHECKPOINT_KEY: &str = "tvh-meta:retain:v1";

/// Report from one bounded 7-day superseded-version retention pass.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SupersededVersionRetentionReport {
    pub dry_run: bool,
    pub retention_seconds: u64,
    /// Nanosecond cutoff: versions with `0 < written_at < cutoff` are expired.
    pub cutoff_written_at_ns: u64,
    pub keys_scanned: u64,
    pub tips_with_chain: u64,
    /// Tombstoned heads left untouched (deleted-record rule).
    pub tips_skipped_tombstoned: u64,
    pub tips_skipped_changed: u64,
    pub tips_skipped_unreadable: u64,
    /// Live heads whose expired tail was (or would be) dropped.
    pub tips_truncated: u64,
    /// `tv:` nodes removed (or that would be removed).
    pub tip_versions_pruned: u64,
    /// `tv:` nodes in truncated chains that stay because they are ≤7d (or undatable).
    pub tip_versions_kept: u64,
    pub tip_version_bytes_approx: u64,
    pub more_remaining: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_after_key: Option<String>,
    /// True when this pass walked the rest of `mk:` and found zero expired work.
    pub settled: bool,
    pub skipped_backup_cut: bool,
    #[serde(default)]
    pub pass_started_at: String,
}

/// Report from one bounded tip-history drain pass.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TipHistoryDrainReport {
    pub dry_run: bool,
    /// `mk:` tips examined this pass.
    pub keys_scanned: u64,
    /// Tips that still carried a non-empty `prev_tip_id` chain.
    pub tips_with_chain: u64,
    /// Tips whose chain was cleared (or would be, on dry-run).
    pub tips_chain_cleared: u64,
    /// `tv:` nodes removed (or that would be removed).
    pub tip_versions_pruned: u64,
    /// Approx bytes of those `tv:` rows (not body atoms).
    pub tip_version_bytes_approx: u64,
    /// Heads abandoned because they moved or vanished mid-pass.
    pub tips_skipped_changed: u64,
    /// `mk:` rows that would not decode as a tip record.
    pub tips_skipped_unreadable: u64,
    /// More `mk:` range remains after this pass's budget.
    pub more_remaining: bool,
    /// Resume cursor for the next pass (full storage key).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_after_key: Option<String>,
    /// When this pass started (RFC3339).
    #[serde(default)]
    pub pass_started_at: String,
}
