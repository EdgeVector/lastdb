//! Option, checkpoint, and report types for the atom GC, tip-history drain,
//! and superseded-version retention passes.

use crate::storage::traits::PhysicalScanCursor;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Report from purging the schema secondary index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaIdxPurgeReport {
    pub keys_deleted: u64,
    pub bytes_freed_approx: u64,
}

/// Report from measuring/purging legacy `ref:` whole-molecule blobs
/// (cold `legacy_blob_refs` collection).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LegacyRefBlobPurgeReport {
    pub dry_run: bool,
    /// LastStore collection that hosts `ref:` keys.
    pub collection: String,
    /// Keys found under the `ref:` prefix, whether or not this call deleted them.
    pub keys_found: u64,
    pub bytes_found_approx: u64,
    /// Keys whose molecule already has per-key coverage (`mh:` / `mk:`) — safe to delete.
    pub keys_safe: u64,
    /// Keys with no per-key coverage — left in place; do not force-delete.
    pub keys_blocked: u64,
    pub bytes_safe_approx: u64,
    pub bytes_blocked_approx: u64,
    /// `0` on a dry run; number of safe keys deleted after a real execute.
    pub keys_deleted: u64,
    /// Sample of blocked keys (capped) for operator follow-up.
    #[serde(default)]
    pub blocked_samples: Vec<String>,
    /// True when no blocked residue remains (and execute finished if not dry-run).
    pub purge_complete: bool,
}

/// Result of ONE bounded pass of `tv:` tip-version chain pruning.
#[derive(Debug, Clone, Default)]
pub(crate) struct TipVersionPrunePlan {
    pub(crate) tips_chain_cleared: u64,
    pub(crate) tip_versions_pruned: u64,
    pub(crate) tip_version_bytes_approx: u64,
    /// `mk:` rows this pass looked at, decided or not eligible.
    pub(crate) keys_scanned: u64,
    /// The pass yielded on its budget with `mk:` range left.
    pub(crate) more_remaining: bool,
    /// Last `mk:` key this pass carried through to a decision, and the
    /// exclusive start for the next one. `None` means the range ran out.
    pub(crate) next_after_key: Option<String>,
    /// Durable physical group/key cursor. A logical `mk:` cursor still resolves
    /// every tips group before it can return one row.
    pub(crate) next_physical_cursor: Option<PhysicalScanCursor>,
    /// Physical tips handles resolved by this prologue pass.
    pub(crate) physical_handles_visited: u64,
    /// Cold shard loads observed while the physical page resolved.
    pub(crate) physical_cold_shard_loads: u64,
    /// Full storage keys of `tv:` rows that will be dropped (dry-run skip set).
    pub(crate) tv_keys_skip: HashSet<String>,
    /// Tips left alone because the `mk:` head changed between the planning scan
    /// and the rewrite — a concurrent writer appended a new version. See
    /// [`AtomGcReport::tips_skipped_changed`].
    pub(crate) tips_skipped_changed: u64,
}

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

/// Durable resume cursor for the `gc-atoms` tip-chain prune prologue.
///
/// Stored under [`GC_ATOMS_PRUNE_CHECKPOINT_KEY`]. Without it every
/// deadline-bounded `gc-atoms --execute` replanned the whole `mk:` plane from
/// zero, so the verb could not converge on a home large enough to need it.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct GcAtomsPruneCheckpoint {
    /// Last full `mk:` key decided (exclusive start for the next pass).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_key: Option<String>,
    /// Physical tips group/key position for the next pass. This supersedes the
    /// logical key cursor because it bounds group resolution as well as rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical_cursor: Option<PhysicalScanCursor>,
    /// Cumulative tips whose chains were cleared across passes.
    #[serde(default)]
    pub tips_chain_cleared_total: u64,
    /// Cumulative `tv:` nodes removed.
    #[serde(default)]
    pub tip_versions_pruned_total: u64,
    /// Cumulative tips whose heads changed before their chain prune landed.
    #[serde(default)]
    pub tips_skipped_changed_total: u64,
    /// One unconfirmed delete-ledger row shared by every pass in this lap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ledger_key: Option<String>,
    /// Tip-version deletes covered by the current unconfirmed ledger row.
    #[serde(default)]
    pub ledger_tip_versions_pruned: u64,
    /// Changed heads covered by the current unconfirmed ledger row.
    #[serde(default)]
    pub ledger_tips_skipped_changed: u64,
    /// How many bounded prologue passes have run (including zero-yield ones).
    #[serde(default)]
    pub passes_completed: u64,
    /// RFC3339 start of the last pass that wrote this checkpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_pass_at: Option<String>,
    /// True once a pass reached the end of `mk:`. The next pass restarts from
    /// the head — chains keep accruing behind the cursor, so a finished sweep
    /// is a lap rather than a terminal state.
    #[serde(default)]
    pub sweep_complete: bool,
}

/// One phase of the durable automatic orphan-byte probe.
///
/// The two clear phases keep probe markers bounded across completed laps. The
/// remaining phases match the manual `gc-atoms` reachability walk, but each
/// call advances at most one physical storage page.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AutomaticGcAtomsProbePhase {
    #[default]
    ClearReferenceMarkers,
    ClearTipVersionSkipMarkers,
    Prologue,
    PinLogReferences,
    Tips,
    TipVersions,
    History,
    Conflicts,
    LegacyRefs,
    Atoms,
    Complete,
}

impl AutomaticGcAtomsProbePhase {
    pub(crate) fn next(self) -> Self {
        match self {
            Self::ClearReferenceMarkers => Self::ClearTipVersionSkipMarkers,
            Self::ClearTipVersionSkipMarkers => Self::Prologue,
            Self::Prologue => Self::PinLogReferences,
            Self::PinLogReferences => Self::Tips,
            Self::Tips => Self::TipVersions,
            Self::TipVersions => Self::History,
            Self::History => Self::Conflicts,
            Self::Conflicts => Self::LegacyRefs,
            Self::LegacyRefs => Self::Atoms,
            Self::Atoms | Self::Complete => Self::Complete,
        }
    }
}

/// Durable state for a bounded automatic orphan-byte probe.
///
/// This row stays constant-size. Reference membership and planned `tv:` skips
/// live in separate keyed rows, never in an unbounded vector in this value.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct AutomaticGcAtomsProbeCheckpoint {
    pub version: u8,
    pub generation: u64,
    pub phase: AutomaticGcAtomsProbePhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<PhysicalScanCursor>,
    #[serde(default)]
    pub started_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
    #[serde(default)]
    pub pages_completed: u64,
    #[serde(default)]
    pub prologue_tips_scanned: u64,
    #[serde(default)]
    pub prologue_tip_versions_planned: u64,
    /// Last durable pin-log key consumed by the external reference page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pin_log_after_key: Option<Vec<u8>>,
    #[serde(default)]
    pub reference_rows_scanned: u64,
    #[serde(default)]
    pub reference_uuids_marked: u64,
    #[serde(default)]
    pub atoms_scanned: u64,
    #[serde(default)]
    pub atoms_referenced: u64,
    #[serde(default)]
    pub unreferenced_atoms: u64,
    #[serde(default)]
    pub unreferenced_bytes_approx: u64,
    #[serde(default)]
    pub atoms_skipped_recent: u64,
    #[serde(default)]
    pub atoms_skipped_undatable: u64,
}

/// One bounded automatic probe call.
#[derive(Debug, Clone, Default)]
pub struct AutomaticGcAtomsProbeOptions {
    pub storage_prefix: Option<String>,
    /// Maximum rows resolved from one physical handle. Defaults to the GC page.
    pub max_rows: Option<usize>,
    /// Start a new generation when the prior probe is complete.
    pub restart_completed: bool,
    /// One strict, bounded page from the durable pin-log reference plane.
    /// Required only while the checkpoint is in `PinLogReferences`.
    pub pin_log_reference_page: Option<AutomaticGcAtomsPinLogReferencePage>,
}

/// One bounded page of atom references from the durable mutation log.
#[derive(Debug, Clone, Default)]
pub struct AutomaticGcAtomsPinLogReferencePage {
    pub atom_uuids: HashSet<String>,
    pub next_after_key: Option<Vec<u8>>,
    pub rows_scanned: u64,
    pub scan_complete: bool,
}

/// Terminal automatic probe result. It exists only after every phase completes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AutomaticGcAtomsProbeResult {
    pub generation: u64,
    pub started_at: String,
    pub completed_at: String,
    pub atoms_scanned: u64,
    pub atoms_referenced: u64,
    pub unreferenced_atoms: u64,
    pub unreferenced_bytes_approx: u64,
    pub atoms_skipped_recent: u64,
    pub atoms_skipped_undatable: u64,
}

/// Progress from one bounded automatic probe call.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AutomaticGcAtomsProbeReport {
    pub generation: u64,
    pub phase: AutomaticGcAtomsProbePhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<PhysicalScanCursor>,
    pub rows_scanned_this_call: u64,
    /// Present only when the full prologue, every reference plane, and the atom
    /// walk completed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<AutomaticGcAtomsProbeResult>,
}

/// One phase of the durable automatic atom-delete pass.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AutomaticGcAtomsDeletePhase {
    #[default]
    Atoms,
    Complete,
}

/// Durable state for the bounded automatic atom-delete pass.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct AutomaticGcAtomsDeleteCheckpoint {
    pub version: u8,
    pub generation: u64,
    pub phase: AutomaticGcAtomsDeletePhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<PhysicalScanCursor>,
    #[serde(default)]
    pub passes_completed: u64,
    #[serde(default)]
    pub rows_scanned: u64,
    #[serde(default)]
    pub candidates_revalidated: u64,
    #[serde(default)]
    pub candidates_cleared_by_revalidation: u64,
    #[serde(default)]
    pub atoms_deleted: u64,
    #[serde(default)]
    pub storage_keys_deleted: u64,
    #[serde(default)]
    pub bytes_freed_approx: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
}

/// Limits for one automatic atom-delete call.
#[derive(Debug, Clone)]
pub struct AutomaticGcAtomsDeleteOptions {
    pub storage_prefix: Option<String>,
    /// Maximum rows resolved from one physical handle. Defaults to the GC page.
    pub max_rows: Option<usize>,
    /// Maximum orphan candidates locked and checked in one call.
    pub candidate_cap: usize,
}

impl Default for AutomaticGcAtomsDeleteOptions {
    fn default() -> Self {
        Self {
            storage_prefix: None,
            max_rows: None,
            candidate_cap: 1_000,
        }
    }
}

/// Terminal automatic atom-delete result.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AutomaticGcAtomsDeleteResult {
    pub generation: u64,
    pub completed_at: String,
    pub passes_completed: u64,
    pub rows_scanned: u64,
    pub candidates_revalidated: u64,
    pub candidates_cleared_by_revalidation: u64,
    pub atoms_deleted: u64,
    pub storage_keys_deleted: u64,
    pub bytes_freed_approx: u64,
}

/// Progress from one bounded automatic atom-delete call.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AutomaticGcAtomsDeleteReport {
    pub generation: u64,
    pub phase: AutomaticGcAtomsDeletePhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<PhysicalScanCursor>,
    pub rows_scanned_this_call: u64,
    pub candidates_revalidated_this_call: u64,
    pub candidates_cleared_by_revalidation_this_call: u64,
    pub atoms_deleted_this_call: u64,
    pub storage_keys_deleted_this_call: u64,
    pub bytes_freed_approx_this_call: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<AutomaticGcAtomsDeleteResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AutomaticGcAtomsProbeMarker {
    pub(crate) generation: u64,
}

/// Meta key for the `gc-atoms` prologue checkpoint (personal plane).
pub const GC_ATOMS_PRUNE_CHECKPOINT_KEY: &str = "gcatoms-meta:prune:v1";

/// Constant-size checkpoint for the automatic orphan-byte probe.
pub const GC_ATOMS_PROBE_CHECKPOINT_KEY: &str = "gcatoms-meta:probe:v1";

/// Constant-size checkpoint for the automatic atom-delete pass.
pub const GC_ATOMS_DELETE_CHECKPOINT_KEY: &str = "gcatoms-meta:delete:v1";

/// Stable, per-UUID reference markers. Values carry the active generation.
pub(crate) const GC_ATOMS_PROBE_REFERENCE_PREFIX: &str = "gcatoms-probe-ref\0";

/// Stable, per-`tv:`-key markers planned by the read-only prologue.
pub(crate) const GC_ATOMS_PROBE_TV_SKIP_PREFIX: &str = "gcatoms-probe-tv-skip:";

/// Override for `mk:` tips per bounded `gc-atoms` prologue pass.
pub const GC_PRUNE_PASS_MAX_KEYS_ENV: &str = "LASTDB_GC_PRUNE_PASS_MAX_KEYS";

/// Override, in seconds, for the wall clock of one prologue pass.
pub const GC_PRUNE_PASS_SECS_ENV: &str = "LASTDB_GC_PRUNE_PASS_SECS";

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

/// Report from orphan atom GC (includes tombstoned tip-version prune).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtomGcReport {
    pub dry_run: bool,
    /// Resolved source schema for a scoped pass. `None` means the whole store.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    pub atoms_scanned: u64,
    pub atoms_referenced: u64,
    pub atoms_deleted: u64,
    /// Approx bytes freed by deleting orphan `atom:` rows (key+value lengths).
    pub bytes_freed_approx: u64,
    /// Tombstoned `mk:` tips whose `prev_tip_id` chain was cleared.
    #[serde(default)]
    pub tips_chain_cleared: u64,
    /// `tv:` tip-version nodes removed (or that would be removed in dry-run).
    #[serde(default)]
    pub tip_versions_pruned: u64,
    /// Approx bytes of those `tv:` rows (not including the body atoms themselves).
    #[serde(default)]
    pub tip_version_bytes_approx: u64,
    /// Tips whose chain was left intact because the `mk:` head changed between
    /// the planning scan and the rewrite — a concurrent writer appended a
    /// version. Pruning on the stale snapshot would have blind-written the old
    /// head back and silently reverted a live record to its previous value.
    /// Non-zero is normal on a busy node and means the guard did its job; these
    /// chains are reconsidered next run.
    #[serde(default)]
    pub tips_skipped_changed: u64,
    /// When the reachability scan began (RFC3339). Every atom row created at or
    /// after this instant is out of scope for this run — see
    /// `atoms_skipped_recent`. Recorded so a later audit can date a GC run
    /// against the store's own evidence instead of a log window.
    #[serde(default)]
    pub scan_started_at: String,
    /// Unreferenced atom rows **protected** because they were created at or
    /// after `scan_started_at`. The reference set is a snapshot taken before
    /// the atom scan, so an atom written into that window has a live tip the
    /// snapshot could not contain. Non-zero here is normal on a busy node and
    /// means the guard did its job; these are reconsidered next run.
    #[serde(default)]
    pub atoms_skipped_recent: u64,
    /// Unreferenced atom rows protected because their `created_at` could not be
    /// read (unparseable row, or the field absent). Deliberately kept rather
    /// than deleted: an undatable row cannot be shown to be outside the race
    /// window. Sustained non-zero is a corruption signal worth chasing, not a
    /// reclaim-tuning knob.
    #[serde(default)]
    pub atoms_skipped_undatable: u64,
    /// Candidates retained because `aref:v2` lacks a complete proof.
    #[serde(default)]
    pub atoms_retained_incomplete_edges: u64,
    /// Candidates retained because the exact target partition has an edge.
    #[serde(default)]
    pub atoms_retained_active_edges: u64,
    /// `mk:` tips the tip-chain prune prologue looked at THIS pass. The
    /// prologue is bounded, so this is a slice of the plane, not the plane.
    #[serde(default)]
    pub tips_scanned: u64,
    /// The prologue yielded on its budget with `mk:` range still ahead of it.
    /// Not a failure: the cursor is durable and the next run continues from it.
    /// Reclaim is still correct in the meantime — an unpruned chain simply
    /// keeps its body atoms referenced, so they are not deleted, only deferred.
    #[serde(default)]
    pub prune_more_remaining: bool,
    /// Where the next prologue pass will resume (full `mk:` key), or `None`
    /// when this pass reached the end of the plane.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prune_next_after_key: Option<String>,
    /// Physical tips handles resolved by the prologue page.
    #[serde(default)]
    pub prune_physical_handles_visited: u64,
    /// Cold tips shard loads observed by the prologue page.
    #[serde(default)]
    pub prune_physical_cold_shard_loads: u64,
}
