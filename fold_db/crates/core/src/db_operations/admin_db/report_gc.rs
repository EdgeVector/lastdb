//! Report and checkpoint types for orphan atom GC (`gc-atoms`).

use crate::storage::traits::PhysicalScanCursor;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

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
