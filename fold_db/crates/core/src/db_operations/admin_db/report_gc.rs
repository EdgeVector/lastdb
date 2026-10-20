//! Report and checkpoint types for orphan atom GC (`gc-atoms`).

use serde::{Deserialize, Serialize};

/// Constant-size checkpoint for the automatic orphan-byte probe.
pub const GC_ATOMS_PROBE_CHECKPOINT_KEY: &str = "gcatoms-meta:probe:v1";

/// Constant-size checkpoint for the automatic atom-delete pass.
pub const GC_ATOMS_DELETE_CHECKPOINT_KEY: &str = "gcatoms-meta:delete:v1";

/// Stable, per-UUID reference markers. Values carry the active generation.
pub(crate) const GC_ATOMS_PROBE_REFERENCE_PREFIX: &str = "gcatoms-probe-ref\0";

/// Report from current orphan atom GC.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
}
