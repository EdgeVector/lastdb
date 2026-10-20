//! Phases, checkpoints, and reports for the durable automatic orphan-byte
//! probe and delete passes.

use crate::storage::traits::PhysicalScanCursor;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// One phase of the durable automatic orphan-byte probe.
///
/// The clear phase keeps reference markers bounded across completed laps. The
/// other phases match the current reachability walk, but each
/// call advances at most one physical storage page.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AutomaticGcAtomsProbePhase {
    #[default]
    ClearReferenceMarkers,
    PinLogReferences,
    Tips,
    History,
    Conflicts,
    Atoms,
    Complete,
}

impl AutomaticGcAtomsProbePhase {
    pub(crate) fn next(self) -> Self {
        match self {
            Self::ClearReferenceMarkers => Self::PinLogReferences,
            Self::PinLogReferences => Self::Tips,
            Self::Tips => Self::History,
            Self::History => Self::Conflicts,
            Self::Conflicts => Self::Atoms,
            Self::Atoms | Self::Complete => Self::Complete,
        }
    }
}

/// Durable state for a bounded automatic orphan-byte probe.
///
/// This record stays constant-size. Reference membership uses separate keyed records.
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
    /// Present only after all current reference and atom reads complete.
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
    /// Why those candidates were kept, summed over this pass.
    #[serde(default)]
    pub candidates_retained: GcAtomsRevalidationTally,
    #[serde(default)]
    pub atoms_deleted: u64,
    #[serde(default)]
    pub storage_keys_deleted: u64,
    #[serde(default)]
    pub bytes_freed_approx: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
}

/// Why the automatic delete pass kept orphan candidates instead of deleting
/// them. Each candidate counts once, under the first gate that kept it.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct GcAtomsRevalidationTally {
    /// A retired intent outcome still names the atom.
    #[serde(default)]
    pub retired_intent_root: u64,
    /// A reference marker of this generation appeared after the scan.
    #[serde(default)]
    pub referenced_marker: u64,
    /// The stored body changed between the scan and the recheck.
    #[serde(default)]
    pub body_changed: u64,
    /// The compact reverse-edge plane is not proven complete.
    #[serde(default)]
    pub reads_not_ready: u64,
    /// An active reverse edge still names the atom.
    #[serde(default)]
    pub active_edge: u64,
    /// A pending hold names the atom.
    #[serde(default)]
    pub pending_hold: u64,
    /// The stored body does not decode.
    #[serde(default)]
    pub unreadable: u64,
    /// No zero-count candidate row names the atom, so no zero epoch exists to
    /// age. The grace window cannot be proven for it.
    #[serde(default)]
    pub no_zero_epoch: u64,
    /// The zero epoch is younger than the grace window.
    #[serde(default)]
    pub within_grace: u64,
    /// The current lap has not recorded an unreachable verdict for this zero
    /// epoch (no verdict yet, a verdict from an older lap, or reachable).
    #[serde(default)]
    pub audit_not_clear: u64,
}

impl GcAtomsRevalidationTally {
    pub fn total(&self) -> u64 {
        self.retired_intent_root
            .saturating_add(self.referenced_marker)
            .saturating_add(self.body_changed)
            .saturating_add(self.reads_not_ready)
            .saturating_add(self.active_edge)
            .saturating_add(self.pending_hold)
            .saturating_add(self.unreadable)
            .saturating_add(self.no_zero_epoch)
            .saturating_add(self.within_grace)
            .saturating_add(self.audit_not_clear)
    }

    pub fn add(&mut self, other: &Self) {
        self.retired_intent_root = self
            .retired_intent_root
            .saturating_add(other.retired_intent_root);
        self.referenced_marker = self
            .referenced_marker
            .saturating_add(other.referenced_marker);
        self.body_changed = self.body_changed.saturating_add(other.body_changed);
        self.reads_not_ready = self.reads_not_ready.saturating_add(other.reads_not_ready);
        self.active_edge = self.active_edge.saturating_add(other.active_edge);
        self.pending_hold = self.pending_hold.saturating_add(other.pending_hold);
        self.unreadable = self.unreadable.saturating_add(other.unreadable);
        self.no_zero_epoch = self.no_zero_epoch.saturating_add(other.no_zero_epoch);
        self.within_grace = self.within_grace.saturating_add(other.within_grace);
        self.audit_not_clear = self.audit_not_clear.saturating_add(other.audit_not_clear);
    }
}

/// What a cap-armed delete must also satisfy, per atom, beyond "no lap marker".
///
/// The byte cap only decides when a delete pass starts. Each delete still needs
/// the approved zero-count gates: a candidate row with a zero epoch older than
/// the grace window, and an unreachable audit verdict that THIS lap recorded
/// for that same epoch. Without a gate the pass has the old behavior (no grace,
/// no audit); only an explicit operator flag may ask for that.
#[derive(Debug, Clone)]
pub struct AutomaticGcAtomsGraceGate {
    /// Audit identifier of the lap whose markers the pass trusts.
    pub audit_id: String,
    pub now_unix_nanos: u64,
    pub grace_window: std::time::Duration,
}

/// Hard ceiling on the orphan candidates one automatic delete call locks.
///
/// One call holds a body lock and a count lock for each candidate. Both lock
/// tables have 256 stripes, so 64 candidates hold at most 64 stripes of each
/// (about 22 percent of the table) and 256 hold about 63 percent. A larger
/// batch would make every tip write wait for the whole delete. The store clamps
/// to this value whatever the caller asks for.
pub const AUTOMATIC_GC_ATOMS_DELETE_BATCH_MAX: usize = 256;

/// Limits for one automatic atom-delete call.
///
/// There is no `Default`. The grace gate decides whether the pass may delete
/// without a grace window and an audit, so a caller must say which pass it
/// wants: [`Self::gated`] or [`Self::ungated_emergency`].
#[derive(Debug, Clone)]
pub struct AutomaticGcAtomsDeleteOptions {
    pub storage_prefix: Option<String>,
    /// Maximum rows resolved from one physical handle. `None` is the GC page.
    pub max_rows: Option<usize>,
    /// Maximum orphan candidates locked and checked in one call (clamped to
    /// [`AUTOMATIC_GC_ATOMS_DELETE_BATCH_MAX`]).
    pub candidate_cap: usize,
    /// Private so a literal cannot leave it out. `None` is the emergency pass.
    pub(super) grace_gate: Option<AutomaticGcAtomsGraceGate>,
    /// Accept that the atoms of a lost multi-device write can be deleted. The
    /// store refuses a delete on a node that has applied a peer mutation or
    /// cannot prove it never did, unless this (or the env override) is set.
    pub(super) allow_after_peer_apply: bool,
}

impl AutomaticGcAtomsDeleteOptions {
    /// The pass that enforces the grace window and one lap's audit per atom.
    #[must_use]
    pub fn gated(gate: AutomaticGcAtomsGraceGate, candidate_cap: usize) -> Self {
        Self {
            storage_prefix: None,
            max_rows: None,
            candidate_cap,
            grace_gate: Some(gate),
            allow_after_peer_apply: false,
        }
    }

    /// The emergency pass: no grace window and no audit. For an operator who
    /// set `LASTDB_GC_ATOMS_BYPASS_GRACE` on a full disk, and nothing else.
    #[must_use]
    pub fn ungated_emergency(candidate_cap: usize) -> Self {
        Self {
            storage_prefix: None,
            max_rows: None,
            candidate_cap,
            grace_gate: None,
            allow_after_peer_apply: false,
        }
    }

    /// Carry the operator's acceptance that a lost multi-device write can be
    /// deleted (the peer-apply interlock override).
    #[must_use]
    pub fn with_allow_after_peer_apply(mut self, allow: bool) -> Self {
        self.allow_after_peer_apply = allow;
        self
    }

    /// The scan page and candidate batch this call may use, both clamped.
    pub(super) fn limits(&self, scan_page: usize) -> (usize, usize) {
        (
            self.max_rows.unwrap_or(scan_page).clamp(1, scan_page),
            self.candidate_cap
                .clamp(1, AUTOMATIC_GC_ATOMS_DELETE_BATCH_MAX),
        )
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
    #[serde(default)]
    pub candidates_retained_this_call: GcAtomsRevalidationTally,
    /// Milliseconds the call held its per-atom body and count locks: from the
    /// last lock taken to the delete batch committed.
    #[serde(default)]
    pub locks_held_ms_this_call: u64,
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
