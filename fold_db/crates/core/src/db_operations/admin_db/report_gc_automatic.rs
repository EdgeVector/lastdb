//! Phases, checkpoints, and reports for the durable automatic orphan-byte
//! probe and delete passes.

use crate::storage::traits::PhysicalScanCursor;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

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
