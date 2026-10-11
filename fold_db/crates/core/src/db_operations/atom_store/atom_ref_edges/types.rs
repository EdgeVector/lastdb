//! Public status, report and transition types for the atom reference edge index.

use super::*;

/// Candidate-atom lookup. Absence is authoritative only when `complete` is
/// true and every referenced molecule manifest is also complete.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AtomRefEdgeLookup {
    pub complete: bool,
    pub edges: Vec<AtomRefEdge>,
}

/// Per-molecule cutover gate.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AtomRefMoleculeManifest {
    pub version: u8,
    pub molecule_uuid: String,
    pub mutation_watermark_nanos: u64,
    pub replay_complete: bool,
}

/// Progress for the bounded v1 -> v2 per-molecule history-edge upgrade.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AtomRefHistoryUpgrade {
    pub version: u8,
    pub after_history_key: Option<String>,
    pub rows_walked: u64,
    pub edges_written: u64,
    pub complete: bool,
}

impl Default for AtomRefHistoryUpgrade {
    fn default() -> Self {
        Self {
            version: ATOM_REF_MANIFEST_VERSION_HISTORY,
            after_history_key: None,
            rows_walked: 0,
            edges_written: 0,
            complete: false,
        }
    }
}

/// Durable rebuild phase.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum AtomRefBackfillPhase {
    #[default]
    Backfill,
    Replay,
    Complete,
    Blocked,
}

/// Durable, resumable state for the internal rebuild.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AtomRefBackfillStatus {
    pub version: u8,
    #[serde(default)]
    pub phase: AtomRefBackfillPhase,
    pub after_tip_key: Option<String>,
    pub mutation_watermark_nanos: u64,
    pub current_molecule: Option<String>,
    pub slots_walked: u64,
    pub edges_written: u64,
    pub molecules_complete: u64,
    pub skipped_rows: u64,
    pub completed: bool,
}

impl Default for AtomRefBackfillStatus {
    fn default() -> Self {
        Self {
            version: 1,
            phase: AtomRefBackfillPhase::Backfill,
            after_tip_key: None,
            mutation_watermark_nanos: unix_nanos(),
            current_molecule: None,
            slots_walked: 0,
            edges_written: 0,
            molecules_complete: 0,
            skipped_rows: 0,
            completed: false,
        }
    }
}

impl AtomRefBackfillStatus {
    pub(super) fn compact_v2() -> Self {
        Self {
            version: 2,
            ..Self::default()
        }
    }
}

/// Work performed by one bounded rebuild page.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AtomRefBackfillReport {
    pub slots_walked: u64,
    pub edges_written: u64,
    pub completed: bool,
    pub status: AtomRefBackfillStatus,
}

/// Durable state for the final removal of legacy v1 reverse-edge rows.
///
/// This row lives in the v2 plane. The drain can therefore remove every v1
/// row, including the old v1 rebuild metadata, without removing its cursor.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AtomRefV1DrainStatus {
    pub version: u8,
    pub cursor: Option<PhysicalScanCursor>,
    pub keys_deleted: u64,
    pub completed: bool,
}

impl Default for AtomRefV1DrainStatus {
    fn default() -> Self {
        Self {
            version: 1,
            cursor: None,
            keys_deleted: 0,
            completed: false,
        }
    }
}

/// Work performed by one bounded v1 drain page.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AtomRefV1DrainReport {
    pub keys_deleted: u64,
    pub completed: bool,
    pub status: AtomRefV1DrainStatus,
}

/// Safety audit for one molecule before its compact manifest can advance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AtomRefV2MoleculeAuditReport {
    pub expected_active_edges: u64,
    pub indexed_active_edges: u64,
    pub missing_edges: u64,
    pub invalid_live_keys: u64,
    pub false_zero_reference_atoms: u64,
    pub slots_audited: u64,
    pub history_rows_audited: u64,
}
