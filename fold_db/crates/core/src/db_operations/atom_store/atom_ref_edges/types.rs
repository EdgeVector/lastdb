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
