//! Report types returned by the schema attribution walks.

use serde::{Deserialize, Serialize};

/// The catalog-root part of an attribution epoch.
///
/// This is deliberately only the schema-to-molecule layer. The next walk
/// reads each molecule's bounded tip pages and classifies atoms, history,
/// blobs, proteins, and remaining physical residue. A root row by itself does
/// not authorize any deletion.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaRootAttributionReport {
    pub schemas_read: u64,
    pub molecule_roots: u64,
    pub root_paths_written: u64,
    /// False means the graph paths are still valid but logical size is not
    /// yet a complete report. The walker never turns this into residue.
    pub size_complete: bool,
    pub missing_molecule_counters: u64,
    pub pending_protein_folds: u64,
    pub logical_value_bytes: u64,
    pub structure_bytes: u64,
    pub retained_history_bytes: u64,
}

/// One bounded continuation page from a schema molecule into live tips and
/// their atom targets. Missing atom bodies remain explicit unknowns.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaRootTipAttributionReport {
    pub molecule_uuid: String,
    pub tips_seen: u64,
    pub atoms_attributed: u64,
    pub unknown_atoms: u64,
    pub missing_tips: u64,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

/// Result of attributing the node-local retention registry's own bookkeeping
/// rows (policy + hash-partition markers) as `Retention`-rooted objects.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaRetentionAttributionReport {
    pub schemas_with_policy: u64,
    pub root_paths_written: u64,
}
