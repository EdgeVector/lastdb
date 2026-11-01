//! Report types returned by the schema attribution walks.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaRootAttributionReport {
    pub schemas_read: u64,
    pub field_key_roots: u64,
    pub root_paths_written: u64,
    /// False means the graph paths are still valid but logical size is not
    /// yet a complete report. The walker never turns this into residue.
    pub size_complete: bool,
    pub missing_field_key_counters: u64,
    pub logical_value_bytes: u64,
    pub structure_bytes: u64,
    pub retained_history_bytes: u64,
}

/// Result of attributing the node-local retention registry's own bookkeeping
/// rows (policy + hash-partition markers) as `Retention`-rooted objects.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaRetentionAttributionReport {
    pub schemas_with_policy: u64,
    pub root_paths_written: u64,
}
