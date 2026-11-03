//! The options and the result of a recount.

use crate::db_operations::record_store::WalkStats;
use serde::{Deserialize, Serialize};

/// What to recount.
#[derive(Debug, Clone, Deserialize)]
pub struct RecountOptions {
    /// Write the changes. Without it the recount only reports them.
    pub execute: bool,
    /// The storage scope prefixes to recount. `None` is the personal scope.
    pub scopes: Vec<Option<String>>,
    /// The new global replay cutoff of the home, in nanoseconds. A write older
    /// than the cutoff is refused.
    #[serde(default)]
    pub delete_cutoff_nanos: Option<u64>,
}

impl Default for RecountOptions {
    fn default() -> Self {
        Self {
            execute: false,
            scopes: vec![None],
            delete_cutoff_nanos: None,
        }
    }
}

/// What a recount found and did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecountReport {
    /// True when the changes were written.
    pub executed: bool,
    pub scopes: Vec<Option<String>>,
    /// Entries the recount read in the retention queue.
    #[serde(default)]
    pub queue_entries_read: u64,
    /// Queue entries whose bucket the directory does not list
    /// (`queue_entry_without_directory`). With `--execute` the row is written.
    #[serde(default)]
    pub queue_entries_without_directory: u64,
    /// Buckets that got their directory row back.
    #[serde(default)]
    pub retention_buckets_repaired: u64,
    /// Records written or deleted, in batches.
    pub operations: u64,
    pub batches: u64,
    pub elapsed_ms: u64,
    pub range_reads: WalkStats,
    #[serde(default)]
    pub replay_cutoff: Option<crate::db_operations::record_store::ReplayCutoffReport>,
    pub notes: Vec<String>,
}

impl RecountReport {
    /// The number of changes the recount makes (or would make).
    #[must_use]
    pub fn changed(&self) -> u64 {
        self.replay_cutoff.as_ref().map_or(0, |raised| {
            u64::from(raised.cutoff_before_nanos != raised.cutoff_after_nanos)
        }) + self.queue_entries_without_directory
    }
}
