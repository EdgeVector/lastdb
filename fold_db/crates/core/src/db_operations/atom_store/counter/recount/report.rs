//! The options and the result of a recount.

use crate::db_operations::atom_store::WalkStats;
use serde::{Deserialize, Serialize};

/// What to recount.
#[derive(Debug, Clone, Deserialize)]
pub struct RecountOptions {
    /// Write the changes. Without it the recount only reports them.
    pub execute: bool,
    /// The storage scope prefixes to recount. `None` is the personal scope.
    pub scopes: Vec<Option<String>>,
    /// Passes over the tips and records. Each pass holds the atoms of one
    /// shard of the atom ids in memory, so more passes use less memory.
    pub shards: u32,
    /// The listed scopes are every scope of the home. The blob step may then lower
    /// a blob counter, clear a `suspect` flag and start the dwell of a blob that
    /// nothing references. Without it the blob step only raises counters.
    #[serde(default)]
    pub blobs_exact: bool,
    /// Explicit global replay floor for stopped-home delete-barrier cleanup.
    #[serde(default)]
    pub delete_cutoff_nanos: Option<u64>,
}

impl Default for RecountOptions {
    fn default() -> Self {
        Self {
            execute: false,
            scopes: vec![None],
            shards: 1,
            blobs_exact: false,
            delete_cutoff_nanos: None,
        }
    }
}

/// One change, for the sample list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Change {
    pub scope: Option<String>,
    pub atom: String,
    /// `create`, `raise`, `lower`, `clear_suspect`, `rewrite`,
    /// `delete_counter` or `delete_atom`.
    pub action: String,
    /// The count before, when a counter was there and readable.
    pub before: Option<u64>,
    /// The count after. `None` when the counter is gone.
    pub after: Option<u64>,
}

/// What a recount found and did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecountReport {
    /// True when the changes were written.
    pub executed: bool,
    pub scopes: Vec<Option<String>>,
    pub shards: u32,
    pub tips_read: u64,
    pub records_read: u64,
    /// Tip or conflict rows that do not decode. Nothing is written while any exists.
    pub unreadable_rows: u64,
    /// Atoms that tips or records name.
    pub referenced_atoms: u64,
    pub counters_read: u64,
    pub bodies_read: u64,
    /// Counters that already held the exact count.
    pub unchanged: u64,
    /// A counter was missing and is written.
    pub created: u64,
    /// A counter held less than the holders.
    pub raised: u64,
    /// A counter held more than the holders.
    pub lowered: u64,
    /// A `suspect` counter held the exact count and is cleared.
    pub suspect_cleared: u64,
    /// A counter did not decode and is written again.
    pub rewritten: u64,
    /// A counter without a body and without holders is deleted.
    pub counters_deleted: u64,
    /// A body without holders is deleted with its counter.
    pub bodies_deleted: u64,
    /// Atoms that tips or records name and that have no body. Nothing can repair them.
    pub lost_bodies: u64,
    /// Entries the recount read in the retention queue (records and drain markers).
    #[serde(default)]
    pub queue_entries_read: u64,
    /// Records that no queue entry names (`record_without_queue_entry`). With
    /// `--execute` each gets a queue entry that expires one window from now, and
    /// the directory row of its bucket.
    #[serde(default)]
    pub records_without_queue_entry: u64,
    /// Queue entries whose bucket the directory does not list
    /// (`queue_entry_without_directory`). With `--execute` the row is written.
    #[serde(default)]
    pub queue_entries_without_directory: u64,
    /// Records that got their rows back.
    #[serde(default)]
    pub retention_records_repaired: u64,
    /// Buckets that got their directory row back.
    #[serde(default)]
    pub retention_buckets_repaired: u64,
    /// Records written or deleted, in batches.
    pub operations: u64,
    pub batches: u64,
    pub elapsed_ms: u64,
    pub range_reads: WalkStats,
    /// The blob step: the blob counters against the atom counters and the tips.
    #[serde(default)]
    pub blobs: crate::db_operations::atom_store::BlobRecountReport,
    #[serde(default)]
    pub delete_prune: Option<crate::db_operations::atom_store::DeletePruneReport>,
    pub changes: Vec<Change>,
    pub notes: Vec<String>,
}

impl RecountReport {
    /// The number of changes the recount makes (or would make): counters and
    /// bodies of atoms, and retention rows of records.
    #[must_use]
    pub fn changed(&self) -> u64 {
        self.delete_prune.as_ref().map_or(0, |prune| {
            prune.pruned + u64::from(prune.cutoff_before_nanos != prune.cutoff_after_nanos)
        }) + self.created
            + self.raised
            + self.lowered
            + self.suspect_cleared
            + self.rewritten
            + self.counters_deleted
            + self.bodies_deleted
            + self.blobs.created
            + self.blobs.raised
            + self.blobs.lowered
            + self.blobs.suspect_cleared
            + self.blobs.dwell_cancelled
            + self.blobs.dwell_started
            + self.blobs.queue_deleted
            + self.blobs.queue_restored
            + self.blobs.directory_rows_restored
            + self.records_without_queue_entry
            + self.queue_entries_without_directory
    }
}
