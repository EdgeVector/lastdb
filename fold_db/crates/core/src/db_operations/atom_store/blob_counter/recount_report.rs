//! The report of the blob step of the offline recount.

use serde::{Deserialize, Serialize};

/// What the blob step found and did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobRecountReport {
    /// The blob step ran.
    pub ran: bool,
    /// The counters were set to the exact count (`--blobs-exact`).
    pub exact: bool,
    /// Blobs that the records and the field history of the listed scopes name.
    pub referenced_blobs: u64,
    /// Records, live or tombstone, and field history entries.
    #[serde(default)]
    pub records_read: u64,
    pub counters_read: u64,
    pub unchanged: u64,
    /// A counter was missing and is made.
    pub created: u64,
    /// A counter held less than the count.
    pub raised: u64,
    /// A counter held more than the count (exact only).
    pub lowered: u64,
    /// A `suspect` flag cleared (exact only).
    pub suspect_cleared: u64,
    /// A counter with a dwell got references, so the dwell ends.
    pub dwell_cancelled: u64,
    /// A blob that nothing references starts its dwell (exact only).
    pub dwell_started: u64,
    pub unreadable_counters: u64,
    /// Blobs that records or a counter hold references to, and whose
    /// `cas_blobs` row is missing. The recount cannot restore the bytes. A late
    /// write that names a blob after its dwell makes one.
    #[serde(default)]
    pub rows_missing: u64,
    /// The first of them, by blob reference.
    #[serde(default)]
    pub missing_row_samples: Vec<String>,
    pub queue_read: u64,
    /// Queue entries that no counter at zero backs, deleted.
    pub queue_deleted: u64,
    /// A counter with a `zero_since` had no queue entry. The entry is put.
    #[serde(default)]
    pub queue_restored: u64,
    /// A bucket held queue entries and had no directory row. The row is put.
    #[serde(default)]
    pub directory_rows_restored: u64,
    pub operations: u64,
    pub batches: u64,
    pub notes: Vec<String>,
}
