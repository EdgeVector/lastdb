//! Restore and scrub report types for snapshots.

use super::*;

/// Local materialization cost for one snapshot restore.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SnapshotRestoreReport {
    pub namespaces: usize,
    pub entries: usize,
    pub batches: usize,
    pub materialize_ms: u64,
    pub final_flush_ms: u64,
}

impl SnapshotRestoreReport {
    pub fn total_ms(&self) -> u64 {
        self.materialize_ms.saturating_add(self.final_flush_ms)
    }
}

/// A row whose at-rest value could not be decrypted during an enumeration.
///
/// The value is deliberately withheld — it is unreadable (a local at-rest
/// "poison" row, typically written by a process that was in a wrong crypto
/// state). Only its location is reported, which is enough to count, log, and
/// scrub it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UndecryptableRow {
    /// Namespace (Sled tree) the row lives in.
    pub namespace: String,
    /// Base64-encoded storage key (keys are plaintext at the at-rest layer).
    pub key_b64: String,
}

/// What a non-aborting snapshot ([`Snapshot::create_reporting`]) had to skip:
/// the undecryptable rows it could not include. Empty on a clean store.
///
/// The backup path reports this loudly so a silently-partial snapshot never
/// passes for complete — the omitted rows carry no recoverable data, but their
/// existence is surfaced, not swallowed.
#[derive(Debug, Clone, Default)]
pub struct SnapshotScrubReport {
    /// Rows skipped because their at-rest value could not be decrypted.
    pub undecryptable: Vec<UndecryptableRow>,
}

impl SnapshotScrubReport {
    /// Number of undecryptable rows skipped from the snapshot.
    pub fn skipped(&self) -> usize {
        self.undecryptable.len()
    }

    /// True when the store held no undecryptable rows (the snapshot is a
    /// complete, byte-for-byte capture of all readable data).
    pub fn is_clean(&self) -> bool {
        self.undecryptable.is_empty()
    }
}

/// The outcome of an explicit scrub pass over the local store
/// (`SyncEngine::scrub_undecryptable_rows`): every undecryptable row found,
/// and — when deletion was requested — how many were removed.
#[derive(Debug, Clone, Default)]
pub struct ScrubReport {
    /// How many snapshot-eligible namespaces were scanned.
    pub scanned_namespaces: usize,
    /// Every undecryptable row found, by namespace + key.
    pub undecryptable: Vec<UndecryptableRow>,
    /// Whether the caller asked for deletion (a read-only report when false).
    pub delete_requested: bool,
    /// How many undecryptable rows were actually deleted (`delete_requested`
    /// only). May be less than `found()` if an individual delete failed.
    pub deleted: usize,
}

impl ScrubReport {
    /// Number of undecryptable rows found across all scanned namespaces.
    pub fn found(&self) -> usize {
        self.undecryptable.len()
    }

    /// True when no undecryptable rows were found.
    pub fn is_clean(&self) -> bool {
        self.undecryptable.is_empty()
    }
}
