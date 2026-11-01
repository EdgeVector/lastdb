//! What remains of the format 6 tip-conflict recorder.
//!
//! A format 7 home keeps a lost write in its record conflict rows
//! (`rcf:`/`rcm:`). No write records a format 6 tip conflict or captures a
//! format 6 field history row. The counter scans still name the counted
//! history rows.

use super::AtomStore;

/// True when `key` is a history row that a recorder wrote as a counted record:
/// `history\0{molecule}:{ts}:conflict:{digest}` from the tip-conflict recorder
/// or `history\0{molecule}:{ts}:suppressed:{digest}` from a Put that lost to a
/// Delete. Each such row holds one reference to every atom it names. A history
/// row of another shape holds none.
pub(crate) fn is_counted_record_key(key: &str) -> bool {
    let Some(rest) = lastdb_model::kind_partition::rest_of(key, "history") else {
        return false;
    };
    let mut parts = rest.rsplitn(3, ':');
    let (_digest, tag) = (parts.next(), parts.next());
    matches!(tag, Some("conflict" | "suppressed" | "archive")) && parts.next().is_some()
}

impl AtomStore {
    /// Sync device id of this node. Empty until a sync engine attaches.
    #[must_use]
    pub(crate) fn local_origin_device(&self) -> String {
        self.local_origin_device
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// The number of record calls that wrote conflict rows so far. Read it
    /// before a scan of the conflict rows, and give it to
    /// `stamp_conflict_index` after the scan.
    pub(crate) fn conflict_record_epoch(&self) -> u64 {
        self.conflict_record_epoch
            .load(std::sync::atomic::Ordering::Acquire)
    }
}
