//! The sync device id and the conflict record epoch of the atom store.
//!
//! A format 7 home keeps a lost write in its record conflict rows
//! (`rcf:`/`rcm:`). No write records a format 6 tip conflict or captures a
//! format 6 field history row.

use super::AtomStore;

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
