//! The sync device id of the atom store.
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
}
