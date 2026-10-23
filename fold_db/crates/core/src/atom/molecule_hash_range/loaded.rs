//! Neutral lifecycle operations for a partially fetched core molecule.

use std::collections::BTreeMap;

use super::{AtomEntry, MoleculeHashRange};

impl MoleculeHashRange {
    /// Partial memory state is never a complete replacement for disk state.
    pub(crate) fn is_partial(&self) -> bool {
        self.loaded_is_partial || self.order_is_tail
    }

    /// Borrow the ordered slots directly so a bounded filter visits its range.
    pub(crate) fn atom_entries_for_hash(&self, hash: &str) -> Option<&BTreeMap<String, AtomEntry>> {
        self.atom_uuids.get(hash)
    }
}
