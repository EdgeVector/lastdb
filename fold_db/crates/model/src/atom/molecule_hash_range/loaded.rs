//! Borrowed reads of a loaded core molecule.

use std::collections::BTreeMap;

use super::{AtomEntry, MoleculeHashRange};

impl MoleculeHashRange {
    /// Borrow the ordered slots directly so a bounded filter visits its range.
    pub fn atom_entries_for_hash(&self, hash: &str) -> Option<&BTreeMap<String, AtomEntry>> {
        self.atom_uuids.get(hash)
    }
}
