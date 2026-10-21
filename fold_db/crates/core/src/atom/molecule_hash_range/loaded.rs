//! Neutral lifecycle operations for a partially fetched core molecule.

use std::collections::BTreeMap;

use super::{AtomEntry, KeyMetadata, MoleculeHashRange};

impl MoleculeHashRange {
    /// Create a logical molecule whose absent slots require a disk fetch.
    pub(crate) fn with_partial_uuid(uuid: String) -> Self {
        let mut molecule = Self::with_uuid(uuid);
        molecule.loaded_is_partial = true;
        molecule
    }

    /// Partial memory state is never a complete replacement for disk state.
    pub(crate) fn is_partial(&self) -> bool {
        self.loaded_is_partial || self.order_is_tail
    }

    /// Restore a fetched slot without a logical write, clock change, or history.
    pub(crate) fn insert_loaded_slot_verbatim(
        &mut self,
        hash: String,
        range: String,
        entry: AtomEntry,
        metadata: Option<KeyMetadata>,
    ) -> Option<AtomEntry> {
        self.loaded_is_partial = true;
        self.remove_key_metadata(&hash, &range);
        if let Some(metadata) = metadata {
            self.set_key_metadata(hash.clone(), range.clone(), metadata);
        }
        self.atom_uuids
            .entry(hash)
            .or_default()
            .insert(range, entry)
    }

    /// Borrow the ordered slots directly so a bounded filter visits its range.
    pub(crate) fn atom_entries_for_hash(&self, hash: &str) -> Option<&BTreeMap<String, AtomEntry>> {
        self.atom_uuids.get(hash)
    }
}
