//! Exact copies of an acknowledged slot, including its optional metadata.

use super::{AtomEntry, KeyMetadata, MoleculeHashRange};
use chrono::Utc;

impl MoleculeHashRange {
    /// Force-install a resident tip's LWW identity without last-writer-wins.
    pub(crate) fn overlay_resident_atom_entry(
        &mut self,
        hash: String,
        range: String,
        entry: AtomEntry,
    ) {
        if self.get_atom_entry(&hash, &range) == Some(&entry) {
            return;
        }
        self.version += 1;
        self.remove_key_metadata(&hash, &range);
        self.atom_uuids
            .entry(hash)
            .or_default()
            .insert(range, entry);
        self.updated_at = Utc::now();
    }

    pub(crate) fn overlay_resident_tip(
        &mut self,
        hash: String,
        range: String,
        entry: AtomEntry,
        metadata: Option<KeyMetadata>,
    ) {
        self.overlay_resident_atom_entry(hash.clone(), range.clone(), entry);
        self.remove_key_metadata(&hash, &range);
        if let Some(metadata) = metadata {
            self.set_key_metadata(hash, range, metadata);
        }
    }
}
