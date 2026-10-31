//! Exact copies of an acknowledged slot, including its optional metadata.

use super::{AtomEntry, KeyMetadata, MoleculeHashRange};
use chrono::{DateTime, Utc};

impl MoleculeHashRange {
    /// Force-install a resident tip's LWW identity without last-writer-wins.
    /// The caller reads the clock for `updated_at`.
    pub fn overlay_resident_atom_entry_at(
        &mut self,
        hash: String,
        range: String,
        entry: AtomEntry,
        updated_at: DateTime<Utc>,
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
        self.updated_at = updated_at;
    }

    pub fn overlay_resident_tip_at(
        &mut self,
        hash: String,
        range: String,
        entry: AtomEntry,
        metadata: Option<KeyMetadata>,
        updated_at: DateTime<Utc>,
    ) {
        self.overlay_resident_atom_entry_at(hash.clone(), range.clone(), entry, updated_at);
        self.remove_key_metadata(&hash, &range);
        if let Some(metadata) = metadata {
            self.set_key_metadata(hash, range, metadata);
        }
    }
}
