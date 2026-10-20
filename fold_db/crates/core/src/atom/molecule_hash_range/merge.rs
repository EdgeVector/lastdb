//! LWW merge and metadata copy helpers.

use crate::security::Ed25519KeyPair;
use chrono::Utc;
use std::collections::BTreeMap;

use super::MoleculeHashRange;
use crate::atom::{AtomEntry, FieldKey, MergeConflict};

impl MoleculeHashRange {
    pub fn merge(&mut self, other: &Self, _keypair: &Ed25519KeyPair) -> Vec<MergeConflict> {
        let mut conflicts = Vec::new();
        let version_before = self.version;
        // Iterate `other.atom_uuids` in deterministic `(hash, range)` order
        // so the conflict list is the same on every node. The outer map is
        // a `HashMap` whose iter order is reseeded per instance.
        // `sample()` sorts live keys itself and does not use this walk.
        let mut other_entries: Vec<(&String, &BTreeMap<String, AtomEntry>)> =
            other.atom_uuids.iter().collect();
        other_entries.sort_by(|a, b| a.0.cmp(b.0));
        for (hash, other_range_map) in other_entries {
            for (range, other_entry) in other_range_map {
                let self_entry = self.atom_uuids.get(hash).and_then(|rm| rm.get(range));

                match self_entry {
                    None => {
                        self.atom_uuids
                            .entry(hash.clone())
                            .or_default()
                            .insert(range.clone(), other_entry.clone());
                        self.copy_key_metadata_from(other, hash, range);
                        self.version += 1;
                    }
                    Some(se) => {
                        // The clock selects the full tip and its metadata.
                        // An equal atom still accepts a newer author clock,
                        // but does not describe a conflict between atoms.
                        if crate::atom::incoming_wins_lww(other_entry.lww_key(), se.lww_key()) {
                            if se.atom_uuid != other_entry.atom_uuid {
                                let field_key = FieldKey::hash_range(hash.clone(), range.clone());
                                conflicts.push(MergeConflict {
                                    key: MergeConflict::display_key(&field_key),
                                    field_key,
                                    winner_atom: other_entry.atom_uuid.clone(),
                                    loser_atom: se.atom_uuid.clone(),
                                    winner_written_at: other_entry.written_at,
                                    loser_written_at: se.written_at,
                                });
                            }
                            self.record_replaced_tip(hash, range);
                            self.atom_uuids
                                .entry(hash.clone())
                                .or_default()
                                .insert(range.clone(), other_entry.clone());
                            self.copy_key_metadata_from(other, hash, range);
                            self.version += 1;
                        }
                    }
                }
            }
        }
        // New keys and newer tips advance the version, including equal atoms
        // whose author clock or metadata changes without an atom conflict.
        if self.version != version_before {
            self.updated_at = Utc::now();
        }
        conflicts
    }

    /// The winning tip owns its complete optional metadata. Absence clears
    /// the previous tip's metadata instead of attaching it to another write.
    fn copy_key_metadata_from(&mut self, source: &Self, hash: &str, range: &str) {
        self.remove_key_metadata(hash, range);
        if let Some(meta) = source.get_key_metadata(hash, range).cloned() {
            self.set_key_metadata(hash.to_string(), range.to_string(), meta);
        }
    }
}
