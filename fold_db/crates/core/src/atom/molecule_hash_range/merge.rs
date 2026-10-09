//! LWW merge and metadata copy helpers.

use crate::security::Ed25519KeyPair;
use chrono::Utc;
use std::collections::{BTreeMap, HashMap};

use super::MoleculeHashRange;
use crate::atom::{AtomEntry, FieldKey, KeyMetadata, MergeConflict};

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
                        // Carry over `other`'s per-key metadata for the newly
                        // merged-in entry. Field-side write code
                        // (`hash_range_field::write_mutation`) bundles
                        // `set_atom_uuid_from_values` and `set_key_metadata`
                        // into the same write, so the LWW per-key resolution
                        // applied to atoms must extend to metadata or the
                        // record's `source_file_name` / custom metadata is
                        // silently lost on cross-node sync — the merged-in
                        // atom surfaces with `key_meta = None` downstream
                        // (see `hash_range_field::resolve_value`).
                        Self::copy_key_metadata_from(
                            &mut self.key_metadata,
                            &other.key_metadata,
                            hash,
                            range,
                        );
                        self.version += 1;
                    }
                    Some(se) => {
                        if se.atom_uuid == other_entry.atom_uuid {
                            continue;
                        }
                        // Only record a conflict when LOCAL state actually
                        // changes (other wins). A self-won "conflict" is a
                        // no-op for this node — the rewind/history record
                        // emitted by
                        // `sync::SyncEngine::store_merge_conflicts` would
                        // be shaped `{ old: peer's atom, new: my atom }`,
                        // and `FieldVariant::rewind_to` would walk back to
                        // the peer's atom on any `as_of` query crossing
                        // that event, putting the molecule into a state
                        // this node was never in. The peer's own local
                        // audit records its loss symmetrically when it
                        // merges with us.
                        if crate::atom::incoming_wins_lww(other_entry.lww_key(), se.lww_key()) {
                            let field_key = FieldKey::hash_range(hash.clone(), range.clone());
                            conflicts.push(MergeConflict {
                                key: MergeConflict::display_key(&field_key),
                                field_key,
                                winner_atom: other_entry.atom_uuid.clone(),
                                loser_atom: se.atom_uuid.clone(),
                                winner_written_at: other_entry.written_at,
                                loser_written_at: se.written_at,
                            });
                            self.atom_uuids
                                .entry(hash.clone())
                                .or_default()
                                .insert(range.clone(), other_entry.clone());
                            // Other won the LWW — adopt its bundled
                            // key_metadata too, mirroring the field-side
                            // write contract that pairs each
                            // `set_atom_uuid_from_values` with a
                            // `set_key_metadata`.
                            Self::copy_key_metadata_from(
                                &mut self.key_metadata,
                                &other.key_metadata,
                                hash,
                                range,
                            );
                            self.version += 1;
                        }
                    }
                }
            }
        }
        // `version` is bumped exactly when the merge changes content (a new
        // key inserted or a conflict resolved in `other`'s favor), so a
        // change in `version` is the precise signal that `updated_at` should
        // advance. Gating on `!conflicts.is_empty()` alone misses the
        // new-key-inserted-without-conflict case — `updated_at` would lag
        // behind `version` on a real content change.
        if self.version != version_before {
            self.updated_at = Utc::now();
        }
        conflicts
    }

    /// Copy `other`'s per-key metadata at `(hash, range)` onto `target`,
    /// leaving `target` untouched when `other` has no metadata for that key.
    /// "No metadata on other" is treated as "no information" rather than
    /// "explicit removal" — the field-side write path always bundles
    /// metadata with the atom, so a missing entry on `other` means it was
    /// written by an older / non-field path and shouldn't erase metadata
    /// the local node may have set.
    fn copy_key_metadata_from(
        target: &mut HashMap<String, BTreeMap<String, KeyMetadata>>,
        source: &HashMap<String, BTreeMap<String, KeyMetadata>>,
        hash: &str,
        range: &str,
    ) {
        if let Some(meta) = source.get(hash).and_then(|rm| rm.get(range)).cloned() {
            target
                .entry(hash.to_string())
                .or_default()
                .insert(range.to_string(), meta);
        }
    }
}
