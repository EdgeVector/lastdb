//! Dirty tips replace every accepted spelling of their logical key.

use super::*;
use crate::atom::{AtomEntry, KeyMetadata, MoleculeKeyCodec};

pub(super) struct ResidentOverlay {
    pub(super) authoritative: bool,
    entries: Vec<OverlayEntry>,
    remove: HashSet<(String, String)>,
}

struct OverlayEntry {
    hash: String,
    range: String,
    entry: AtomEntry,
    metadata: Option<KeyMetadata>,
}

/// Match the loader's transient coordinates: composite hashes and exact
/// composite ranges use API keys, as do both 1-D multi-get dimensions.
struct OverlayKeys<'a> {
    codec: MoleculeKeyCodec,
    molecule: &'a str,
    api_hash: bool,
    api_range: bool,
}

impl<'a> OverlayKeys<'a> {
    fn new(
        db_ops: &DbOperations,
        molecule: &'a str,
        kind: FieldKind,
        filter: &HashRangeFilter,
    ) -> Self {
        let multi_get = matches!(filter, HashRangeFilter::HashRangeKeys(_));
        let api_hash =
            matches!(kind, FieldKind::HashRange) || (matches!(kind, FieldKind::Hash) && multi_get);
        Self {
            codec: db_ops.atoms().key_codec_for_molecule(molecule),
            molecule,
            api_hash,
            api_range: (matches!(kind, FieldKind::Range) && multi_get)
                || (matches!(kind, FieldKind::HashRange)
                    && matches!(
                        filter,
                        HashRangeFilter::HashRangeKey { .. } | HashRangeFilter::HashRangeKeys(_)
                    )),
        }
    }

    fn primary(&self, key: &ResidentMoleculeKey) -> Result<(String, String), SchemaError> {
        let hash = if self.api_hash {
            key.hash.clone()
        } else {
            self.codec
                .storage_hash(self.molecule, &key.hash)
                .map_err(|error| SchemaError::InvalidData(error.to_string()))?
        };
        let range = if self.api_range {
            key.range.clone()
        } else {
            self.codec
                .storage_range(self.molecule, &key.range)
                .map_err(|error| SchemaError::InvalidData(error.to_string()))?
        };
        Ok((hash, range))
    }

    /// At most two UUID spellings and two key candidates per coordinate.
    /// Remove these exact slots before the dirty tip replaces their winner.
    fn accepted(&self, key: &ResidentMoleculeKey) -> Result<Vec<(String, String)>, SchemaError> {
        let mut slots = Vec::new();
        for molecule in crate::atom::molecule_uuid::molecule_uuid_read_candidates(self.molecule) {
            let hashes = if self.api_hash {
                vec![key.hash.clone()]
            } else {
                self.codec
                    .storage_hash_read_candidates(&molecule, &key.hash)
                    .map_err(|error| SchemaError::InvalidData(error.to_string()))?
            };
            let ranges = if self.api_range {
                vec![key.range.clone()]
            } else {
                self.codec
                    .storage_range_read_candidates(&molecule, &key.range)
                    .map_err(|error| SchemaError::InvalidData(error.to_string()))?
            };
            for hash in hashes {
                for range in &ranges {
                    slots.push((hash.clone(), range.clone()));
                }
            }
        }
        Ok(slots)
    }
}

impl FieldVariant {
    pub(super) fn resident_overlay_for_read(
        db_ops: &DbOperations,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        filter: &HashRangeFilter,
        kind: FieldKind,
    ) -> Result<Option<ResidentOverlay>, SchemaError> {
        if !crate::db_operations::resident_read::tip_reads_resident_first(db_ops, storage_prefix) {
            return Ok(None);
        }
        let projected = match (kind, filter) {
            (FieldKind::Hash | FieldKind::Range, HashRangeFilter::HashRangeKeys(keys)) => {
                Some(HashRangeFilter::HashRangeKeys(
                    keys.iter()
                        .map(|(hash, range)| match kind {
                            FieldKind::Hash => (hash.clone(), String::new()),
                            _ => (String::new(), range.clone()),
                        })
                        .collect(),
                ))
            }
            _ => None,
        };
        let snapshot = if let Some(projected) = projected.as_ref() {
            resident_keys_for_filter(db_ops.resident(), molecule_uuid, projected)
        } else if matches!(kind, FieldKind::HashRange) {
            resident_keys_for_filter(db_ops.resident(), molecule_uuid, filter)
        } else {
            db_ops
                .resident()
                .resident_key_set_range(molecule_uuid, None, None)
        };
        let codec = OverlayKeys::new(db_ops, molecule_uuid, kind, filter);
        let mut entries = Vec::with_capacity(snapshot.keys.len());
        let mut remove = HashSet::new();
        for key in &snapshot.keys {
            let Some(tip) = db_ops
                .resident()
                .resolve_tip(molecule_uuid, &key.hash, &key.range)
            else {
                continue;
            };
            let (hash, range) = codec.primary(key)?;
            remove.extend(codec.accepted(key)?);
            let tip = tip.value;
            let mut entry = AtomEntry::thin_with_author(
                tip.atom_uuid,
                tip.written_at,
                tip.logical_counter,
                tip.device_id,
                tip.mutation_uuid,
                String::new(),
            );
            entry.writer_pubkey = tip.writer_pubkey;
            entries.push(OverlayEntry {
                hash,
                range,
                entry,
                metadata: tip.key_metadata,
            });
        }
        for key in &snapshot.tombstones {
            remove.extend(codec.accepted(key)?);
        }
        let authoritative = matches!(
            snapshot.completeness,
            ResidentKeySetCompleteness::Complete { .. }
        ) && entries.len() == snapshot.keys.len();
        let active = authoritative
            || (db_ops.resident().molecule_has_dirty(molecule_uuid)
                && (!entries.is_empty() || !remove.is_empty()));
        Ok(active.then_some(ResidentOverlay {
            authoritative,
            entries,
            remove,
        }))
    }

    /// Keep the dirty winner's clocks and key metadata after exact twin removal.
    pub(super) fn apply_resident_overlay(
        &mut self,
        molecule_uuid: &str,
        overlay: Option<ResidentOverlay>,
    ) {
        let Some(overlay) = overlay else { return };
        if overlay.entries.is_empty() && overlay.remove.is_empty() {
            return;
        }
        let molecule = self.molecule.get_or_insert_with(|| {
            crate::atom::MoleculeHashRange::from_per_key_records(
                molecule_uuid.to_string(),
                0,
                Utc::now(),
                Vec::new(),
            )
        });
        for (hash, range) in overlay.remove {
            molecule.remove_atom_uuid(&hash, &range);
        }
        for tip in overlay.entries {
            molecule.overlay_resident_atom_entry(tip.hash.clone(), tip.range.clone(), tip.entry);
            if let Some(metadata) = tip.metadata {
                molecule.set_key_metadata(tip.hash, tip.range, metadata);
            }
        }
    }
}
