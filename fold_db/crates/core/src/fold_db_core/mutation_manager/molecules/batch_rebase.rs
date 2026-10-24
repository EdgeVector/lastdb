//! One physical-slot base shared by every schema delta in an apply transaction.

use std::collections::HashMap;

use crate::atom::{AtomEntry, ImportedTip, KeyMetadata, MoleculeHashRange};
use crate::db_operations::ChangedKey;
use crate::schema::types::Schema;
use crate::schema::SchemaError;

use super::super::MutationManager;
use super::delta::PreparedSchemaDelta;
use super::prepared::{AcceptedSources, SourceId};

type Slot = (String, String, String);

#[derive(Clone)]
struct SlotHead {
    entry: AtomEntry,
    metadata: Option<KeyMetadata>,
}

impl SlotHead {
    fn at(data: &MoleculeHashRange, hash: &str, range: &str) -> Option<Self> {
        Some(Self {
            entry: data.get_atom_entry(hash, range)?.clone(),
            metadata: data.get_key_metadata(hash, range).cloned(),
        })
    }

    fn overlay(&self, data: &mut MoleculeHashRange, hash: &str, range: &str) {
        data.overlay_resident_tip(
            hash.to_string(),
            range.to_string(),
            self.entry.clone(),
            self.metadata.clone(),
        );
    }
}

#[derive(Default)]
pub(in crate::fold_db_core::mutation_manager) struct BatchSlotHeads(HashMap<Slot, SlotHead>);

impl BatchSlotHeads {
    fn overlay_schema(&self, delta: &mut PreparedSchemaDelta) {
        self.overlay_changed_schema(&mut delta.schema, &delta.changed_keys);
    }

    pub(super) fn overlay_changed_schema(
        &self,
        schema: &mut Schema,
        changed_keys: &HashMap<String, std::collections::HashSet<ChangedKey>>,
    ) {
        for (name, changed) in changed_keys {
            let Some(field) = schema.runtime_fields.get_mut(name) else {
                continue;
            };
            let Some(uuid) = field.common().molecule_uuid().cloned() else {
                continue;
            };
            for key in changed {
                let (hash, range) = (key.hash_or_empty(), key.range_or_empty());
                let Some(head) = self
                    .0
                    .get(&(uuid.clone(), hash.to_string(), range.to_string()))
                else {
                    continue;
                };
                let data = field
                    .molecule
                    .get_or_insert_with(|| empty_molecule(&uuid, 0));
                head.overlay(data, hash, range);
            }
        }
    }

    pub(super) fn overlay_field(
        &self,
        field: &mut crate::schema::types::field::FieldVariant,
        key: &crate::schema::types::KeyValue,
    ) {
        let Some(uuid) = field.common().molecule_uuid().cloned() else {
            return;
        };
        let Some((hash, range)) = field.slot_coordinates(key) else {
            return;
        };
        if let Some(head) = self.0.get(&(uuid.clone(), hash.clone(), range.clone())) {
            head.overlay(
                field
                    .molecule
                    .get_or_insert_with(|| empty_molecule(&uuid, 0)),
                &hash,
                &range,
            );
        }
    }

    pub(super) fn record_field(
        &mut self,
        field: &crate::schema::types::field::FieldVariant,
        key: &crate::schema::types::KeyValue,
    ) {
        let (Some(uuid), Some(data), Some(changed)) = (
            field.common().molecule_uuid(),
            field.molecule.as_ref(),
            field.changed_key_for(key),
        ) else {
            return;
        };
        self.record_slot(uuid, data, &changed);
    }

    fn record_schema(
        &mut self,
        schema: &Schema,
        changed: &HashMap<String, std::collections::HashSet<ChangedKey>>,
    ) {
        for (name, keys) in changed {
            let Some(field) = schema.runtime_fields.get(name) else {
                continue;
            };
            let (Some(uuid), Some(data)) =
                (field.common().molecule_uuid(), field.molecule.as_ref())
            else {
                continue;
            };
            for key in keys {
                self.record_slot(uuid, data, key);
            }
        }
    }

    fn record_slot(&mut self, uuid: &str, data: &MoleculeHashRange, key: &ChangedKey) {
        let (hash, range) = (key.hash_or_empty(), key.range_or_empty());
        if let Some(head) = SlotHead::at(data, hash, range) {
            self.0.insert(
                (uuid.to_string(), hash.to_string(), range.to_string()),
                head,
            );
        }
    }

    pub(in crate::fold_db_core::mutation_manager) fn record(
        &mut self,
        delta: &PreparedSchemaDelta,
    ) {
        self.record_schema(&delta.schema, &delta.changed_keys);
        for (uuid, data, changed) in &delta.sibling_updates {
            for key in changed {
                self.record_slot(uuid, data, key);
            }
        }
    }
}

fn empty_molecule(uuid: &str, version: u64) -> MoleculeHashRange {
    MoleculeHashRange::from_write_records(uuid.to_string(), version, chrono::Utc::now(), Vec::new())
}

/// A moved unchanged value keeps its Atom but uses the accepted move clock.
fn sibling_source(
    source: &SlotHead,
    replacement: Option<&(String, Option<KeyMetadata>)>,
) -> SlotHead {
    let mut source = source.clone();
    if let Some((atom, metadata)) = replacement {
        let entry = &source.entry;
        source.entry = AtomEntry::thin_with_author(
            atom.clone(),
            entry.written_at,
            entry.logical_counter,
            entry.lww_device().to_owned(),
            entry.mutation_uuid.clone(),
            String::new(),
        )
        .with_origin_device(entry.origin_device.clone());
        source.metadata.clone_from(metadata);
    }
    source
}

fn winning_sources(
    delta: &PreparedSchemaDelta,
    schema: &Schema,
    accepted: &AcceptedSources,
) -> HashMap<SourceId, SlotHead> {
    delta
        .atom_results
        .iter()
        .filter_map(|prepared| {
            let idx = prepared.mutation_index;
            let name = &prepared.field_name;
            let atom = &prepared.atom;
            let field = schema.runtime_fields.get(name)?;
            let uuid = field.common().molecule_uuid()?;
            let source = (idx, uuid.clone(), atom.uuid().to_string());
            let accepted = accepted.get(&source)?;
            let (hash, range) = field.slot_coordinates(&delta.mutation_key_values[idx])?;
            let head = SlotHead::at(field.molecule.as_ref()?, &hash, &range)?;
            (head.entry.lww_key() == accepted.lww_key()).then_some((source, head))
        })
        .collect()
}

impl MutationManager {
    /// Rebase both the serving delta and its persist envelope under the held gates.
    pub(in crate::fold_db_core::mutation_manager) async fn rebase_delta_for_publish(
        &self,
        delta: &mut PreparedSchemaDelta,
        heads: &mut BatchSlotHeads,
    ) -> Result<
        (
            super::super::helpers::ModifiedFieldKeys,
            std::time::Duration,
        ),
        SchemaError,
    > {
        heads.overlay_schema(delta);
        let mut prospective = delta.schema.clone();
        let (modified, accepted) = self.apply_mutations_to_molecules(
            &mut prospective,
            &delta.schema_mutations,
            &delta.mutation_key_values,
            &delta.atom_results,
        );
        let sources = winning_sources(delta, &prospective, &accepted);
        let fold_started = std::time::Instant::now();
        let eligible = super::protein_batch::eligible_protein_atoms(
            &prospective,
            &delta.mutation_key_values,
            &delta.atom_results,
            &accepted,
        );
        // Select sibling coordinates only after the source's final LWW
        // decision. All reads remain inside the private native transaction.
        let (updates, origins, moves) = self
            .fold_protein_siblings_after_write(
                &delta.schema,
                &delta.schema_mutations,
                &delta.mutation_key_values,
                &eligible,
            )
            .await?;
        let fold_elapsed = fold_started.elapsed();
        delta.sibling_updates = updates;
        delta.sibling_origins = origins;
        delta.sibling_moves = moves;
        delta
            .sibling_moves
            .retain(|movement| sources.contains_key(&movement.source));
        heads.record_schema(&prospective, &delta.changed_keys);
        delta.schema = prospective;
        let updates = std::mem::take(&mut delta.sibling_updates);
        let origins = std::mem::take(&mut delta.sibling_origins);
        for ((uuid, data, changed), origin) in updates.into_iter().zip(origins) {
            let Some(source) = sources.get(&origin.source) else {
                continue;
            };
            let source = sibling_source(source, origin.replacement.as_ref());
            if changed.is_empty() {
                continue;
            }
            let mut rebased = empty_molecule(&uuid, data.version());
            for key in &changed {
                let (hash, range) = (key.hash_or_empty(), key.range_or_empty());
                let slot = (uuid.clone(), hash.to_string(), range.to_string());
                let base = heads.0.get(&slot).cloned().or_else(|| {
                    origin
                        .base
                        .clone()
                        .map(|(entry, metadata)| SlotHead { entry, metadata })
                });
                if let Some(base) = base {
                    base.overlay(&mut rebased, hash, range);
                }
                Self::apply_sibling_source(&mut rebased, key, &source);
                heads.record_slot(&uuid, &rebased, key);
            }
            delta.sibling_updates.push((uuid, rebased, changed));
            delta.sibling_origins.push(origin);
        }
        Ok((modified, fold_elapsed))
    }

    fn apply_sibling_source(data: &mut MoleculeHashRange, key: &ChangedKey, source: &SlotHead) {
        let (hash, range) = (key.hash_or_empty(), key.range_or_empty());
        let entry = &source.entry;
        let accepted = data.set_atom_uuid_from_values_imported_with_author(
            hash.to_string(),
            range.to_string(),
            ImportedTip {
                atom_uuid: entry.atom_uuid.clone(),
                device_id: entry.lww_device().to_string(),
                origin_device: entry.origin_device.clone(),
                writer_pubkey: entry.writer_pubkey().to_string(),
                signature: entry.signature().to_string(),
                signature_version: entry.signature_version(),
                written_at: Some(entry.written_at),
                logical_counter: entry.logical_counter,
                mutation_uuid: entry.mutation_uuid.clone(),
            },
        );
        if accepted {
            let winner = data
                .get_atom_entry(hash, range)
                .expect("accepted sibling tip")
                .clone();
            data.overlay_resident_tip(
                hash.to_string(),
                range.to_string(),
                winner,
                source.metadata.clone(),
            );
        }
    }
}
