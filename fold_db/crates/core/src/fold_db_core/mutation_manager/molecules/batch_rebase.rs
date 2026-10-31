//! One physical-slot base shared by every schema delta in an apply transaction.

use std::collections::HashMap;

use crate::atom::{AtomEntry, KeyMetadata, MoleculeHashRange};
use crate::db_operations::ChangedKey;
use crate::schema::types::Schema;

use super::super::MutationManager;
use super::delta::PreparedSchemaDelta;

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
    }
}

fn empty_molecule(uuid: &str, version: u64) -> MoleculeHashRange {
    MoleculeHashRange::from_write_records(uuid.to_string(), version, chrono::Utc::now(), Vec::new())
}

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) fn rebase_delta_for_publish(
        &self,
        delta: &mut PreparedSchemaDelta,
        heads: &mut BatchSlotHeads,
    ) -> super::super::helpers::ModifiedFieldKeys {
        heads.overlay_schema(delta);
        let mut prospective = delta.schema.clone();
        let modified = self.apply_mutations_to_molecules(
            &mut prospective,
            &delta.schema_mutations,
            &delta.mutation_key_values,
            &delta.atom_results,
        );
        heads.record_schema(&prospective, &delta.changed_keys);
        delta.schema = prospective;
        modified
    }
}
