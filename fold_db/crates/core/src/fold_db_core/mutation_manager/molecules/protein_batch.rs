//! Exact batched protein metadata for one mutation.

use std::collections::{HashMap, HashSet};

use super::super::MutationManager;
use super::{scoped_gate::schema_is_scoped, PreparedSlotRevision};
use crate::atom::Atom;
use crate::db_operations::atom_store::AtomStore;
use crate::protein::Protein;
use crate::schema::types::{KeyValue, Mutation, Schema};
use crate::schema::SchemaError;

pub(super) type ProteinFoldMetadata = (
    HashMap<String, Option<String>>,
    HashMap<String, Option<Protein>>,
);

pub(super) fn schema_storage_prefix<'a>(
    schema: &'a Schema,
    molecule_uuid: Option<&str>,
) -> Option<&'a str> {
    schema.runtime_fields.values().find_map(|field| {
        let common = field.common();
        molecule_uuid
            .is_none_or(|uuid| common.molecule_uuid().is_some_and(|id| id == uuid))
            .then(|| common.storage_prefix())
            .flatten()
    })
}

fn distinct_in_order<'a>(values: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    let mut seen = HashSet::new();
    values
        .into_iter()
        .filter(|value| seen.insert(*value))
        .map(str::to_string)
        .collect()
}

pub(super) fn eligible_protein_atoms<'a>(
    schema: &'a Schema,
    schema_mutations: &[Mutation],
    mutation_key_values: &[KeyValue],
    atom_results: &'a [(usize, String, Atom)],
) -> Vec<(usize, &'a Atom, &'a str)> {
    let mut eligible = Vec::new();
    for (idx, field_name, atom) in atom_results {
        let key_value = &mutation_key_values[*idx];
        let Some(field) = schema.runtime_fields.get(field_name) else {
            continue;
        };
        let Some(mol_uuid) = field.common().molecule_uuid() else {
            continue;
        };
        let mutation = &schema_mutations[*idx];
        // Imported apply already LWW-decided the entry tip. A losing atom
        // neither reads a backref nor changes a sibling tip.
        if mutation.imported_written_at.is_some() {
            if let Some(mol) = field.molecule.as_ref() {
                let Some((hash, range)) = field.disk_slot_for_key(key_value) else {
                    continue;
                };
                if mol.get_atom_uuid(&hash, &range).map(String::as_str) != Some(atom.uuid()) {
                    continue;
                }
            }
        }
        eligible.push((*idx, atom, mol_uuid.as_str()));
    }
    eligible
}

pub(super) async fn load_protein_fold_metadata(
    store: &AtomStore,
    member_uuids: &[&str],
    storage_prefix: Option<&str>,
) -> Result<ProteinFoldMetadata, SchemaError> {
    let molecule_uuids = distinct_in_order(member_uuids.iter().copied());
    let owners = store
        .protein_of_molecules_in_scope(&molecule_uuids, storage_prefix)
        .await?;
    let owners_by_molecule: HashMap<String, Option<String>> =
        molecule_uuids.into_iter().zip(owners).collect();
    let protein_uuids = distinct_in_order(member_uuids.iter().filter_map(|mol_uuid| {
        owners_by_molecule
            .get(*mol_uuid)
            .and_then(Option::as_ref)
            .map(String::as_str)
    }));
    let proteins = store
        .protein_get_many_in_scope(&protein_uuids, storage_prefix)
        .await?;
    let proteins_by_uuid = protein_uuids.into_iter().zip(proteins).collect();
    Ok((owners_by_molecule, proteins_by_uuid))
}

impl MutationManager {
    /// Observe every planned sibling before any sibling tip load. The leases
    /// stay in the prepared delta until the apply gate accepts its snapshot.
    pub(super) fn prepare_protein_sibling_slots(
        &self,
        schema: &Schema,
        mutations: &[Mutation],
        keys: &[KeyValue],
        eligible: &[(usize, &Atom, &str)],
        metadata: &ProteinFoldMetadata,
    ) -> Vec<PreparedSlotRevision> {
        if schema_is_scoped(schema) {
            return Vec::new();
        }
        let entry_layout = schema.key.as_ref().and_then(|key| {
            key.hash_field
                .as_deref()
                .map(|hash| (hash, key.range_field.as_deref()))
        });
        let mut observed = HashSet::new();
        let mut prepared = Vec::new();
        for (idx, _atom, entry_molecule) in eligible {
            let Some(protein) = metadata
                .0
                .get(*entry_molecule)
                .and_then(Option::as_ref)
                .and_then(|uuid| metadata.1.get(uuid))
                .and_then(Option::as_ref)
            else {
                continue;
            };
            let fields = super::apply::sibling_key_fields(schema, &keys[*idx], &mutations[*idx]);
            for member in &protein.members {
                if member.molecule_uuid == *entry_molecule
                    && entry_layout.is_none_or(|(hash, range)| {
                        member.hash_field == hash && member.range_field.as_deref() == range
                    })
                {
                    continue;
                }
                let Some((hash, range)) = member.tip_coords_from_fields(&fields) else {
                    continue;
                };
                if observed.insert((member.molecule_uuid.clone(), hash.clone(), range.clone())) {
                    prepared.push(PreparedSlotRevision::capture(
                        self.resident_graph(),
                        &member.molecule_uuid,
                        &hash,
                        &range,
                    ));
                }
            }
        }
        prepared
    }
}
