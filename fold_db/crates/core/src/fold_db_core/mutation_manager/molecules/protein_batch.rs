//! Exact batched protein metadata for one mutation.

use std::collections::{HashMap, HashSet};

use crate::atom::Atom;
use crate::db_operations::atom_store::AtomStore;
use crate::protein::Protein;
use crate::schema::types::{KeyValue, Mutation, Schema};
use crate::schema::SchemaError;

pub(super) type ProteinFoldMetadata = (
    HashMap<String, Option<String>>,
    HashMap<String, Option<Protein>>,
);

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
                let hash = key_value.hash.as_deref().unwrap_or("");
                let range = key_value.range.as_deref().unwrap_or("");
                if mol.get_atom_uuid(hash, range).map(String::as_str) != Some(atom.uuid()) {
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
) -> Result<ProteinFoldMetadata, SchemaError> {
    let molecule_uuids = distinct_in_order(member_uuids.iter().copied());
    let owners = store.protein_of_molecules(&molecule_uuids).await?;
    let owners_by_molecule: HashMap<String, Option<String>> =
        molecule_uuids.into_iter().zip(owners).collect();
    let protein_uuids = distinct_in_order(member_uuids.iter().filter_map(|mol_uuid| {
        owners_by_molecule
            .get(*mol_uuid)
            .and_then(Option::as_ref)
            .map(String::as_str)
    }));
    let proteins = store.protein_get_many(&protein_uuids).await?;
    let proteins_by_uuid = protein_uuids.into_iter().zip(proteins).collect();
    Ok((owners_by_molecule, proteins_by_uuid))
}
