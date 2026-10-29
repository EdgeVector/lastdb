//! Exact batched protein metadata for one mutation.

use std::collections::{HashMap, HashSet};

use crate::db_operations::atom_store::AtomStore;
use crate::protein::Protein;
use crate::schema::types::Schema;
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
