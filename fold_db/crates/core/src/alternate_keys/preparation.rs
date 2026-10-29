//! Select only new records and writes that can change alternate coordinates.

use super::*;
use crate::schema::types::Mutation;
use std::collections::{BTreeSet, HashSet};

pub(crate) fn affected_keys(
    schema: &Schema,
    mutations: &[Mutation],
    keys: &[KeyValue],
) -> Vec<KeyValue> {
    if schema.alternate_keys.is_empty() {
        return Vec::new();
    }
    let primary = schema
        .key
        .as_ref()
        .and_then(|key| key.hash_field.as_deref());
    let dependencies: BTreeSet<_> = schema
        .alternate_keys
        .values()
        .flat_map(schema_types::AlternateKey::fields)
        .filter(|name| Some(*name) != primary)
        .collect();
    let primary_field = primary.and_then(|name| schema.runtime_fields.get(name));
    let mut seen = HashSet::new();
    mutations
        .iter()
        .zip(keys)
        .filter_map(|(mutation, key)| {
            let exists = primary_field
                .and_then(|field| {
                    field
                        .molecule_data()?
                        .get_atom_entry(key.hash.as_deref().unwrap_or_default(), "")
                })
                .is_some();
            let changes_key = mutation
                .fields_and_values
                .keys()
                .any(|name| dependencies.contains(name.as_str()));
            ((!exists || changes_key) && seen.insert(key.clone())).then(|| key.clone())
        })
        .collect()
}
