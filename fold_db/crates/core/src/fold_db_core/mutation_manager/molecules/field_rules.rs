//! Storage-free field validation, changed-key loads, and accepted protein sources.

use std::collections::{HashMap, HashSet};

use super::prepared::{AcceptedSources, PreparedFieldAtom};
use crate::atom::Atom;
use crate::db_operations::ChangedKey;
use crate::schema::types::{KeyValue, Mutation, Schema};
use crate::schema::SchemaError;

/// One cold field owns its names and borrows this request's changed keys.
pub(super) struct FieldLoadPlan<'a> {
    pub(super) field_name: String,
    pub(super) molecule_uuid: String,
    pub(super) storage_prefix: Option<String>,
    pub(super) changed_keys: &'a HashSet<ChangedKey>,
}

/// Non-schema protein key values stay on the mutation but produce no atom.
pub(super) fn validate_field_value(
    schema_name: &str,
    schema: &Schema,
    field_name: &str,
    value: &serde_json::Value,
) -> Result<bool, SchemaError> {
    // Extra keys (e.g. sibling partition values for field_hash
    // protein fold) are not schema fields — keep them on the
    // mutation for fold field maps, but do not create atoms.
    if !schema.runtime_fields.contains_key(field_name) {
        return Ok(false);
    }
    let field_type = schema.get_field_type(field_name);
    if let Err(type_err) = field_type.validate(value) {
        return Err(SchemaError::InvalidData(format!(
            "Type error in field '{}' of schema '{}': {}. Expected {}, got {}",
            field_name,
            schema_name,
            type_err,
            field_type,
            serde_json::to_string(value).unwrap_or_else(|_| "?".to_string())
        )));
    }
    Ok(true)
}

/// Select the keys each declared field receives from this mutation batch.
/// Delete is hard-erased outside this path; only Create/Update fields land here.
pub(in crate::fold_db_core::mutation_manager) fn changed_keys_by_field(
    schema: &Schema,
    schema_mutations: &[Mutation],
    mutation_key_values: &[KeyValue],
) -> HashMap<String, HashSet<ChangedKey>> {
    let mut by_field: HashMap<String, HashSet<ChangedKey>> = HashMap::new();
    for (mutation, key_value) in schema_mutations.iter().zip(mutation_key_values.iter()) {
        for field_name in mutation.fields_and_values.keys() {
            let Some(field) = schema.runtime_fields.get(field_name) else {
                continue;
            };
            let Some(changed_key) = field.changed_key_for(key_value) else {
                continue;
            };
            by_field
                .entry(field_name.clone())
                .or_default()
                .insert(changed_key);
        }
    }
    by_field
}

/// Only changed fields with an unloaded molecule and nonempty keys need a read.
pub(super) fn cold_field_loads<'a>(
    schema: &Schema,
    changed_keys: &'a HashMap<String, HashSet<ChangedKey>>,
) -> Vec<FieldLoadPlan<'a>> {
    let mut plans = Vec::new();
    for (name, field) in &schema.runtime_fields {
        if field.common().molecule_uuid().is_none()
            || field.has_molecule()
            || !changed_keys.contains_key(name.as_str())
        {
            continue;
        }
        let Some(molecule_uuid) = field.common().molecule_uuid().cloned() else {
            continue;
        };
        let storage_prefix = field.common().storage_prefix().map(ToString::to_string);
        let Some(keys) = changed_keys.get(name.as_str()) else {
            continue;
        };
        if keys.is_empty() {
            continue;
        }
        plans.push(FieldLoadPlan {
            field_name: name.clone(),
            molecule_uuid,
            storage_prefix,
            changed_keys: keys,
        });
    }
    plans
}

/// Only the accepted authored source can update related protein molecules.
pub(super) fn eligible_protein_atoms<'a>(
    schema: &'a Schema,
    mutation_key_values: &[KeyValue],
    atom_results: &'a [PreparedFieldAtom],
    accepted: &AcceptedSources,
) -> Vec<(usize, &'a Atom, &'a str)> {
    let mut eligible = Vec::new();
    for PreparedFieldAtom {
        mutation_index: idx,
        field_name,
        atom,
    } in atom_results
    {
        let key_value = &mutation_key_values[*idx];
        let Some(field) = schema.runtime_fields.get(field_name) else {
            continue;
        };
        let Some(mol_uuid) = field.common().molecule_uuid() else {
            continue;
        };
        let source = (*idx, mol_uuid.clone(), atom.uuid().to_string());
        let Some(accepted) = accepted.get(&source) else {
            continue;
        };
        // A UUID identifies value bytes, not the authored write. An equal
        // value can lose its clock, or a later batch write can replace it.
        if field
            .atom_entry_at_key(key_value)
            .is_none_or(|entry| entry.lww_key() != accepted.lww_key())
        {
            continue;
        }
        eligible.push((*idx, atom, mol_uuid.as_str()));
    }
    eligible
}
