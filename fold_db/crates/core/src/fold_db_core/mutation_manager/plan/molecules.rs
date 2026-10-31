//! Molecule key decisions: which keys a write changes, which cold molecules
//! a write must load, and which keys the persist step writes.

use std::collections::{HashMap, HashSet};

use crate::atom::molecule_slot::ChangedKey;
use crate::atom::Atom;
use crate::schema::types::{KeyValue, Mutation, Schema};
use crate::schema::SchemaError;

use super::super::helpers::ModifiedFieldKeys;

/// A prepared atom keeps its source mutation position through publication.
pub(in crate::fold_db_core::mutation_manager) struct PreparedFieldAtom {
    pub mutation_index: usize,
    pub field_name: String,
    pub atom: Atom,
}

/// One cold field owns its names and borrows this request's changed keys.
pub(in crate::fold_db_core::mutation_manager) struct FieldLoadPlan<'a> {
    pub(in crate::fold_db_core::mutation_manager) field_name: String,
    pub(in crate::fold_db_core::mutation_manager) molecule_uuid: String,
    pub(in crate::fold_db_core::mutation_manager) storage_prefix: Option<String>,
    pub(in crate::fold_db_core::mutation_manager) changed_keys: &'a HashSet<ChangedKey>,
}

/// Only a declared field can receive a value.
pub(in crate::fold_db_core::mutation_manager) fn validate_field_value(
    schema_name: &str,
    schema: &Schema,
    field_name: &str,
    value: &serde_json::Value,
) -> Result<bool, SchemaError> {
    if !schema.runtime_fields.contains_key(field_name) {
        return Err(SchemaError::InvalidData(format!(
            "field '{field_name}' is not declared in schema '{schema_name}'"
        )));
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
pub(in crate::fold_db_core::mutation_manager) fn cold_field_loads<'a>(
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

/// The molecule keys a resident apply changed, per field: the persist step
/// writes only these `mk:` records.
///
/// Decided over the schema AFTER the in-memory apply. A field write assigns
/// a molecule identity to a field that had none, and only a field with an
/// identity persists. A field with an identity is listed even when its
/// change set is empty: a `Single` field has no per-key shape, and the
/// per-key store path takes it with an empty changed set.
pub(in crate::fold_db_core::mutation_manager) fn modified_field_keys(
    applied: &Schema,
    mutation_key_values: &[KeyValue],
    atom_results: &[PreparedFieldAtom],
) -> ModifiedFieldKeys {
    let mut modified: ModifiedFieldKeys = HashMap::new();
    for prepared in atom_results {
        let Some(field) = applied.runtime_fields.get(&prepared.field_name) else {
            continue;
        };
        if field.common().molecule_uuid().is_none() {
            continue;
        }
        let entry = modified.entry(prepared.field_name.clone()).or_default();
        if let Some(changed_key) =
            field.changed_key_for(&mutation_key_values[prepared.mutation_index])
        {
            entry.insert(changed_key);
        }
    }
    modified
}

/// Whether a schema feeds the retention-age or hash-partition trackers.
/// Only the personal scope tracks retention.
pub(in crate::fold_db_core::mutation_manager) fn retention_tracking(
    schema: &Schema,
    storage_prefix: Option<&str>,
) -> (bool, bool) {
    use crate::schema::types::schema::DeclarativeSchemaType as Kind;
    let tracks_age =
        storage_prefix.is_none() && matches!(&schema.schema_type, Kind::Hash | Kind::Single);
    let tracks_hash_partitions =
        storage_prefix.is_none() && matches!(&schema.schema_type, Kind::HashRange);
    (tracks_age, tracks_hash_partitions)
}
