//! Storage-free field validation and changed-key load plans.

use std::collections::{HashMap, HashSet};

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

/// Only a declared field can receive a value.
pub(super) fn validate_field_value(
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
