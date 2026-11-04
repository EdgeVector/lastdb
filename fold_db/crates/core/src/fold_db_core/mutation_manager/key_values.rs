//! The primary key of each mutation and the field values that a record can hold.

use super::MutationManager;
use crate::schema::types::{KeyValue, Mutation, Schema};
use crate::schema::SchemaError;

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) fn resolve_mutation_key_value(
        &self,
        _schema_name: &str,
        schema: &Schema,
        mutation: &Mutation,
    ) -> Result<KeyValue, SchemaError> {
        crate::schema::require_logical_layout(&schema.schema_type)?;
        mutation
            .target()
            .validate(schema.require_record_keys()?)
            .map_err(SchemaError::InvalidData)?;
        self.db_ops
            .field_keys()
            .canonical_key(schema, mutation.key_value.clone())
    }
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
