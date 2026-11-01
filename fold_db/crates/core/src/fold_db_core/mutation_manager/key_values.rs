//! The primary key of each mutation and the field values that a record can hold.

use super::MutationManager;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Mutation, Schema};
use crate::schema::SchemaError;

mod key_fields;
mod key_rules;

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) fn resolve_mutation_key_value(
        &self,
        schema_name: &str,
        schema: &Schema,
        mutation: &Mutation,
    ) -> Result<KeyValue, SchemaError> {
        crate::schema::require_logical_layout(&schema.schema_type)?;
        // Prefer the pre-computed key_value from the mutation (set by the
        // ingestion service with date normalization and proper field extraction).
        // Fall back to KeyValue::from_mutation() only when both fields are None.
        let key_value = if mutation.key_value.hash.is_some() || mutation.key_value.range.is_some() {
            mutation.key_value.clone()
        } else {
            let key_config = schema.key.clone();
            KeyValue::from_mutation(
                &mutation.fields_and_values,
                key_config.as_ref().ok_or_else(|| {
                    SchemaError::InvalidData(format!(
                        "Schema '{schema_name}' has no key configuration. Cannot execute mutation."
                    ))
                })?,
            )
        };

        if schema.schema_type == DeclarativeSchemaType::Hash && key_value.range.is_some() {
            return Err(SchemaError::InvalidData(
                "a Hash mutation cannot supply an undeclared range key".into(),
            ));
        }

        // Validate that the key matches the schema type.
        // A mismatch means a bug upstream — fail loudly.
        match &schema.schema_type {
            DeclarativeSchemaType::Hash if key_value.hash.is_none() => Err(
                SchemaError::InvalidData(format!(
                    "Hash schema '{}' mutation {} has no hash key",
                    schema_name, mutation.uuid
                )),
            ),
            DeclarativeSchemaType::HashRange
                if key_value.hash.is_none() || key_value.range.is_none() =>
            {
                Err(SchemaError::InvalidData(format!(
                    "HashRange schema '{}' mutation {} requires both hash and range keys, got hash={:?} range={:?}",
                    schema_name, mutation.uuid, key_value.hash, key_value.range
                )))
            }
            _ => Ok(key_value),
        }.and_then(|key| self.db_ops.field_keys().canonical_key(schema, key))
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
