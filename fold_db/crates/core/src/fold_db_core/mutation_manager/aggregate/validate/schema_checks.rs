//! Schema shape checks shared by contract validation and enrolled-schema validation.
//!
//! `verb` is the constraint phrase in the error text: "have type" when a new
//! contract is validated, "remain" when an enrolled schema is re-checked.

use super::*;
use crate::schema::types::aggregate::{
    AGGREGATE_VALUES_FIELD, AGGREGATE_WINNER_COUNTER_FIELD, AGGREGATE_WINNER_WRITTEN_AT_FIELD,
};
use crate::schema::types::Schema;

fn key_fields(schema: &Schema) -> impl Iterator<Item = &str> {
    schema.key.iter().flat_map(|key| {
        key.hash_field
            .as_deref()
            .into_iter()
            .chain(key.range_field.as_deref())
    })
}

pub(super) fn declares_reserved_aggregate_field(schema: &Schema) -> bool {
    schema.runtime_fields.keys().any(|field| {
        field == AGGREGATE_VALID_FIELD
            || field == AGGREGATE_GUARD_TOKEN_FIELD
            || AGGREGATE_MEMBER_RESERVED_FIELDS.contains(&field.as_str())
    })
}

/// True when a target key field is an aggregate-owned field or a metric field.
pub(super) fn target_key_uses_owned_field(
    schema: &Schema,
    is_metric: impl Fn(&str) -> bool,
) -> bool {
    key_fields(schema).any(|field| {
        field == AGGREGATE_VALID_FIELD || field == AGGREGATE_GUARD_TOKEN_FIELD || is_metric(field)
    })
}

pub(super) fn member_key_uses_reserved_field(schema: &Schema) -> bool {
    key_fields(schema).any(|field| AGGREGATE_MEMBER_RESERVED_FIELDS.contains(&field))
}

/// The valid and guard-token fields the target schema must carry.
pub(super) fn check_target_owned_fields(
    schema_name: &str,
    schema: &Schema,
    verb: &str,
) -> Result<(), SchemaError> {
    for (field, expected) in [
        (AGGREGATE_VALID_FIELD, FieldValueType::Integer),
        (AGGREGATE_GUARD_TOKEN_FIELD, FieldValueType::String),
    ] {
        if !schema.runtime_fields.contains_key(field) || schema.get_field_type(field) != &expected {
            return Err(SchemaError::InvalidField(format!(
                "aggregate target field '{schema_name}.{field}' must {verb} {expected:?}"
            )));
        }
    }
    Ok(())
}

/// Winner, values and remaining reserved member fields keep their declared types.
pub(super) fn check_member_field_types(
    schema_name: &str,
    schema: &Schema,
    verb: &str,
) -> Result<(), SchemaError> {
    for field in [
        AGGREGATE_WINNER_COUNTER_FIELD,
        AGGREGATE_WINNER_WRITTEN_AT_FIELD,
    ] {
        if schema.get_field_type(field) != &FieldValueType::String {
            return Err(SchemaError::InvalidField(format!(
                "aggregate member field '{schema_name}.{field}' must {verb} String"
            )));
        }
    }
    if schema.get_field_type(AGGREGATE_VALUES_FIELD) != &FieldValueType::Any {
        return Err(SchemaError::InvalidField(format!(
            "aggregate member field '{schema_name}.{AGGREGATE_VALUES_FIELD}' must {verb} Any"
        )));
    }
    for field in AGGREGATE_MEMBER_RESERVED_FIELDS
        .iter()
        .copied()
        .filter(|field| {
            !matches!(
                *field,
                AGGREGATE_WINNER_COUNTER_FIELD
                    | AGGREGATE_WINNER_WRITTEN_AT_FIELD
                    | AGGREGATE_VALUES_FIELD
            )
        })
    {
        if schema.get_field_type(field) != &FieldValueType::String {
            return Err(SchemaError::InvalidField(format!(
                "aggregate member field '{schema_name}.{field}' must {verb} String"
            )));
        }
    }
    Ok(())
}

/// Every payload field is declared, live, and type-correct.
pub(super) fn check_source_payload(source: &Mutation, schema: &Schema) -> Result<(), SchemaError> {
    for (field, value) in &source.fields_and_values {
        if !schema.runtime_fields.contains_key(field) {
            return Err(SchemaError::InvalidField(format!(
                "aggregate source schema '{}' does not declare payload field '{field}'",
                source.schema_name
            )));
        }
        if crate::atom::is_tombstone_value(value) {
            return Err(SchemaError::InvalidData(
                "aggregate source mutation rejects tombstone-valued fields; write a live logical removal value with an explicit zero contribution"
                    .into(),
            ));
        }
        let field_type = schema.get_field_type(field);
        if let Err(type_err) = field_type.validate(value) {
            return Err(SchemaError::InvalidData(format!(
                "Type error in field '{}' of schema '{}': {}. Expected {}, got {}",
                field,
                source.schema_name,
                type_err,
                field_type,
                serde_json::to_string(value).unwrap_or_else(|_| "?".to_string())
            )));
        }
    }
    Ok(())
}

pub(super) fn check_source_has_non_key_field(
    source: &Mutation,
    schema: &Schema,
) -> Result<(), SchemaError> {
    let source_key_fields: std::collections::HashSet<&str> = key_fields(schema).collect();
    if !source
        .fields_and_values
        .keys()
        .any(|field| !source_key_fields.contains(field.as_str()))
    {
        return Err(SchemaError::InvalidData(
            "aggregate source mutation must persist at least one live declared non-key field"
                .into(),
        ));
    }
    Ok(())
}
