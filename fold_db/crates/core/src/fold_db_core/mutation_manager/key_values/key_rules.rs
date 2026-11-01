//! Storage-free key-field selection, consistency, and value conversion rules.

use crate::schema::types::key_value::scalar_key_coordinate;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{FieldValueType, KeyValue, Mutation, MutationType, Schema};
use crate::schema::SchemaError;
use schema_types::field_range_profile::FieldRangeProfile;

pub(super) type KeyFieldPairs<'a> = [Option<(&'a str, &'a str)>; 2];

pub(super) fn key_field_pairs<'a>(
    schema_name: &str,
    schema: &'a Schema,
    mutation: &Mutation,
    key_value: &'a KeyValue,
) -> Result<KeyFieldPairs<'a>, SchemaError> {
    // An erasure merges as a delete floor and writes no key field.
    if !matches!(
        mutation.mutation_type,
        MutationType::Create | MutationType::Update
    ) {
        return Ok([None, None]);
    }
    let hash_only = matches!(schema.schema_type, DeclarativeSchemaType::Hash);
    if !hash_only && !matches!(schema.schema_type, DeclarativeSchemaType::HashRange) {
        return Ok([None, None]);
    }
    let hash_field = schema
        .key
        .as_ref()
        .and_then(|key| key.hash_field.as_deref());
    if hash_only && hash_field.is_none() {
        // A Hash schema without a declared key field has no field to fill.
        return Ok([None, None]);
    }
    let hash_field = hash_field.ok_or_else(|| {
        SchemaError::InvalidData(format!("Schema '{schema_name}' has no declared hash field"))
    })?;
    let hash = key_value.hash.as_deref().ok_or_else(|| {
        SchemaError::InvalidData(format!(
            "Schema '{schema_name}' mutation {} has no hash coordinate",
            mutation.uuid
        ))
    })?;
    let hash = Some((hash_field, hash));
    if hash_only {
        return Ok([hash, None]);
    }
    let range_field = schema
        .key
        .as_ref()
        .and_then(|key| key.range_field.as_deref())
        .ok_or_else(|| {
            SchemaError::InvalidData(format!(
                "Schema '{schema_name}' has no declared range field"
            ))
        })?;
    let range = key_value.range.as_deref().ok_or_else(|| {
        SchemaError::InvalidData(format!(
            "Schema '{schema_name}' mutation {} has no range coordinate",
            mutation.uuid
        ))
    })?;
    Ok([hash, Some((range_field, range))])
}

pub(super) fn validated_materialized_key_field(
    schema_name: &str,
    schema: &Schema,
    mutation: &Mutation,
    field_name: &str,
    coordinate: &str,
    profile: FieldRangeProfile,
) -> Result<Option<serde_json::Value>, SchemaError> {
    if !schema.runtime_fields.contains_key(field_name) {
        return Err(SchemaError::InvalidData(format!(
            "Schema '{schema_name}' key field '{field_name}' is not a runtime field"
        )));
    }
    if let Some(explicit) = mutation.fields_and_values.get(field_name) {
        let explicit_coordinate = scalar_key_coordinate(explicit).ok_or_else(|| {
            SchemaError::InvalidData(format!(
                "Schema '{schema_name}' mutation {} key field '{field_name}' must be a scalar",
                mutation.uuid
            ))
        })?;
        let is_range = schema
            .key
            .as_ref()
            .and_then(|key| key.range_field.as_deref())
            == Some(field_name);
        let matches = if is_range {
            crate::schema::types::key_comparison::normalize::canonical_range(
                profile,
                &explicit_coordinate,
            )? == coordinate
        } else {
            explicit_coordinate == coordinate
        };
        if !matches {
            return Err(SchemaError::InvalidData(format!(
                "Schema '{schema_name}' mutation {} key field '{field_name}' conflicts with key_value: payload={explicit_coordinate:?}, coordinate={coordinate:?}",
                mutation.uuid
            )));
        }
        return Ok(None);
    }

    let value = materialized_key_value(schema.get_field_type(field_name), coordinate).ok_or_else(
        || {
            SchemaError::InvalidData(format!(
                "Schema '{schema_name}' cannot materialize key field '{field_name}' with type {} from coordinate {coordinate:?}",
                schema.get_field_type(field_name)
            ))
        },
    )?;
    if scalar_key_coordinate(&value).as_deref() != Some(coordinate) {
        return Err(SchemaError::InvalidData(format!(
            "Schema '{schema_name}' key field '{field_name}' type {} cannot preserve coordinate {coordinate:?}",
            schema.get_field_type(field_name)
        )));
    }
    Ok(Some(value))
}

fn materialized_key_value(
    field_type: &FieldValueType,
    coordinate: &str,
) -> Option<serde_json::Value> {
    use serde_json::Value;

    match field_type {
        FieldValueType::Any | FieldValueType::String => Some(Value::String(coordinate.to_string())),
        FieldValueType::Date | FieldValueType::DateTime => {
            let value = Value::String(coordinate.to_string());
            field_type.validate(&value).ok().map(|()| value)
        }
        FieldValueType::Integer => coordinate
            .parse::<i64>()
            .map(serde_json::Number::from)
            .or_else(|_| coordinate.parse::<u64>().map(serde_json::Number::from))
            .ok()
            .map(Value::Number),
        FieldValueType::Float | FieldValueType::Number => coordinate
            .parse::<serde_json::Number>()
            .ok()
            .map(Value::Number),
        FieldValueType::Boolean => coordinate.parse::<bool>().ok().map(Value::Bool),
        FieldValueType::OneOf(variants) => variants.iter().find_map(|variant| {
            materialized_key_value(variant, coordinate)
                .filter(|value| scalar_key_coordinate(value).as_deref() == Some(coordinate))
        }),
        FieldValueType::Null
        | FieldValueType::Array(_)
        | FieldValueType::Map(_)
        | FieldValueType::Object(_)
        | FieldValueType::SchemaRef(_) => None,
    }
}
