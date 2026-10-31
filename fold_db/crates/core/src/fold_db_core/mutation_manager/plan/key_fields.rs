//! Mutation key decisions: the requested key, its shape check, and the
//! key-field selection, consistency, and value conversion rules.

use crate::schema::types::key_value::scalar_key_coordinate;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{FieldValueType, KeyValue, Mutation, MutationType, Schema};
use crate::schema::SchemaError;
use schema_types::molecule_range_profile::MoleculeRangeProfile;

/// The key a mutation asks for, before the schema's canonical key form.
pub(in crate::fold_db_core::mutation_manager) struct RequestedKey {
    pub(in crate::fold_db_core::mutation_manager) key_value: KeyValue,
    /// A hash-only schema ignored a supplied range key. The caller logs it.
    pub(in crate::fold_db_core::mutation_manager) dropped_range: bool,
}

/// Prefer the pre-computed `key_value` from the mutation (set by the
/// ingestion service with date normalization and proper field extraction).
/// Fall back to the schema key fields only when both components are `None`.
pub(in crate::fold_db_core::mutation_manager) fn requested_key(
    schema_name: &str,
    schema: &Schema,
    mutation: &Mutation,
) -> Result<RequestedKey, SchemaError> {
    crate::schema::require_logical_layout(&schema.schema_type)?;
    let mut key_value = if mutation.key_value.hash.is_some() || mutation.key_value.range.is_some() {
        mutation.key_value.clone()
    } else {
        KeyValue::from_mutation(
            &mutation.fields_and_values,
            schema.key.as_ref().ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "Schema '{schema_name}' has no key configuration. Cannot execute mutation."
                ))
            })?,
        )
    };
    let mut dropped_range = false;
    if schema.schema_type == DeclarativeSchemaType::Hash && key_value.range.is_some() {
        if schema.uses_independent_fields() {
            return Err(SchemaError::InvalidData(
                "an independent Hash mutation cannot supply an undeclared range key".into(),
            ));
        }
        key_value.range = None;
        dropped_range = true;
    }
    Ok(RequestedKey {
        key_value,
        dropped_range,
    })
}

/// Validate that the key matches the schema type.
/// A mismatch means a bug upstream — fail loudly.
pub(in crate::fold_db_core::mutation_manager) fn checked_key_shape(
    schema_name: &str,
    schema: &Schema,
    mutation: &Mutation,
    key_value: KeyValue,
) -> Result<KeyValue, SchemaError> {
    match &schema.schema_type {
        DeclarativeSchemaType::Hash if key_value.hash.is_none() => {
            Err(SchemaError::InvalidData(format!(
                "Hash schema '{}' mutation {} has no hash key",
                schema_name, mutation.uuid
            )))
        }
        DeclarativeSchemaType::HashRange
            if key_value.hash.is_none() || key_value.range.is_none() =>
        {
            Err(SchemaError::InvalidData(format!(
                "HashRange schema '{}' mutation {} requires both hash and range keys, got hash={:?} range={:?}",
                schema_name, mutation.uuid, key_value.hash, key_value.range
            )))
        }
        _ => Ok(key_value),
    }
}

pub(in crate::fold_db_core::mutation_manager) type KeyFieldPairs<'a> =
    [Option<(&'a str, &'a str)>; 2];

pub(in crate::fold_db_core::mutation_manager) fn key_field_pairs<'a>(
    schema_name: &str,
    schema: &'a Schema,
    mutation: &Mutation,
    key_value: &'a KeyValue,
) -> Result<KeyFieldPairs<'a>, SchemaError> {
    // Erasures remove tips or bodies and never materialize key-field atoms.
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

pub(in crate::fold_db_core::mutation_manager) fn validated_materialized_key_field(
    schema_name: &str,
    schema: &Schema,
    mutation: &Mutation,
    field_name: &str,
    coordinate: &str,
    profile: MoleculeRangeProfile,
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
