//! Normalize omitted declared key fields in the same private field batch.

use super::super::super::MutationManager;
use crate::schema::types::key_value::scalar_key_coordinate;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{FieldValueType, KeyValue, Mutation, MutationType, Schema};
use crate::schema::SchemaError;
use std::collections::HashMap;

impl MutationManager {
    /// Check omitted key fields and HashRange key conflicts before idempotency or
    /// any schema group can publish. Preparation repeats the same check while
    /// it adds omitted fields to its private mutation copy.
    pub(in crate::fold_db_core::mutation_manager) fn validate_key_field_payloads(
        &self,
        mutations: &[Mutation],
    ) -> Result<(), SchemaError> {
        let mut schemas = HashMap::new();
        for mutation in mutations {
            if !schemas.contains_key(&mutation.schema_name) {
                let mut schema = self
                    .schema_manager
                    .get_schema_metadata(&mutation.schema_name)?
                    .ok_or_else(|| {
                        SchemaError::InvalidData(format!(
                            "Schema '{}' not found",
                            mutation.schema_name
                        ))
                    })?;
                if schema.runtime_fields.is_empty() {
                    schema.populate_runtime_fields()?;
                }
                schemas.insert(mutation.schema_name.clone(), schema);
            }
            let schema = &schemas[&mutation.schema_name];
            let key_value =
                Self::resolve_mutation_key_value(&mutation.schema_name, schema, mutation)?;
            Self::validate_key_fields(&mutation.schema_name, schema, mutation, &key_value)?;
        }
        Ok(())
    }

    /// Make omitted declared key fields part of the same field batch as
    /// the caller's payload.
    ///
    /// A mutation can carry its coordinates only in `key_value`. Before this
    /// normalization, that shape advanced payload molecules but left the
    /// declared hash/range-field molecules sparse. Mutating the prepared copy
    /// here keeps the request and mutation-intent wire shapes unchanged while
    /// making atom preparation, changed-key restore, resident apply, Search,
    /// and durable persistence use one complete atomic field set.
    pub(super) fn materialize_key_fields(
        schema_name: &str,
        schema: &Schema,
        mutation: &mut Mutation,
        key_value: &KeyValue,
    ) -> Result<(), SchemaError> {
        for (role, field_name, coordinate) in
            key_field_pairs(schema_name, schema, mutation, key_value)?
                .into_iter()
                .flatten()
        {
            if let Some(value) = validated_materialized_key_field(
                schema_name,
                schema,
                mutation,
                field_name,
                coordinate,
                role,
                true,
            )? {
                mutation
                    .fields_and_values
                    .insert(field_name.to_string(), value);
            }
        }
        Ok(())
    }

    fn validate_key_fields(
        schema_name: &str,
        schema: &Schema,
        mutation: &Mutation,
        key_value: &KeyValue,
    ) -> Result<(), SchemaError> {
        for (role, field_name, coordinate) in
            key_field_pairs(schema_name, schema, mutation, key_value)?
                .into_iter()
                .flatten()
        {
            validated_materialized_key_field(
                schema_name,
                schema,
                mutation,
                field_name,
                coordinate,
                role,
                false,
            )?;
        }
        Ok(())
    }
}

/// Which coordinate a declared key field carries.
#[derive(Clone, Copy, PartialEq, Eq)]
enum KeyRole {
    Hash,
    Range,
}

/// HashRange mutations whose explicit range-field value differed from the
/// range coordinate and kept the payload value (process lifetime).
static HASHRANGE_KEY_FIELD_CONFLICTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Count one conflict and WARN on the 1st, 2nd, 4th, 8th, … so a writer that
/// conflicts on every row (LastGit CI status) cannot flood the log.
fn note_key_field_conflict(schema_name: &str, field_name: &str, payload: &str, coordinate: &str) {
    let seen = HASHRANGE_KEY_FIELD_CONFLICTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    if seen.is_power_of_two() {
        tracing::warn!(
            schema = schema_name,
            field = field_name,
            payload,
            coordinate,
            conflicts = seen,
            "HashRange range field value differs from key_value; keeping the payload value"
        );
    }
}

type KeyFieldPairs<'a> = [Option<(KeyRole, &'a str, &'a str)>; 2];

fn key_field_pairs<'a>(
    schema_name: &str,
    schema: &'a Schema,
    mutation: &Mutation,
    key_value: &'a KeyValue,
) -> Result<KeyFieldPairs<'a>, SchemaError> {
    let hash_only = matches!(schema.schema_type, DeclarativeSchemaType::Hash);
    if !hash_only && !matches!(schema.schema_type, DeclarativeSchemaType::HashRange) {
        return Ok([None, None]);
    }
    // Hash erasures do not create atoms. Keep their existing key-only shape.
    if hash_only
        && !matches!(
            mutation.mutation_type,
            MutationType::Create | MutationType::Update
        )
    {
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
    let hash = Some((KeyRole::Hash, hash_field, hash));
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
    Ok([hash, Some((KeyRole::Range, range_field, range))])
}

fn validated_materialized_key_field(
    schema_name: &str,
    schema: &Schema,
    mutation: &Mutation,
    field_name: &str,
    coordinate: &str,
    role: KeyRole,
    note_conflict: bool,
) -> Result<Option<serde_json::Value>, SchemaError> {
    // Explicit Hash values retain their existing meaning, including values
    // that differ from the caller's lookup key. Only an omitted field is filled.
    if matches!(schema.schema_type, DeclarativeSchemaType::Hash)
        && mutation.fields_and_values.contains_key(field_name)
    {
        return Ok(None);
    }
    if !schema.runtime_fields.contains_key(field_name) {
        return Err(SchemaError::InvalidData(format!(
            "Schema '{schema_name}' key field '{field_name}' is not a runtime field"
        )));
    }
    if let Some(explicit) = mutation.fields_and_values.get(field_name) {
        // A legacy tombstone is a control marker, not a caller-supplied key
        // coordinate. Preserve it so bounded tombstone drains and old homes
        // can still remove a key-field tip.
        if crate::atom::is_tombstone_value(explicit) {
            return Ok(None);
        }
        let explicit_coordinate = scalar_key_coordinate(explicit).ok_or_else(|| {
            SchemaError::InvalidData(format!(
                "Schema '{schema_name}' mutation {} key field '{field_name}' must be a scalar",
                mutation.uuid
            ))
        })?;
        if explicit_coordinate != coordinate {
            // A conflicting HASH field would store a row that partition reads
            // on its own hash no longer return, so it stays refused.
            if role == KeyRole::Hash {
                return Err(SchemaError::InvalidData(format!(
                    "Schema '{schema_name}' mutation {} key field '{field_name}' conflicts with key_value: payload={explicit_coordinate:?}, coordinate={coordinate:?}",
                    mutation.uuid
                )));
            }
            // A conflicting RANGE field keeps the caller's value, as before
            // key-field materialization. Tom's primary (2026-09-29) has
            // LastgitCiStatus with `range_field = event_id` (the
            // CiStatus/CrEvent interleave), while LastGit writes range =
            // `repo:oid:context` and a separate `event_id` lease value that
            // its CAS reads back. Refusing failed every CI status write on a
            // candidate build; overwriting with the coordinate would break the
            // lease CAS. The row is still keyed by `key_value`.
            if note_conflict {
                note_key_field_conflict(schema_name, field_name, &explicit_coordinate, coordinate);
            }
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
