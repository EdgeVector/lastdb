//! Pure helpers for erase planning and key matching.

use crate::schema::types::key_value::KeyValue;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::Schema;
use crate::schema::SchemaError;

/// Same shape-check the mutation pipeline applies to Delete in
/// `mutation_manager::prepare_atoms_and_key_values`. Failing here means
/// the caller submitted a `Hash` schema purge with no hash, etc.
pub(crate) fn validate_purge_key_shape(
    schema: &Schema,
    schema_name: &str,
    key: &KeyValue,
) -> Result<(), SchemaError> {
    match &schema.schema_type {
        DeclarativeSchemaType::Hash if key.hash.is_none() => Err(SchemaError::InvalidData(
            format!("Hash schema '{schema_name}' purge has no hash key"),
        )),
        DeclarativeSchemaType::Range if key.range.is_none() => Err(SchemaError::InvalidData(
            format!("Range schema '{schema_name}' purge has no range key"),
        )),
        DeclarativeSchemaType::HashRange if key.hash.is_none() || key.range.is_none() => {
            Err(SchemaError::InvalidData(format!(
                "HashRange schema '{}' purge requires both hash and range keys, got hash={:?} range={:?}",
                schema_name, key.hash, key.range,
            )))
        }
        _ => Ok(()),
    }
}

/// Human-readable rendering of a KeyValue for log/error messages.
pub(crate) fn describe_key(kv: &KeyValue) -> String {
    match (&kv.hash, &kv.range) {
        (Some(h), Some(r)) => format!("hash='{h}' range='{r}'"),
        (Some(h), None) => format!("hash='{h}'"),
        (None, Some(r)) => format!("range='{r}'"),
        (None, None) => "<single>".to_string(),
    }
}
