//! Exact logical membership for schemas that share physical field molecules.

use crate::db_operations::DbOperations;
use crate::schema::types::field::fetch_atoms_with_key_metadata_async_with_prefix;
use crate::schema::types::key_value::scalar_key_coordinate;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Schema};
use crate::schema::SchemaError;
use crate::storage::disk_pack::logical_paths::field::FieldScope;
use serde_json::Value;
use std::sync::Arc;

/// A present declared HashRange hash spine must prove both coordinates.
/// An absent spine permits the existing projection fallback; the caller still
/// establishes field presence. Hash and Single retain their current rules.
pub(crate) async fn logical_key_membership(
    db_ops: &Arc<DbOperations>,
    schema: &Schema,
    keys: &[KeyValue],
    storage_prefix: Option<&str>,
) -> Result<Vec<bool>, SchemaError> {
    let mut matches = vec![true; keys.len()];
    if schema.schema_type != DeclarativeSchemaType::HashRange || keys.is_empty() {
        return Ok(matches);
    }
    let Some(layout) = schema.key.as_ref() else {
        return Ok(matches);
    };
    let Some(hash_name) = layout.hash_field.as_deref() else {
        return Ok(matches);
    };
    if !schema.runtime_fields.contains_key(hash_name) {
        return Ok(matches);
    }
    let hash_values = field_values(db_ops, schema, hash_name, keys, storage_prefix).await?;
    let mut range_keys = Vec::new();
    let mut range_indices = Vec::new();
    for (index, (key, value)) in keys.iter().zip(hash_values).enumerate() {
        let Some(value) = value else {
            continue;
        };
        matches[index] = key.hash_matches_scalar(&value);
        if !matches[index] {
            continue;
        }
        if layout.range_field.as_deref() == Some(hash_name) {
            matches[index] = scalar_key_coordinate(&value).as_deref() == key.range.as_deref();
        } else {
            range_keys.push(key.clone());
            range_indices.push(index);
        }
    }
    if range_keys.is_empty() {
        return Ok(matches);
    }
    let Some(range) = layout
        .range_field
        .as_ref()
        .filter(|name| schema.runtime_fields.contains_key(*name))
    else {
        for index in range_indices {
            matches[index] = false;
        }
        return Ok(matches);
    };
    let range_values = field_values(db_ops, schema, range, &range_keys, storage_prefix).await?;
    for ((index, key), value) in range_indices.into_iter().zip(range_keys).zip(range_values) {
        matches[index] = value
            .is_some_and(|value| scalar_key_coordinate(&value).as_deref() == key.range.as_deref());
    }
    Ok(matches)
}

/// Select each tip once under its actual field binding. Current reads reuse
/// the shared field reader and admit fetched values; private writes retain
/// their existing provenance guard. Missing tips stay absent; bodies are required.
async fn field_values(
    db_ops: &Arc<DbOperations>,
    schema: &Schema,
    field_name: &str,
    keys: &[KeyValue],
    storage_prefix: Option<&str>,
) -> Result<Vec<Option<Value>>, SchemaError> {
    let field = schema.runtime_fields.get(field_name).ok_or_else(|| {
        SchemaError::InvalidField(format!(
            "logical membership field '{field_name}' is missing"
        ))
    })?;
    let Some(molecule) = field.common().molecule_uuid() else {
        return Ok(vec![None; keys.len()]);
    };
    let binding = FieldScope::new(schema, field_name)?;
    binding
        .run(async {
            db_ops.atoms().load_molecule_key_bundle(molecule).await?;
            let records = db_ops
                .atoms()
                .load_query_tip_records(storage_prefix, molecule, keys)
                .await?;
            if records.len() != keys.len() {
                return Err(SchemaError::StorageRead {
                    detail: "logical membership tip count differs".into(),
                });
            }
            let selected = keys.iter().zip(records).filter_map(|(key, record)| {
                let (_, record) = record?;
                if record.meta.as_ref().is_some_and(|meta| meta.tombstoned) {
                    return None;
                }
                Some((
                    key.clone(),
                    record.entry.atom_uuid,
                    record.meta,
                    Some(record.entry.writer_pubkey),
                ))
            });
            let values = fetch_atoms_with_key_metadata_async_with_prefix(
                db_ops,
                selected,
                storage_prefix,
                Some(molecule),
            )
            .await?;
            Ok(keys
                .iter()
                .map(|key| values.get(key).map(|field| field.value.clone()))
                .collect())
        })
        .await
}
