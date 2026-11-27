//! Pure in-memory schema and record validation before FoldDB writes.
//!
//! Validating schema declarations and record payloads in memory before calling
//! FoldDB's write path separates malformed signed-history entries from genuine
//! storage/backend failures (`From<StorageError> for SchemaError` maps most I/O
//! and backend errors into `SchemaError::InvalidData`). Storage errors therefore
//! always propagate so `materialize_retry` stays set.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Deref;

use fold_db::record_replacement::{RecordExpectation, RecordReplacement, RecordTarget};
use fold_db::record_value::{RecordStamp, StoredRecord, MAX_RECORD_BYTES};
use fold_db::schema::require_logical_layout;
use fold_db::schema::types::Schema;
use serde_json::Value;

/// Validate a signed `schema` declaration intrinsically in memory, independent
/// of any locally installed schema state.
pub(crate) fn validate_intrinsic_schema(schema: &Schema) -> Result<(), String> {
    if schema.record_keys.is_none() {
        return Err("this home requires explicit record_keys".into());
    }
    schema.validate_owned_schema()?;
    require_logical_layout(&schema.schema_type).map_err(|e| e.to_string())?;
    schema.validate().map_err(|e| e.to_string())
}

/// True when `next` can be applied as an in-place additive update over `existing`
/// in FoldDB without dropping and recreating the schema first.
pub(crate) fn is_compatible_schema_update(existing: &Schema, next: &Schema) -> bool {
    if existing.record_keys != next.record_keys
        || existing.schema_type != next.schema_type
        || existing.file_required != next.file_required
    {
        return false;
    }
    let next_fields: HashSet<&str> = next.fields.iter().flatten().map(String::as_str).collect();
    for field in existing.fields.iter().flatten() {
        if !next_fields.contains(field.as_str())
            || existing.get_field_type(field) != next.get_field_type(field)
        {
            return false;
        }
    }
    existing.field_types.iter().all(|(field, existing_type)| {
        (next_fields.contains(field.as_str()) || next.field_types.contains_key(field))
            && next.get_field_type(field) == existing_type
    })
}

/// Validate a live record winner's target keys, fields, field types, file
/// requirement, and 64 KiB encoded record size purely in memory.
pub(crate) fn validate_record(
    schema: &Schema,
    target: &RecordTarget,
    fields: &HashMap<String, Value>,
    creator: &str,
    author: &str,
) -> Result<(), String> {
    require_logical_layout(&schema.schema_type).map_err(|e| e.to_string())?;
    if schema.file_required {
        return Err("this record requires a file".into());
    }
    let record_keys = schema.require_record_keys().map_err(|e| e.to_string())?;
    target.validate(record_keys)?;
    let mut ordered = BTreeMap::new();
    for (field, value) in fields {
        <Schema as Deref>::Target::validate_field_name(field)?;
        if !schema.fields.iter().flatten().any(|f| f == field) {
            return Err(format!(
                "field '{field}' is not declared in schema '{}'",
                schema.name
            ));
        }
        schema
            .get_field_type(field)
            .validate(value)
            .map_err(|err| format!("type error in field '{field}': {err}"))?;
        ordered.insert(field.clone(), value.clone());
    }
    validate_record_size(target, ordered, creator, author)
}

fn validate_record_size(
    target: &RecordTarget,
    fields: BTreeMap<String, Value>,
    creator: &str,
    author: &str,
) -> Result<(), String> {
    // Use maximum-width u64 stamp fields (20 digits each) and a 36-byte UUID so
    // the size check is a deterministic upper bound across all receivers,
    // independent of each node's local clock or mutation sequence counter.
    let stamp: RecordStamp = serde_json::from_str(
        r#"{"authored_at_ms":18446744073709551615,"position":{"source_stream":"ffffffff-ffff-4fff-8fff-ffffffffffff","sequence":18446744073709551615}}"#,
    )
    .map_err(|e| e.to_string())?;
    let replacement = RecordReplacement {
        target: target.clone(),
        expected: RecordExpectation::Absent,
        fields,
        metadata: None,
        file: None,
        inline_file: None,
    };
    let mut stored = StoredRecord::replacement(replacement, stamp, None, true);
    stored.created_by = Some(creator.to_owned());
    stored.updated_by = Some(author.to_owned());
    let encoded = stored.encode_uncapped().map_err(|e| e.to_string())?;
    if encoded.len() > MAX_RECORD_BYTES {
        return Err(format!(
            "record size {} exceeds {MAX_RECORD_BYTES} byte cap",
            encoded.len()
        ));
    }
    Ok(())
}
