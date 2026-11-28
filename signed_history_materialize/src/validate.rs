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
use fold_db::schema::types::{Schema, SchemaError};
use serde_json::Value;

const AUTHOR_HEX_64: &str = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

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
    file: Option<&fold_db::record_value::FileMarker>,
    inline_file: Option<&fold_db::record_value::FileMarker>,
) -> Result<(), String> {
    require_logical_layout(&schema.schema_type).map_err(|e| e.to_string())?;
    if schema.file_required && file.is_none() {
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
    check_ordered_record_size(target, ordered, file.cloned(), inline_file.cloned())
        .map_err(|err| err.to_string())
}

/// Enforce the deterministic shared-database 64 KiB `StoredRecord` cap using
/// maximum-width `u64::MAX` stamp fields and 64-hex authorship metadata.
///
/// Shared write, proposal, incorporation, and materialization paths all run
/// this identical bound so an accepted shared write can never exceed the cap on
/// a receiver with a larger local sequence counter.
pub fn check_shared_record_size(
    target: &RecordTarget,
    fields: &HashMap<String, Value>,
) -> Result<(), SchemaError> {
    check_shared_record_size_with_attachments(target, fields, None, None)
}

/// Like [`check_shared_record_size`], including attachment markers in the bound.
pub fn check_shared_record_size_with_attachments(
    target: &RecordTarget,
    fields: &HashMap<String, Value>,
    file: Option<fold_db::record_value::FileMarker>,
    inline_file: Option<fold_db::record_value::FileMarker>,
) -> Result<(), SchemaError> {
    let ordered: BTreeMap<String, Value> =
        fields.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    check_ordered_record_size(target, ordered, file, inline_file)
}

fn check_ordered_record_size(
    target: &RecordTarget,
    fields: BTreeMap<String, Value>,
    file: Option<fold_db::record_value::FileMarker>,
    inline_file: Option<fold_db::record_value::FileMarker>,
) -> Result<(), SchemaError> {
    let stamp: RecordStamp = serde_json::from_str(
        r#"{"authored_at_ms":18446744073709551615,"position":{"source_stream":"ffffffff-ffff-4fff-8fff-ffffffffffff","sequence":18446744073709551615}}"#,
    )
    .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
    let replacement = RecordReplacement {
        target: target.clone(),
        expected: RecordExpectation::Absent,
        fields,
        metadata: None,
        file,
        inline_file,
    };
    let mut stored = StoredRecord::replacement(replacement, stamp, None, true);
    stored.created_by = Some(AUTHOR_HEX_64.to_owned());
    stored.updated_by = Some(AUTHOR_HEX_64.to_owned());
    let encoded = stored
        .encode_uncapped()
        .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
    if encoded.len() > MAX_RECORD_BYTES {
        return Err(SchemaError::RecordTooLarge {
            size: encoded.len(),
            limit: MAX_RECORD_BYTES,
        });
    }
    Ok(())
}
