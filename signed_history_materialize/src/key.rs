//! Map a history record key onto the FoldDB key of its schema.

use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::KeyValue;
use mutation_log::split_record_key;

/// The FoldDB key for `record_key` in `namespace`.
///
/// A hash schema takes the whole string as its hash key. A hash-range schema
/// splits it at the first range separator. An undeclared schema, or a ranged
/// schema with an unsplittable key, is an error.
pub fn key_value(store: &FoldDB, namespace: &str, record_key: &str) -> Result<KeyValue, String> {
    let schema = store
        .schema_manager()
        .get_schema_metadata(namespace)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("schema {namespace} is not declared"))?;
    let keys = schema.require_record_keys().map_err(|e| e.to_string())?;
    if keys.primary.range_type.is_none() {
        return Ok(KeyValue::new(Some(record_key.to_owned()), None));
    }
    let (hash, range) = split_record_key(record_key)
        .ok_or_else(|| format!("{namespace}/{record_key} has no range part for its ranged key"))?;
    Ok(KeyValue::new(Some(hash.to_owned()), Some(range.to_owned())))
}
