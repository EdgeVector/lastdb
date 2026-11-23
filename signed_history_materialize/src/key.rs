//! Map a history record key onto the FoldDB key of its schema.

use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::KeyValue;
use mutation_log::split_record_key;

/// Canonicalize `(hash, range)` against `namespace`'s primary key when `namespace`
/// is declared in `store`, and join into a history `record_key`.
pub fn canonical_record_key(
    store: &FoldDB,
    namespace: &str,
    hash: &str,
    range: Option<&str>,
) -> Result<String, String> {
    let Some(schema) = store
        .schema_manager()
        .get_schema_metadata(namespace)
        .map_err(|e| e.to_string())?
    else {
        return mutation_log::join_record_key(hash, range)
            .ok_or_else(|| format!("{namespace}/{hash} cannot be encoded as a record key"));
    };
    let keys = schema.require_record_keys().map_err(|e| e.to_string())?;
    keys.primary.validate_coordinates(Some(hash), range)?;
    let canon_range = match range {
        Some(raw) => {
            let profile = keys.primary.range_profile()?;
            let encoded = profile.encode_value(raw)?;
            Some(profile.decode_value(&encoded)?)
        }
        None => None,
    };
    mutation_log::join_record_key(hash, canon_range.as_deref())
        .ok_or_else(|| format!("{namespace}/{hash} cannot be encoded as a record key"))
}

/// The FoldDB key for `record_key` in `namespace`.
///
/// A hash schema takes the whole string as its hash key. A hash-range schema
/// splits it at the first range separator and canonicalizes its range part.
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
    let profile = keys.primary.range_profile()?;
    let encoded = profile.encode_value(range)?;
    let canon_range = profile.decode_value(&encoded)?;
    Ok(KeyValue::new(Some(hash.to_owned()), Some(canon_range)))
}

/// Canonicalize an already-joined history `record_key` against `namespace` in `store`.
pub fn canonical_record_key_from_joined(
    store: &FoldDB,
    namespace: &str,
    record_key: &str,
) -> Result<String, String> {
    let kv = key_value(store, namespace, record_key)?;
    let hash = kv.hash.as_deref().unwrap_or_default();
    mutation_log::join_record_key(hash, kv.range.as_deref())
        .ok_or_else(|| format!("{namespace}/{record_key} cannot be encoded as a record key"))
}
