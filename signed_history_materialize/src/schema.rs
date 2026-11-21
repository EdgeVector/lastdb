//! Declare Hash schemas that materialize can write into.

use std::collections::BTreeSet;

use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::Schema;
use serde_json::json;

use crate::codec::STRING_FIELD;

/// Declare a Hash schema `name` with field `v: String`, when it is missing.
pub async fn ensure_hash_schema(store: &FoldDB, name: &str) -> Result<(), String> {
    let fields = BTreeSet::from([STRING_FIELD.to_owned()]);
    ensure_string_fields_schema(store, name, &fields).await
}

/// Declare a Hash schema `name` with the given String fields, when missing.
///
/// Phase 1 signed history has no schema entries. Receivers infer a Hash layout
/// from the JSON object keys of LWW winners before the first write.
pub async fn ensure_string_fields_schema(
    store: &FoldDB,
    name: &str,
    fields: &BTreeSet<String>,
) -> Result<(), String> {
    if fields.is_empty() {
        return Err(format!("schema {name}: no fields to declare"));
    }
    let manager = store.schema_manager();
    if manager
        .get_schema_metadata(name)
        .map_err(|e| e.to_string())?
        .is_some()
    {
        return Ok(());
    }
    let field_list: Vec<&str> = fields.iter().map(String::as_str).collect();
    let field_types =
        serde_json::Map::from_iter(fields.iter().map(|field| (field.clone(), json!("String"))));
    let schema: Schema = serde_json::from_value(json!({
        "name": name,
        "schema_type": "Hash",
        "record_keys": { "primary": { "kind": "hash" }, "alternates": {} },
        "fields": field_list,
        "field_types": field_types,
    }))
    .map_err(|e| format!("schema {name}: {e}"))?;
    manager
        .load_schema_internal(schema)
        .await
        .map_err(|e| format!("declare schema {name}: {e}"))
}
