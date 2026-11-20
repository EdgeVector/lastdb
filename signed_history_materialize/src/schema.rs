//! Declare the one-field Hash schema that proofs materialize into.

use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::Schema;
use serde_json::json;

use crate::codec::STRING_FIELD;

/// Declare a Hash schema `name` with field `v: String`, when it is missing.
pub async fn ensure_hash_schema(store: &FoldDB, name: &str) -> Result<(), String> {
    let manager = store.schema_manager();
    if manager
        .get_schema_metadata(name)
        .map_err(|e| e.to_string())?
        .is_some()
    {
        return Ok(());
    }
    let schema: Schema = serde_json::from_value(json!({
        "name": name,
        "schema_type": "Hash",
        "record_keys": { "primary": { "kind": "hash" }, "alternates": {} },
        "fields": [STRING_FIELD],
        "field_types": { STRING_FIELD: "String" },
    }))
    .map_err(|e| format!("schema {name}: {e}"))?;
    manager
        .load_schema_internal(schema)
        .await
        .map_err(|e| format!("declare schema {name}: {e}"))
}
