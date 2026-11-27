//! Declare FoldDB schemas from signed-history schema entries.

use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::Schema;
use mutation_log::VerifiedHistory;

use crate::validate::{is_compatible_schema_update, validate_intrinsic_schema};

/// Make `store` hold the valid schema declarations carried by `history`.
///
/// Validity is decided purely from the signed declaration, independent of local
/// FoldDB state. When a locally installed schema is incompatible with the
/// history winner (or when the history winner is invalid), the local schema is
/// dropped first so every node converges on the identical schema state, and all
/// FoldDB storage/operational errors propagate for retry.
pub async fn declare_history_schemas(
    store: &FoldDB,
    history: &VerifiedHistory,
) -> Result<(), String> {
    for (namespace, definition) in history.schemas() {
        declare_one(store, namespace, definition).await?;
    }
    Ok(())
}

async fn declare_one(store: &FoldDB, namespace: &str, definition: &[u8]) -> Result<(), String> {
    let existing = store
        .schema_manager()
        .get_schema_metadata(namespace)
        .map_err(|error| format!("read schema {namespace}: {error}"))?;
    let Some(schema) = parse_valid_schema(namespace, definition) else {
        if existing.is_some() {
            drop_installed(store, namespace).await?;
        }
        return Ok(());
    };
    if let Some(installed) = &existing {
        if already_installed(installed, &schema) {
            return Ok(());
        }
        if !is_compatible_schema_update(installed, &schema) {
            tracing::warn!(
                namespace = %namespace,
                "replacing locally installed schema incompatible with signed-history winner"
            );
            drop_installed(store, namespace).await?;
        }
    }
    store
        .schema_manager()
        .load_schema_internal(schema)
        .await
        .map_err(|error| format!("declare schema {namespace}: {error}"))
}

fn parse_valid_schema(namespace: &str, definition: &[u8]) -> Option<Schema> {
    let schema: Schema = match serde_json::from_slice(definition) {
        Ok(schema) => schema,
        Err(error) => {
            tracing::warn!(
                namespace = %namespace,
                %error,
                "skipping invalid schema definition JSON in signed history"
            );
            return None;
        }
    };
    if schema.name != namespace {
        tracing::warn!(
            namespace = %namespace,
            schema_name = %schema.name,
            "skipping schema entry whose definition name differs from its namespace"
        );
        return None;
    }
    if let Err(reason) = validate_intrinsic_schema(&schema) {
        tracing::warn!(
            namespace = %namespace,
            %reason,
            "skipping invalid schema definition in signed history"
        );
        return None;
    }
    Some(schema)
}

async fn drop_installed(store: &FoldDB, namespace: &str) -> Result<(), String> {
    store
        .schema_manager()
        .drop_schema(namespace)
        .await
        .map(|_| ())
        .map_err(|error| format!("drop schema {namespace}: {error}"))
}

fn already_installed(existing: &Schema, incoming: &Schema) -> bool {
    if existing == incoming {
        return true;
    }
    let mut normalized = incoming.clone();
    normalized.ensure_field_hashes();
    let _ = normalized.recompute_identity_hash_unless_newer();
    if existing.identity_is_newer_than_binary() {
        normalized.identity_hash.clone_from(&existing.identity_hash);
        normalized.identity_hash_algo_version = existing.identity_hash_algo_version;
    }
    *existing == normalized
}
