//! Declare FoldDB schemas from signed-history schema entries.

use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::{Schema, SchemaError};
use mutation_log::VerifiedHistory;

use crate::validate::validate_schema;

/// Install every valid schema declaration carried by `history`.
///
/// A missing declaration is not invented here. Invalid schema definitions are
/// validated in memory, logged, and skipped so a bad declaration cannot block
/// materialization, while storage and backend errors propagate so retry stays
/// scheduled.
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
    let schema: Schema = match serde_json::from_slice(definition) {
        Ok(schema) => schema,
        Err(error) => {
            tracing::warn!(
                namespace = %namespace,
                %error,
                "skipping invalid schema definition JSON in signed history"
            );
            return Ok(());
        }
    };
    if schema.name != namespace {
        tracing::warn!(
            namespace = %namespace,
            schema_name = %schema.name,
            "skipping schema entry whose definition name differs from its namespace"
        );
        return Ok(());
    }
    let existing = store
        .schema_manager()
        .get_schema_metadata(namespace)
        .map_err(|error| format!("read schema {namespace}: {error}"))?;
    if let Err(reason) = validate_schema(&schema, existing.as_ref()) {
        tracing::warn!(
            namespace = %namespace,
            %reason,
            "skipping invalid or incompatible schema definition in signed history"
        );
        return Ok(());
    }
    if let Some(existing) = &existing {
        if already_installed(existing, &schema) {
            return Ok(());
        }
    }
    match store.schema_manager().load_schema_internal(schema).await {
        Ok(()) => Ok(()),
        Err(error) if is_incompatible_schema_error(&error) => {
            tracing::warn!(
                namespace = %namespace,
                %error,
                "skipping incompatible schema update in signed history"
            );
            Ok(())
        }
        Err(error) => Err(format!("declare schema {namespace}: {error}")),
    }
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

fn is_incompatible_schema_error(error: &SchemaError) -> bool {
    match error {
        SchemaError::InvalidField(_) => true,
        SchemaError::InvalidData(msg) => {
            msg.starts_with("explicit primary/alternate key declarations are immutable")
                || msg.starts_with("changing the file requirement needs a new schema identity")
                || (msg.starts_with("existing field '") && msg.contains("cannot "))
        }
        _ => false,
    }
}
