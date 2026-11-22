//! Declare FoldDB schemas from signed-history schema entries.

use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::Schema;
use mutation_log::VerifiedHistory;

/// Install every schema declaration carried by `history`.
///
/// A missing declaration is not invented here. Record apply fails closed when
/// a live winner names a namespace that was never declared in the log.
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
    let schema: Schema = serde_json::from_slice(definition)
        .map_err(|e| format!("schema {namespace}: definition is not valid JSON: {e}"))?;
    if schema.name != namespace {
        return Err(format!(
            "schema {namespace}: definition name is {}",
            schema.name
        ));
    }
    // Always load: FoldDB applies compatible updates (additive fields) when the
    // namespace is already installed. Skipping would leave receivers on the first
    // signed definition forever.
    store
        .schema_manager()
        .load_schema_internal(schema)
        .await
        .map_err(|e| format!("declare schema {namespace}: {e}"))
}
