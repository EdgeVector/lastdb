//! Exact logical absence for recovery of a refused erasure batch.

use super::bulk::collect_purge_trace;
use super::*;

/// A foreign mapped layout is absent in the requested schema even when its
/// physical slots still exist. TTL drops these and physically missing keys
/// before it retries the other targets. All reads share one captured state.
pub(in crate::fold_db_core) async fn absent_purge_targets(
    scope: PurgeScope<'_>,
    keys: &[KeyValue],
) -> Result<Vec<KeyValue>, SchemaError> {
    let read = scope.db_ops.read_scope().await?;
    read.run_result(absent_purge_targets_captured(scope, keys))
        .await
}

async fn absent_purge_targets_captured(
    scope: PurgeScope<'_>,
    keys: &[KeyValue],
) -> Result<Vec<KeyValue>, SchemaError> {
    let PurgeScope { db_ops, .. } = scope;
    let mut seen = HashSet::new();
    let keys: Vec<KeyValue> = keys
        .iter()
        .filter(|key| seen.insert(describe_key(key)))
        .cloned()
        .collect();
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let mut schema = scope.schema()?;
    let membership = crate::db_operations::logical_membership::logical_key_membership(
        db_ops,
        &schema,
        &keys,
        scope.storage_prefix,
    )
    .await?;
    refresh_runtime_field_molecules_for_purge_keys(db_ops, &mut schema, &keys).await?;
    let trace = collect_purge_trace(db_ops, &schema, &keys).await?;
    Ok(keys
        .into_iter()
        .enumerate()
        .filter(|(index, _)| !membership[*index] || !trace.traced.contains(index))
        .map(|(_, key)| key)
        .collect())
}
