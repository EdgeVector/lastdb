//! Which keys of a batch have no trace in a schema: a read, no change.

use super::bulk::collect_purge_trace;
use super::*;

/// The keys of `keys` that no field of `schema_name` has a record for, in the
/// order given and without repeats. A TTL purge uses it to drop the keys that
/// a client or a peer already removed, and then retries the rest of its batch.
pub(in crate::fold_db_core) async fn absent_purge_targets(
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
    refresh_runtime_field_molecules_for_purge_keys(db_ops, &mut schema, &keys).await?;
    let trace = collect_purge_trace(db_ops, &schema, &keys).await?;
    Ok(keys
        .into_iter()
        .enumerate()
        .filter(|(index, _)| !trace.traced.contains(index))
        .map(|(_, key)| key)
        .collect())
}
