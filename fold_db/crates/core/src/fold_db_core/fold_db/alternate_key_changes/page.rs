use super::*;
use std::collections::{BTreeSet, HashSet};

pub(super) async fn execute(
    db: &FoldDB,
    store: &dyn KvStore,
    mut change: Change,
    keys: &[KeyValue],
) -> Result<Change, SchemaError> {
    if change.phase != Phase::Backfill || keys.is_empty() || keys.len() > 64 {
        return Err(invalid(
            "backfill requires an active change and 1 to 64 explicit primary keys",
        ));
    }
    let mut seen = HashSet::new();
    let keys: Vec<_> = keys
        .iter()
        .filter(|key| seen.insert((*key).clone()))
        .cloned()
        .collect();
    if keys
        .iter()
        .any(|key| key.hash.is_none() || key.range.is_some())
    {
        return Err(invalid("backfill requires primary hash keys"));
    }
    let receipts: Vec<_> = keys
        .iter()
        .map(|key| {
            serde_json::to_string(key)
                .map(|encoded| format!("seen:{}:{encoded}", change.id).into_bytes())
                .map_err(|_| invalid("invalid primary key"))
        })
        .collect::<Result<_, _>>()?;
    let existing = store.get_many(receipts.clone()).await?;
    if existing.len() != keys.len() {
        return Err(invalid("incomplete backfill receipt read"));
    }
    if existing
        .iter()
        .flatten()
        .any(|receipt| receipt.as_slice() != [1])
    {
        return Err(invalid("invalid backfill receipt"));
    }
    let fresh: Vec<_> = keys
        .into_iter()
        .zip(receipts)
        .zip(existing)
        .filter_map(|((key, receipt), old)| old.is_none().then_some((key, receipt)))
        .collect();
    let keys: Vec<_> = fresh.iter().map(|(key, _)| key.clone()).collect();
    if keys.is_empty() {
        return Ok(change);
    }
    let names: BTreeSet<_> = change
        .source
        .runtime_fields
        .keys()
        .map(String::as_str)
        .collect();
    let fields = db
        .db_ops
        .atoms()
        .schema_fields_at_keys(&change.source, &keys, None, &names)
        .await?;
    for (key, values) in keys.iter().zip(&fields) {
        let actual = values
            .get(primary(&change.source)?)
            .and_then(crate::schema::types::key_value::scalar_key_coordinate);
        if actual.as_deref() != key.hash.as_deref() {
            return Err(invalid(
                "a backfill source key is absent or has different primary coordinates",
            ));
        }
        for name in values.keys() {
            *change.observed.entry(name.clone()).or_default() += 1;
        }
    }
    // New molecules remain unpublished until every source field's exact counter
    // agrees with the unique known-key pages. Primary atoms and clocks never move.
    let before = vec![BTreeMap::new(); keys.len()];
    db.db_ops
        .atoms()
        .stage_alternate_changes(&change.target, &keys, &before, None)
        .await?;
    store
        .batch_mutate(
            fresh
                .into_iter()
                .map(|(_, key)| KvMutation::put(key, vec![1]))
                .collect(),
        )
        .await?;
    change.processed += keys.len() as u64;
    if change.processed
        > change
            .expected
            .get(primary(&change.source)?)
            .copied()
            .unwrap_or_default()
    {
        return Err(invalid("backfill exceeds the frozen source key count"));
    }
    save(store, &change).await?;
    Ok(change)
}
