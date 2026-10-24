//! Exact tip ages for a bounded page of operator-selected retention keys.

use super::*;

pub(crate) async fn retention_tip_clocks(
    db_ops: &Arc<DbOperations>,
    schemas: &Arc<SchemaCore>,
    name: &str,
    keys: &[KeyValue],
) -> Result<Vec<(KeyValue, u64)>, SchemaError> {
    let loaded = targets::load_targets(db_ops, schemas, name, keys, None).await?;
    let mut clocks: HashMap<usize, u64> = HashMap::new();
    for target in loaded.targets {
        let clock = target.record.entry.written_at;
        if clock == 0 {
            return Err(SchemaError::InvalidData(
                "retention backfill found a tip without an original write time".into(),
            ));
        }
        clocks
            .entry(target.key_index)
            .and_modify(|old| *old = (*old).max(clock))
            .or_insert(clock);
    }
    Ok(loaded
        .keys
        .into_iter()
        .enumerate()
        .filter_map(|(index, key)| clocks.get(&index).map(|clock| (key, clock / 1_000_000_000)))
        .collect())
}
