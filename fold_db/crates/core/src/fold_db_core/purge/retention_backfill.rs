//! Exact tip ages for a bounded page of operator-selected retention keys.
//! A format 7 home ages a record by its own write time instead.

use super::*;
use lastdb_model::record_value::StoredRecord;

pub(crate) async fn retention_tip_clocks(
    db_ops: &Arc<DbOperations>,
    schemas: &Arc<SchemaCore>,
    name: &str,
    keys: &[KeyValue],
) -> Result<Vec<(KeyValue, u64)>, SchemaError> {
    if db_ops.atoms().record_home() {
        return retention_record_clocks(db_ops, schemas, name, keys).await;
    }
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

/// The write time of each live record, in unix seconds: the newest stamp of
/// its fields and its metadata. One batch read by key. A key without a live
/// record has no age.
async fn retention_record_clocks(
    db_ops: &Arc<DbOperations>,
    schemas: &Arc<SchemaCore>,
    name: &str,
    keys: &[KeyValue],
) -> Result<Vec<(KeyValue, u64)>, SchemaError> {
    let schema = schemas.require_schema_metadata(name)?;
    let scope = crate::db_operations::atom_store::record_scope(&schema);
    let atoms = db_ops.atoms();
    let record_keys = keys
        .iter()
        .map(|key| atoms.record_key(&schema, key, scope.as_deref()))
        .collect::<Result<Vec<_>, _>>()?;
    let records = atoms.read_records(&record_keys).await?;
    Ok(keys
        .iter()
        .zip(records)
        .filter_map(|(key, record)| {
            let written_at = record.filter(StoredRecord::is_live)?.written_at()?;
            Some((key.clone(), written_at / 1_000_000_000))
        })
        .collect())
}
