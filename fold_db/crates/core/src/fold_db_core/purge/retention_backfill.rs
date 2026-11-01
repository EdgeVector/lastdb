//! Record ages for a bounded page of operator-selected retention keys.

use super::*;

/// The write time of each live record, in unix seconds: the newest stamp of
/// its fields and its metadata. One batch read by key. A key without a live
/// record has no age.
pub(crate) async fn retention_tip_clocks(
    db_ops: &Arc<DbOperations>,
    schemas: &Arc<SchemaCore>,
    name: &str,
    keys: &[KeyValue],
) -> Result<Vec<(KeyValue, u64)>, SchemaError> {
    let schema = schemas.require_schema_metadata(name)?;
    let scope = crate::db_operations::record_store::record_scope(&schema);
    let record_store = db_ops.records();
    let record_keys = keys
        .iter()
        .map(|key| record_store.record_key(&schema, key, scope.as_deref()))
        .collect::<Result<Vec<_>, _>>()?;
    let records = record_store.read_records(&record_keys).await?;
    Ok(keys
        .iter()
        .zip(records)
        .filter_map(|(key, record)| {
            let record = record?;
            if !record.is_live() {
                return None;
            }
            Some((key.clone(), record.written_at()? / 1_000_000_000))
        })
        .collect())
}
