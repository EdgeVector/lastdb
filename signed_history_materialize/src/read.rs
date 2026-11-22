//! Keyed reads of materialized records.

use std::collections::HashMap;

use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::Query;
use serde_json::Value;

use crate::apply::Stored;

/// Field map of the record at hash key `record_key`, or `None` when absent.
pub async fn read_field_map(
    store: &FoldDB,
    namespace: &str,
    record_key: &str,
) -> Result<Option<HashMap<String, Value>>, String> {
    Ok(read_stored(store, namespace, record_key)
        .await?
        .map(|stored| stored.fields))
}

/// Live revision at hash key `record_key`, or `None` when absent.
pub async fn read_revision(
    store: &FoldDB,
    namespace: &str,
    record_key: &str,
) -> Result<Option<String>, String> {
    Ok(read_stored(store, namespace, record_key)
        .await?
        .map(|stored| stored.revision))
}

pub(crate) async fn read_stored(
    store: &FoldDB,
    namespace: &str,
    record_key: &str,
) -> Result<Option<Stored>, String> {
    let query = Query::new_with_filter(
        namespace.to_owned(),
        Some(HashRangeFilter::HashKey(record_key.to_owned())),
    );
    let records = store
        .query_executor()
        .query(query)
        .await
        .map_err(|e| format!("read {namespace}/{record_key}: {e}"))?;
    let mut rows = records.into_values();
    let found = rows.next().map(|record| Stored {
        revision: record.revision,
        fields: record.fields,
    });
    if rows.next().is_some() {
        return Err(format!(
            "{namespace}/{record_key} names more than one record"
        ));
    }
    Ok(found)
}
