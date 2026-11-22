//! Keyed reads of materialized records.

use std::collections::HashMap;

use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::{KeyValue, Query};
use serde_json::Value;

use crate::apply::Stored;
use crate::key::key_value;

/// Field map of the record at `record_key`, or `None` when absent.
pub async fn read_field_map(
    store: &FoldDB,
    namespace: &str,
    record_key: &str,
) -> Result<Option<HashMap<String, Value>>, String> {
    Ok(read_stored(store, namespace, record_key)
        .await?
        .map(|stored| stored.fields))
}

/// Live revision at `record_key`, or `None` when absent.
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
    let key = key_value(store, namespace, record_key)?;
    read_at(store, namespace, &key).await
}

pub(crate) async fn read_at(
    store: &FoldDB,
    namespace: &str,
    key: &KeyValue,
) -> Result<Option<Stored>, String> {
    let hash = key.hash.clone().unwrap_or_default();
    let filter = match &key.range {
        Some(range) => HashRangeFilter::HashRangeKey {
            hash,
            range: range.clone(),
        },
        None => HashRangeFilter::HashKey(hash),
    };
    let query = Query::new_with_filter(namespace.to_owned(), Some(filter));
    let records = store
        .query_executor()
        .query(query)
        .await
        .map_err(|e| format!("read {namespace}/{}: {e}", key.to_storage_key()))?;
    let mut rows = records.into_values();
    let found = rows.next().map(|record| Stored {
        revision: record.revision,
        created_by: record.created_by,
        updated_by: record.updated_by,
        fields: record.fields,
    });
    if rows.next().is_some() {
        return Err(format!(
            "{namespace}/{} names more than one record",
            key.to_storage_key()
        ));
    }
    Ok(found)
}
