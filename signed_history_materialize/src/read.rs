//! Keyed reads of materialized records.

use std::collections::HashMap;

use fold_db::fold_db_core::FoldDB;
use fold_db::record_value::FileMarker;
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::{KeyValue, Query};
use serde_json::Value;

use crate::apply::Stored;
use crate::key::key_value;
use crate::value::Alternates;

/// What a store holds for a live record: its fields, where it sits under each
/// alternate key, and its file.
pub struct StoredState {
    /// The fields of the record.
    pub fields: HashMap<String, Value>,
    /// The record's address under each declared alternate key.
    pub alternates: Alternates,
    /// The marker of the record's file, when it has one.
    pub file: Option<FileMarker>,
}

/// The fields, alternate addresses and file of the record at `record_key`, or
/// `None` when it is absent. A writer of the history reads these back from the
/// store, so the history carries what the store holds.
pub async fn read_state(
    store: &FoldDB,
    namespace: &str,
    record_key: &str,
) -> Result<Option<StoredState>, String> {
    Ok(read_stored(store, namespace, record_key)
        .await?
        .map(|stored| StoredState {
            fields: stored.fields,
            alternates: stored.alternates,
            file: stored.file,
        }))
}

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
        alternates: record.alternates,
        file: record.metadata.and_then(|metadata| metadata.file),
    });
    if rows.next().is_some() {
        return Err(format!(
            "{namespace}/{} names more than one record",
            key.to_storage_key()
        ));
    }
    Ok(found)
}
