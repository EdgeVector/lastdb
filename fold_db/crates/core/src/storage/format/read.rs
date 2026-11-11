//! Read capabilities for providers. Views do not expose write operations.

use crate::storage::disk_pack::PackKvStore;
use crate::storage::error::{StorageError, StorageResult};
use crate::storage::traits::YieldedScan;
use serde::de::DeserializeOwned;

#[derive(Clone)]
pub(crate) struct ReadStore {
    store: PackKvStore,
}

impl ReadStore {
    pub(in crate::storage::format) fn new(store: PackKvStore) -> Self {
        Self { store }
    }

    fn decode_rows<T: DeserializeOwned>(
        results: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> StorageResult<Vec<(String, T)>> {
        let mut items = Vec::with_capacity(results.len());
        for (key_bytes, value_bytes) in results {
            let key = String::from_utf8_lossy(&key_bytes).into_owned();
            let value = serde_json::from_slice(&value_bytes).map_err(|e| {
                StorageError::SerializationError(format!("Failed to deserialize {key}: {e}"))
            })?;
            items.push((key, value));
        }
        Ok(items)
    }

    pub(crate) async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.store.get(key).await
    }

    pub(crate) async fn get_many(&self, keys: Vec<Vec<u8>>) -> StorageResult<Vec<Option<Vec<u8>>>> {
        self.store.get_many(keys).await
    }

    pub(crate) async fn scan_prefix(
        &self,
        prefix: &[u8],
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.store.scan_prefix(prefix).await
    }

    pub(crate) async fn scan_range_yielding(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        future_reads: usize,
    ) -> StorageResult<YieldedScan> {
        self.store
            .scan_range_yielding(start, end, limit, future_reads)
            .await
    }

    pub(crate) async fn scan_range_paged(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.store.scan_range_paged(start, end, limit).await
    }

    pub(crate) async fn get_item<T: DeserializeOwned + Send + Sync>(
        &self,
        key: &str,
    ) -> StorageResult<Option<T>> {
        let raw = self.store.get(key.as_bytes()).await?;
        raw.map(|bytes| {
            serde_json::from_slice(&bytes)
                .map_err(|error| StorageError::SerializationError(error.to_string()))
        })
        .transpose()
    }

    pub(crate) async fn get_items<T: DeserializeOwned + Send + Sync>(
        &self,
        keys: &[String],
    ) -> StorageResult<Vec<Option<T>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let primary = keys.iter().map(|key| key.as_bytes().to_vec()).collect();
        let raw = self.store.get_many(primary).await?;
        let mut out = Vec::with_capacity(keys.len());
        for (key, bytes) in keys.iter().zip(raw) {
            match bytes {
                Some(bytes) => {
                    let item = serde_json::from_slice(&bytes).map_err(|e| {
                        StorageError::SerializationError(format!(
                            "Failed to deserialize {key}: {e}"
                        ))
                    })?;
                    out.push(Some(item));
                }
                None => out.push(None),
            }
        }
        Ok(out)
    }

    pub(crate) async fn list_keys_with_prefix(&self, prefix: &str) -> StorageResult<Vec<String>> {
        let rows = self.store.scan_prefix(prefix.as_bytes()).await?;
        Ok(rows
            .into_iter()
            .map(|(k, _)| String::from_utf8_lossy(&k).into_owned())
            .collect())
    }

    pub(crate) async fn scan_items_with_prefix<T: DeserializeOwned + Send + Sync>(
        &self,
        prefix: &str,
    ) -> StorageResult<Vec<(String, T)>> {
        let rows = self.store.scan_prefix(prefix.as_bytes()).await?;
        Self::decode_rows(rows)
    }

    pub(crate) async fn scan_items_in_range_paged<T: DeserializeOwned + Send + Sync>(
        &self,
        start: &str,
        end: &str,
        limit: usize,
    ) -> StorageResult<Vec<(String, T)>> {
        Self::decode_rows(
            self.store
                .scan_range_paged(start.as_bytes(), end.as_bytes(), limit)
                .await?,
        )
    }
}
