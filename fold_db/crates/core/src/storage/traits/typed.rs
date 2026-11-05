//! `TypedKvStore`: JSON serialize/deserialize helper over any `KvStore`.

use serde::de::DeserializeOwned;
use std::sync::Arc;

use super::*;

/// Adapter that wraps a KvStore and provides high-level typed storage
/// operations
///
/// This provides a convenient API on top of KvStore for storing/retrieving
/// typed Rust structs (serialized as JSON or bincode)
pub struct TypedKvStore<S: KvStore + ?Sized> {
    inner: Arc<S>,
}

impl<S: KvStore + ?Sized> TypedKvStore<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self { inner: store }
    }

    /// Get a reference to the underlying KvStore
    pub fn inner(&self) -> &Arc<S> {
        &self.inner
    }
}

impl<S: KvStore + ?Sized + 'static> TypedKvStore<S> {
    /// Decode raw `(key, value)` rows into `(String, T)` items.
    /// Shared by all scan helpers so we don't maintain four copy-pasted loops.
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

    /// Get a typed item
    pub async fn get_item<T: DeserializeOwned + Send + Sync>(
        &self,
        key: &str,
    ) -> StorageResult<Option<T>> {
        let raw = self.inner.get(key.as_bytes()).await?;
        raw.map(|bytes| {
            serde_json::from_slice(&bytes)
                .map_err(|error| StorageError::SerializationError(error.to_string()))
        })
        .transpose()
    }

    /// Get multiple typed items, preserving input order.
    ///
    /// Each supplied storage key is an exact identity.
    pub async fn get_items<T: DeserializeOwned + Send + Sync>(
        &self,
        keys: &[String],
    ) -> StorageResult<Vec<Option<T>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let primary = keys.iter().map(|key| key.as_bytes().to_vec()).collect();
        let raw = self.inner.get_many(primary).await?;
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

    /// List all keys with a given prefix
    pub async fn list_keys_with_prefix(&self, prefix: &str) -> StorageResult<Vec<String>> {
        let keys = self.inner.scan_prefix_keys(prefix.as_bytes()).await?;
        Ok(keys
            .into_iter()
            .map(|k| String::from_utf8_lossy(&k).into_owned())
            .collect())
    }

    /// Get all items with a given prefix
    pub async fn scan_items_with_prefix<T: DeserializeOwned + Send + Sync>(
        &self,
        prefix: &str,
    ) -> StorageResult<Vec<(String, T)>> {
        let rows = self.inner.scan_prefix(prefix.as_bytes()).await?;
        Self::decode_rows(rows)
    }

    /// Get at most `limit` items with a given prefix, in ascending key order —
    /// the bounded, paginated counterpart to [`Self::scan_items_with_prefix`].
    /// Backs a paginated list query so the read no longer materializes the whole
    /// prefix; see [`KvStore::scan_prefix_paged`] for the boundedness contract.
    pub async fn scan_items_with_prefix_paged<T: DeserializeOwned + Send + Sync>(
        &self,
        prefix: &str,
        limit: usize,
    ) -> StorageResult<Vec<(String, T)>> {
        let rows = self
            .inner
            .scan_prefix_paged(prefix.as_bytes(), limit)
            .await?;
        Self::decode_rows(rows)
    }

    /// Get at most `limit` items whose key falls in `start..end`, in ascending
    /// key order. See [`KvStore::scan_range_paged`] for the boundedness
    /// contract.
    pub async fn scan_items_in_range_paged<T: DeserializeOwned + Send + Sync>(
        &self,
        start: &str,
        end: &str,
        limit: usize,
    ) -> StorageResult<Vec<(String, T)>> {
        Self::decode_rows(
            self.inner
                .scan_range_paged(start.as_bytes(), end.as_bytes(), limit)
                .await?,
        )
    }
}
