//! Read capabilities for providers. Neither view exposes its raw handle.

use crate::storage::error::StorageResult;
use crate::storage::{KvStore, TypedKvStore};
use serde::de::DeserializeOwned;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct ReadStore {
    store: Arc<TypedKvStore<dyn KvStore>>,
}

impl ReadStore {
    pub(in crate::storage::format) fn new(store: Arc<TypedKvStore<dyn KvStore>>) -> Self {
        Self { store }
    }

    /// Stable cache-owner identity, not a storage capability.
    pub(crate) fn identity(&self) -> usize {
        Arc::as_ptr(&self.store) as usize
    }

    pub(crate) fn inner(&self) -> KvRead<'_> {
        KvRead {
            store: self.store.inner().as_ref(),
        }
    }

    pub(crate) async fn get_item<T: DeserializeOwned + Send + Sync>(
        &self,
        key: &str,
    ) -> StorageResult<Option<T>> {
        self.store.get_item(key).await
    }

    pub(crate) async fn get_items<T: DeserializeOwned + Send + Sync>(
        &self,
        keys: &[String],
    ) -> StorageResult<Vec<Option<T>>> {
        self.store.get_items(keys).await
    }

    pub(crate) async fn list_keys_with_prefix(&self, prefix: &str) -> StorageResult<Vec<String>> {
        self.store.list_keys_with_prefix(prefix).await
    }

    pub(crate) async fn scan_items_with_prefix<T: DeserializeOwned + Send + Sync>(
        &self,
        prefix: &str,
    ) -> StorageResult<Vec<(String, T)>> {
        self.store.scan_items_with_prefix(prefix).await
    }

    pub(crate) async fn scan_items_with_prefix_paged<T: DeserializeOwned + Send + Sync>(
        &self,
        prefix: &str,
        limit: usize,
    ) -> StorageResult<Vec<(String, T)>> {
        self.store.scan_items_with_prefix_paged(prefix, limit).await
    }

    pub(crate) async fn scan_items_in_range_paged<T: DeserializeOwned + Send + Sync>(
        &self,
        start: &str,
        end: &str,
        limit: usize,
    ) -> StorageResult<Vec<(String, T)>> {
        self.store
            .scan_items_in_range_paged(start, end, limit)
            .await
    }
}

pub(crate) struct KvRead<'a> {
    store: &'a dyn KvStore,
}

impl KvRead<'_> {
    pub(crate) async fn get(self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.store.get(key).await
    }

    pub(crate) async fn get_many(self, keys: Vec<Vec<u8>>) -> StorageResult<Vec<Option<Vec<u8>>>> {
        self.store.get_many(keys).await
    }

    pub(crate) async fn scan_range_paged(
        self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.store.scan_range_paged(start, end, limit).await
    }
}
