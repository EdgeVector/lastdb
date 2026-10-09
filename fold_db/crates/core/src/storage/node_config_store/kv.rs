use super::{block_on_storage, NodeConfigStore};
use crate::storage::error::StorageError;

impl NodeConfigStore {
    // --- Generic key-value ---

    pub fn get(&self, key: &str) -> Result<Option<String>, StorageError> {
        let store = self.store.clone();
        let key = key.as_bytes().to_vec();
        block_on_storage(async move {
            Ok(store
                .get(&key)
                .await?
                .map(|v| String::from_utf8_lossy(&v).into_owned()))
        })
    }

    pub fn set(&self, key: &str, value: &str) -> Result<(), StorageError> {
        let store = self.store.clone();
        let key = key.as_bytes().to_vec();
        let value = value.as_bytes().to_vec();
        block_on_storage(async move {
            store.put(&key, value).await?;
            store.flush().await
        })
    }

    pub fn delete(&self, key: &str) -> Result<(), StorageError> {
        let store = self.store.clone();
        let key = key.as_bytes().to_vec();
        block_on_storage(async move {
            store.delete(&key).await?;
            store.flush().await
        })
    }

    pub(super) fn set_many(&self, items: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), StorageError> {
        let store = self.store.clone();
        block_on_storage(async move {
            store.batch_put(items).await?;
            store.flush().await
        })
    }

    pub fn is_empty(&self) -> bool {
        // On lock contention / IO error, report NOT empty: a transiently
        // locked DB must never read as a fresh node, or onboarding/bootstrap
        // could re-run over real data.
        let store = self.store.clone();
        block_on_storage(async move { store.scan_prefix_keys(b"").await })
            .is_ok_and(|keys| keys.is_empty())
    }
}
