use super::error::StorageResult;
use super::traits::{KvMutation, KvStore, NamespacedStore};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// In-memory KvStore implementation for testing
#[derive(Clone)]
pub struct InMemoryKvStore {
    data: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl InMemoryKvStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for InMemoryKvStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl KvStore for InMemoryKvStore {
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        let data = self.data.read()?;
        Ok(data.get(key).cloned())
    }

    async fn put(&self, key: &[u8], value: Vec<u8>) -> StorageResult<()> {
        let mut data = self.data.write()?;
        data.insert(key.to_vec(), value);
        Ok(())
    }

    /// Compare and write under one write lock, so no put or delete can
    /// interleave.
    async fn compare_and_swap(
        &self,
        key: &[u8],
        expected: &[u8],
        new: Option<Vec<u8>>,
    ) -> StorageResult<bool> {
        let mut data = self.data.write()?;
        if data.get(key).map(Vec::as_slice) != Some(expected) {
            return Ok(false);
        }
        match new {
            Some(value) => {
                data.insert(key.to_vec(), value);
            }
            None => {
                data.remove(key);
            }
        }
        Ok(true)
    }

    async fn delete(&self, key: &[u8]) -> StorageResult<bool> {
        let mut data = self.data.write()?;
        Ok(data.remove(key).is_some())
    }

    async fn exists(&self, key: &[u8]) -> StorageResult<bool> {
        let data = self.data.read()?;
        Ok(data.contains_key(key))
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let data = self.data.read()?;

        let mut results: Vec<_> = data
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        results.sort_by(|a, b| a.0.cmp(&b.0));

        Ok(results)
    }

    async fn max_key_u64_after_marker(
        &self,
        prefix: &[u8],
        marker: &[u8],
    ) -> StorageResult<Option<u64>> {
        let data = self.data.read()?;
        Ok(data
            .keys()
            .filter(|key| key.starts_with(prefix))
            .filter_map(|key| super::traits::key_u64_after_last_marker(key, marker))
            .max())
    }

    async fn batch_put(&self, items: Vec<(Vec<u8>, Vec<u8>)>) -> StorageResult<()> {
        let mut data = self.data.write()?;

        for (key, value) in items {
            data.insert(key, value);
        }

        Ok(())
    }

    async fn batch_mutate(&self, mutations: Vec<KvMutation>) -> StorageResult<()> {
        let mut data = self.data.write()?;
        for mutation in mutations {
            match mutation {
                KvMutation::Put { key, value } => {
                    data.insert(key, value);
                }
                KvMutation::Delete { key } => {
                    data.remove(&key);
                }
            }
        }
        Ok(())
    }

    async fn batch_delete(&self, keys: Vec<Vec<u8>>) -> StorageResult<()> {
        let mut data = self.data.write()?;

        for key in keys {
            data.remove(&key);
        }

        Ok(())
    }

    async fn flush(&self) -> StorageResult<()> {
        // In-memory storage doesn't need flushing
        Ok(())
    }

    fn backend_name(&self) -> &'static str {
        "in-memory"
    }

    fn execution_model(&self) -> super::traits::ExecutionModel {
        // In-memory is sync but wrapped in async
        super::traits::ExecutionModel::SyncWrapped
    }

    fn flush_behavior(&self) -> super::traits::FlushBehavior {
        // In-memory doesn't need flushing
        super::traits::FlushBehavior::NoOp
    }
}

/// In-memory NamespacedStore for testing
#[derive(Clone)]
pub struct InMemoryNamespacedStore {
    namespaces: Arc<RwLock<HashMap<String, Arc<InMemoryKvStore>>>>,
}

impl InMemoryNamespacedStore {
    pub fn new() -> Self {
        Self {
            namespaces: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for InMemoryNamespacedStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl NamespacedStore for InMemoryNamespacedStore {
    async fn open_namespace(&self, name: &str) -> StorageResult<Arc<dyn KvStore>> {
        let mut namespaces = self.namespaces.write()?;

        let store = namespaces
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(InMemoryKvStore::new()))
            .clone();

        Ok(store as Arc<dyn KvStore>)
    }

    async fn list_namespaces(&self) -> StorageResult<Vec<String>> {
        let namespaces = self.namespaces.read()?;

        Ok(namespaces.keys().cloned().collect())
    }

    async fn delete_namespace(&self, name: &str) -> StorageResult<bool> {
        let mut namespaces = self.namespaces.write()?;

        Ok(namespaces.remove(name).is_some())
    }
}
