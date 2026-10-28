//! Namespace wrapper capability forwarding.
use super::*;
use async_trait::async_trait;

#[async_trait]
impl NamespacedStore for EncryptingNamespacedStore {
    fn disk_pack_runtime(&self) -> Option<Arc<crate::storage::disk_pack::PackRuntime>> {
        self.inner.disk_pack_runtime()
    }

    async fn restore_durability_barrier(&self) -> StorageResult<()> {
        self.inner.restore_durability_barrier().await
    }

    async fn open_namespace(&self, name: &str) -> StorageResult<Arc<dyn KvStore>> {
        let inner_store = self.inner.open_namespace(name).await?;

        if self.should_encrypt(name) {
            Ok(Arc::new(EncryptingKvStore::new(
                inner_store,
                self.crypto.clone(),
            )))
        } else {
            // Other records retain their own content/key protection without a storage envelope.
            Ok(inner_store)
        }
    }

    async fn list_namespaces(&self) -> StorageResult<Vec<String>> {
        self.inner.list_namespaces().await
    }

    async fn delete_namespace(&self, name: &str) -> StorageResult<bool> {
        self.inner.delete_namespace(name).await
    }
}
