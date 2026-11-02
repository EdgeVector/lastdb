//! Namespace and physical-store capability surface.
use super::*;
use crate::storage::error::StorageResult;
use async_trait::async_trait;
use std::sync::Arc;

/// Namespace storage trait
///
/// Provides logical separation of data into "namespaces" or collections
/// (native collections, or logical partitions in remote stores)
#[async_trait]
pub trait NamespacedStore: Send + Sync {
    /// The native disk runtime of the complete home that backs these namespaces.
    ///
    /// Every store has a runtime. A wrapper forwards the runtime of the store
    /// that it wraps.
    fn disk_pack_runtime(&self) -> Arc<crate::storage::disk_pack::PackRuntime>;

    /// Open or create a namespace (native collection / logical partition)
    async fn open_namespace(&self, name: &str) -> StorageResult<Arc<dyn KvStore>>;

    /// List all namespaces
    async fn list_namespaces(&self) -> StorageResult<Vec<String>>;

    /// Delete a namespace and all its data
    async fn delete_namespace(&self, name: &str) -> StorageResult<bool>;

    /// Make every photograph-restore batch durable before bootstrap exposes
    /// this store.
    ///
    /// Backends that share one durability domain across namespaces should
    /// override this method with one barrier. The safe default flushes every
    /// namespace.
    async fn restore_durability_barrier(&self) -> StorageResult<()> {
        for namespace in self.list_namespaces().await? {
            self.open_namespace(&namespace).await?.flush().await?;
        }
        Ok(())
    }
}
