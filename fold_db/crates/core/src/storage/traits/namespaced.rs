//! Namespace and physical-store capability surface.
use super::KvStore;
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
}
