//! Namespace and physical-store capability surface.
use super::*;
use crate::storage::error::{StorageError, StorageResult};
use async_trait::async_trait;
use std::sync::Arc;

/// Namespace storage trait
///
/// Provides logical separation of data into "namespaces" or collections
/// (native collections, or logical partitions in remote stores)
#[async_trait]
pub trait NamespacedStore: Send + Sync {
    /// Native disk runtime, when a complete native home backs these namespaces.
    fn disk_pack_runtime(&self) -> Option<Arc<crate::storage::disk_pack::PackRuntime>> {
        None
    }

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

    /// Cold shard loads since open, if this backend tracks them.
    ///
    /// **Cheap by contract: one relaxed atomic load, no lock.** This is the
    /// accessor the per-request telemetry path may call. Wrapper stores
    /// delegate to their inner store; backends without hash-group shards
    /// return `None`.
    fn cold_shard_loads(&self) -> Option<u64> {
        None
    }

    /// Ids stepped over by keys-only walks since open, if this backend tracks
    /// them. `None` on backends with no walk path.
    ///
    /// **Cheap by contract: one relaxed atomic load, no lock.** This is the
    /// scan sensor: a read path that claims to be scan-free must leave this
    /// counter unchanged. Wrapper stores delegate to their inner store.
    fn walk_ids_visited(&self) -> Option<u64> {
        None
    }

    /// Owner maintenance: rewrite existing `ENC:` values in one collection to
    /// `ENB:` (+ deflate at the 256 B floor) under the same key.
    ///
    /// Default is unsupported. The encrypting wrapper implements it. The
    /// physical store has no crypto.
    async fn reseal_at_rest(
        &self,
        options: crate::storage::reseal_at_rest::ResealAtRestOptions,
    ) -> StorageResult<crate::storage::reseal_at_rest::ResealAtRestReport> {
        let _ = options;
        Err(StorageError::BackendError(
            "reseal-at-rest is not supported by this storage backend".to_string(),
        ))
    }

    /// Owner maintenance: remove un-enveloped rows from one encrypted
    /// collection. Such rows already read as absent
    /// (`decision-2026-09-14-drop-dual-read-unsealed-is-gone`); this returns
    /// their bytes.
    ///
    /// Default is unsupported. The encrypting wrapper implements it, because
    /// only the seam knows which namespaces it seals. The physical store has
    /// no crypto and so cannot tell an unsealed row from a plaintext-by-policy
    /// one.
    async fn reap_unsealed(
        &self,
        options: crate::storage::reap_unsealed::ReapUnsealedOptions,
    ) -> StorageResult<crate::storage::reap_unsealed::ReapUnsealedReport> {
        let _ = options;
        Err(StorageError::BackendError(
            "reap-unsealed is not supported by this storage backend".to_string(),
        ))
    }
}
