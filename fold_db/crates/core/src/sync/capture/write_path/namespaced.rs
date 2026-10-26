//! `NamespacedStore` wrapper that hands out capturing `KvStore` handles.

use super::*;

pub(crate) struct MutationLogCaptureNamespacedStore {
    inner: Arc<dyn NamespacedStore>,
    router: Arc<MutationLogCaptureRouter>,
}

impl MutationLogCaptureNamespacedStore {
    pub(crate) fn new(
        inner: Arc<dyn NamespacedStore>,
        router: Arc<MutationLogCaptureRouter>,
    ) -> Self {
        Self { inner, router }
    }
}

#[async_trait]
impl NamespacedStore for MutationLogCaptureNamespacedStore {
    fn disk_pack_runtime(&self) -> Option<Arc<crate::storage::disk_pack::PackRuntime>> {
        self.inner.disk_pack_runtime()
    }

    async fn open_namespace(&self, name: &str) -> StorageResult<Arc<dyn KvStore>> {
        self.inner.open_namespace(name).await
    }

    async fn list_namespaces(&self) -> StorageResult<Vec<String>> {
        self.inner.list_namespaces().await
    }

    async fn delete_namespace(&self, name: &str) -> StorageResult<bool> {
        self.inner.delete_namespace(name).await
    }

    fn cold_shard_loads(&self) -> Option<u64> {
        self.inner.cold_shard_loads()
    }

    fn walk_ids_visited(&self) -> Option<u64> {
        self.inner.walk_ids_visited()
    }

    fn read_cost(&self) -> Option<ReadCostStats> {
        self.inner.read_cost()
    }

    fn warm_set_admission_stats(&self) -> Option<crate::storage::traits::WarmSetAdmissionStats> {
        self.inner.warm_set_admission_stats()
    }

    async fn reseal_at_rest(
        &self,
        options: crate::storage::reseal_at_rest::ResealAtRestOptions,
    ) -> StorageResult<crate::storage::reseal_at_rest::ResealAtRestReport> {
        // Same-key ciphertext replace. Must not clone ENB bodies into the
        // mutation log (captured-plane compaction lesson). The inner pass
        // already writes LastStore directly; suppress anyway so a future
        // put through this wrapper cannot absorb.
        let _guard = self.router.enter_kv_suppress();
        with_capture_suppressed(self.inner.reseal_at_rest(options)).await
    }

    async fn reap_unsealed(
        &self,
        options: crate::storage::reap_unsealed::ReapUnsealedOptions,
    ) -> StorageResult<crate::storage::reap_unsealed::ReapUnsealedReport> {
        // Deletes of rows that already read as absent. The inner pass deletes
        // on the raw store, so nothing here reaches this wrapper's delete;
        // suppress anyway so a future put/delete through the wrapper cannot
        // absorb a reap into the mutation log and replay it on a peer.
        let _guard = self.router.enter_kv_suppress();
        with_capture_suppressed(self.inner.reap_unsealed(options)).await
    }

    // This wrapper is the OUTERMOST store on the production write path, so a
    // missing delegation here reads as "backend cannot measure" to every caller
    // above it, on the only node that matters. Capture changes what is logged,
    // never how many bytes a collection directory holds.
    fn collection_disk_bytes(&self, collection: &str) -> Option<u64> {
        self.inner.collection_disk_bytes(collection)
    }
}
