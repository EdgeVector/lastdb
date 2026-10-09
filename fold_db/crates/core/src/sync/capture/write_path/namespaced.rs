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
    async fn open_namespace(&self, name: &str) -> StorageResult<Arc<dyn KvStore>> {
        let inner = self.inner.open_namespace(name).await?;
        if policy::capture_should_skip_namespace(name) {
            return Ok(inner);
        }
        Ok(Arc::new(MutationLogCaptureKvStore {
            inner,
            router: Arc::clone(&self.router),
            namespace: name.to_string(),
        }))
    }

    async fn list_namespaces(&self) -> StorageResult<Vec<String>> {
        self.inner.list_namespaces().await
    }

    async fn delete_namespace(&self, name: &str) -> StorageResult<bool> {
        self.inner.delete_namespace(name).await
    }

    async fn flush_written_keys(&self, keys: &[laststore::ShardKey]) -> StorageResult<()> {
        self.inner.flush_written_keys(keys).await
    }

    fn raw_last_store(&self) -> Option<Arc<laststore::LastStore>> {
        self.inner.raw_last_store()
    }

    fn logical_resident_set(
        &self,
    ) -> Option<Arc<std::sync::Mutex<crate::resident::LogicalResidentSet>>> {
        self.inner.logical_resident_set()
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

    fn trim_warm_cache_for_pressure(&self) -> StorageResult<Option<u64>> {
        self.inner.trim_warm_cache_for_pressure()
    }

    fn warm_set_admission_stats(&self) -> Option<crate::storage::traits::WarmSetAdmissionStats> {
        self.inner.warm_set_admission_stats()
    }

    fn set_effective_warm_bytes(&self, bytes: u64) {
        self.inner.set_effective_warm_bytes(bytes);
    }

    fn set_warm_drain_hold(&self, hold: bool) {
        self.inner.set_warm_drain_hold(hold);
    }

    fn set_host_pressure_high(&self, high: bool) {
        self.inner.set_host_pressure_high(high);
    }

    fn evict_warm_set_to_bytes(
        &self,
        target: u64,
    ) -> StorageResult<Option<crate::storage::traits::WarmSetEvictionReport>> {
        self.inner.evict_warm_set_to_bytes(target)
    }

    async fn drain_plane_residue(
        &self,
        options: crate::storage::laststore::PlaneResidueDrainOptions,
    ) -> StorageResult<crate::storage::laststore::PlaneResidueDrainReport> {
        self.inner.drain_plane_residue(options).await
    }

    async fn compact_collection(
        &self,
        options: crate::storage::laststore::CollectionCompactOptions,
    ) -> StorageResult<crate::storage::laststore::CollectionCompactReport> {
        // Physical LastStore compact rewrites segments under the inner store
        // and does not call this wrapper's put. Suppress anyway so a future
        // put-loop rewrite of live `mk:` keys cannot `value.clone()` into the
        // pin-log (2026-08-08 amplifier). Combined with the `mk:` capture skip.
        let _guard = self.router.enter_kv_suppress();
        with_capture_suppressed(self.inner.compact_collection(options)).await
    }

    async fn compact_retired_groups(&self) -> StorageResult<()> {
        // Same seam as collection compact: the rewrite is inside LastStore.
        // Suppress so a later put-loop cannot copy bodies into the pin log.
        let _guard = self.router.enter_kv_suppress();
        with_capture_suppressed(self.inner.compact_retired_groups()).await
    }

    fn report_committed_successor_history(
        &self,
    ) -> StorageResult<crate::storage::laststore::StampCommittedSuccessorHistoryReport> {
        self.inner.report_committed_successor_history()
    }

    // A directory drop never calls put, so there is nothing to capture; the
    // dropped key was capture-skipped anyway (`CAPTURE_SKIP_EXACT_KEYS`).
    fn drop_dead_hash_group(
        &self,
        options: crate::storage::laststore::DeadHashGroupDropOptions,
    ) -> StorageResult<crate::storage::laststore::DeadHashGroupDropReport> {
        self.inner.drop_dead_hash_group(options)
    }

    fn stamp_pending_committed_successor_history(
        &self,
    ) -> StorageResult<crate::storage::laststore::StampCommittedSuccessorHistoryReport> {
        self.inner.stamp_pending_committed_successor_history()
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

    fn collection_disk_usage(
        &self,
        collection: &str,
    ) -> Option<crate::storage::traits::CollectionDiskUsage> {
        self.inner.collection_disk_usage(collection)
    }
}
