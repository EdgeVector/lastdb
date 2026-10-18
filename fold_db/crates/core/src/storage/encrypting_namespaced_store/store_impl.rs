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

    async fn flush_written_keys(&self, keys: &[laststore::ShardKey]) -> StorageResult<()> {
        self.inner.flush_written_keys(keys).await
    }

    fn raw_last_store(&self) -> Option<Arc<laststore::LastStore>> {
        self.inner.raw_last_store()
    }

    fn laststore_backup_source(
        &self,
    ) -> Option<Arc<crate::storage::laststore::LastStoreNamespacedStore>> {
        self.inner.laststore_backup_source()
    }

    async fn open_namespace(&self, name: &str) -> StorageResult<Arc<dyn KvStore>> {
        let inner_store = self.inner.open_namespace(name).await?;

        if self.should_encrypt(name) {
            Ok(Arc::new(EncryptingKvStore::new(
                name,
                inner_store,
                self.crypto.clone(),
            )))
        } else {
            // Non-sensitive namespaces: pass through without encryption
            Ok(inner_store)
        }
    }

    async fn list_namespaces(&self) -> StorageResult<Vec<String>> {
        self.inner.list_namespaces().await
    }

    async fn delete_namespace(&self, name: &str) -> StorageResult<bool> {
        self.inner.delete_namespace(name).await
    }

    // The plane drain moves opaque value bytes between collections BELOW the
    // encryption seam (collection placement lives inside one encrypted
    // namespace), so ciphertext stays valid — the same reason the offline
    // `lastdb_local_maintain` drain works on real homes. Delegate, or the
    // production stack — which always wraps LastStore in this store — would
    // report "unsupported" on exactly the node the drain exists for.
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
        self.inner.compact_collection(options).await
    }

    async fn compact_retired_groups(&self) -> StorageResult<()> {
        // The production stack always wraps LastStore in this store. A default
        // no-op here would drop the receipt pass on the node it exists for.
        self.inner.compact_retired_groups().await
    }

    fn report_committed_successor_history(
        &self,
    ) -> StorageResult<crate::storage::laststore::StampCommittedSuccessorHistoryReport> {
        self.inner.report_committed_successor_history()
    }

    // Directory-level; the seam sees no values. Ids are plaintext on every
    // packaging this verb can prove (it needs the id sidecar), so the
    // expected id passes through unchanged.
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
        options: ResealAtRestOptions,
    ) -> StorageResult<ResealAtRestReport> {
        self.reseal_at_rest_impl(options).await
    }

    async fn reap_unsealed(
        &self,
        options: ReapUnsealedOptions,
    ) -> StorageResult<ReapUnsealedReport> {
        self.reap_unsealed_impl(options).await
    }

    // Bytes on disk are bytes on disk: encryption changes the size of a record
    // but not which backend owns the directory. Delegate, or the production
    // stack — which always wraps LastStore in this store — would report `None`
    // and the pin-log bloat trigger would be blind on exactly the node that
    // carried 20.4 GiB of dead segment bytes behind one live record.
    fn collection_disk_bytes(&self, collection: &str) -> Option<u64> {
        self.inner.collection_disk_bytes(collection)
    }

    fn collection_disk_usage(
        &self,
        collection: &str,
    ) -> Option<crate::storage::traits::CollectionDiskUsage> {
        self.inner.collection_disk_usage(collection)
    }

    // Read cost is a property of the physical store, not of the encryption
    // seam. Delegate, or the production stack — which always wraps LastStore in
    // this store — would report `None` and the counters would stay invisible on
    // exactly the node they exist for.
    fn cold_shard_loads(&self) -> Option<u64> {
        self.inner.cold_shard_loads()
    }

    fn walk_ids_visited(&self) -> Option<u64> {
        self.inner.walk_ids_visited()
    }

    fn read_cost(&self) -> Option<crate::storage::traits::ReadCostStats> {
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
}
