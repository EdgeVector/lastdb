//! Namespace and physical-store capability surface.
use super::*;
use crate::storage::error::{StorageError, StorageResult};
use async_trait::async_trait;
use std::sync::Arc;

/// Namespace storage trait
///
/// Provides logical separation of data into "namespaces" or collections
/// (Last Store collections, or logical partitions in remote stores)
#[async_trait]
pub trait NamespacedStore: Send + Sync {
    /// Native disk runtime, when a complete native home backs these namespaces.
    fn disk_pack_runtime(&self) -> Option<Arc<crate::storage::disk_pack::PackRuntime>> {
        None
    }

    /// Open or create a namespace (Last Store collection / logical partition)
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

    /// Sync exactly these groups. One call is one barrier.
    ///
    /// Memory backends keep the no-op. A wrapper in front of LastStore must
    /// delegate: the default would let a durable batch ack without a barrier.
    async fn flush_written_keys(&self, _keys: &[laststore::ShardKey]) -> StorageResult<()> {
        Ok(())
    }

    /// The LastStore these namespaces share, when this backend has one.
    fn raw_last_store(&self) -> Option<Arc<laststore::LastStore>> {
        None
    }

    /// Clone the serving LastStore backup facade without reopening its active
    /// path. Its high-water lock and physical cut locks remain shared.
    fn laststore_backup_source(
        &self,
    ) -> Option<Arc<crate::storage::laststore::LastStoreNamespacedStore>> {
        None
    }

    /// Cold shard loads since open, if this backend tracks them.
    ///
    /// **Cheap by contract: one relaxed atomic load, no lock.** This is the
    /// accessor the per-request telemetry path may call. Wrapper stores
    /// delegate to their inner store; backends without hash-group shards
    /// return `None`.
    ///
    /// Kept separate from [`Self::read_cost`] on purpose — that one takes the
    /// warm-set mutex, which is fine for a periodic sampler and NOT fine on
    /// every request.
    fn cold_shard_loads(&self) -> Option<u64> {
        None
    }

    /// Ids stepped over by keys-only walks since open, if this backend tracks
    /// them. `None` on backends with no walk path.
    ///
    /// **Cheap by contract: one relaxed atomic load, no lock.** This is the
    /// scan sensor: a read path that claims to be scan-free must leave this
    /// counter unchanged. Wrapper stores delegate to their inner store, or a
    /// production stack that always wraps LastStore would report `None` and the
    /// assertion would silently pass on any implementation.
    fn walk_ids_visited(&self) -> Option<u64> {
        None
    }

    /// Full read-cost picture: cold loads plus warm-set residency vs budget.
    ///
    /// **Metadata-only by contract.** A backend may read counters and cached
    /// charge totals, but it must not open a namespace, admit a group, hydrate
    /// a body, or walk resident handles. Use [`Self::cold_shard_loads`] when a
    /// hot path needs only the atomic counter.
    fn read_cost(&self) -> Option<ReadCostStats> {
        None
    }

    /// Re-measure resident groups and release reproducible warm-cache bytes.
    ///
    /// The process-footprint sampler calls this only after measured memory
    /// diverges from its projection. Implementations must not open namespaces,
    /// admit cold groups, or hydrate bodies. `None` means the backend has no
    /// bounded warm cache.
    fn trim_warm_cache_for_pressure(&self) -> StorageResult<Option<u64>> {
        Ok(None)
    }

    /// In-flight cold-load charge and LRU eviction counters. Metadata only.
    fn warm_set_admission_stats(&self) -> Option<WarmSetAdmissionStats> {
        None
    }

    /// Shrink or grow the effective warm-set byte budget.
    fn set_effective_warm_bytes(&self, _bytes: u64) {}

    /// While held, a point admit that does not fit stays uncharged.
    /// Default is a no-op for backends with no warm set.
    fn set_warm_drain_hold(&self, _hold: bool) {}

    /// While set, the warm body budget is 0. Independent of drain hold.
    /// Default is a no-op for backends with no warm set.
    fn set_host_pressure_high(&self, _high: bool) {}

    /// Evict unpinned LRU groups until resident bytes are at or under `target`.
    ///
    /// Never refuses a request. `None` means the backend has no warm set.
    fn evict_warm_set_to_bytes(
        &self,
        _target: u64,
    ) -> StorageResult<Option<WarmSetEvictionReport>> {
        Ok(None)
    }

    /// Owner maintenance: drain one bounded page of plane residue (rows whose
    /// canonical plane home differs from the collection they sit in) toward
    /// the canonical collection. Copy-then-delete per key, target wins.
    ///
    /// Default is unsupported — only backends with real per-collection
    /// placement (LastStore) implement it. Exposed on the trait so the live
    /// daemon can run the drain over the owner socket instead of requiring an
    /// offline exclusive open of the primary home.
    async fn drain_plane_residue(
        &self,
        options: crate::storage::laststore::PlaneResidueDrainOptions,
    ) -> StorageResult<crate::storage::laststore::PlaneResidueDrainReport> {
        let _ = options;
        Err(StorageError::BackendError(
            "plane-residue drain is not supported by this storage backend".to_string(),
        ))
    }

    /// Owner maintenance: compact one LastStore collection (rewrite live keys,
    /// drop superseded segment records so space returns to the OS).
    ///
    /// Default is unsupported. LastStore implements it; encryption wrappers
    /// delegate. Callers must pass a collection on the compact allowlist
    /// (`atoms` requires the LastStore implementation's durable retirement
    /// receipt protocol).
    async fn compact_collection(
        &self,
        options: crate::storage::laststore::CollectionCompactOptions,
    ) -> StorageResult<crate::storage::laststore::CollectionCompactReport> {
        let _ = options;
        Err(StorageError::BackendError(
            "collection compact is not supported by this storage backend".to_string(),
        ))
    }

    /// Rewrite only the hash groups named by the store's retire receipt.
    ///
    /// A missing receipt is a no-op. Default is a no-op so a test double does
    /// not rewrite. LastStore implements it. Wrappers in front of LastStore
    /// must delegate, or the production stack would skip the pass.
    async fn compact_retired_groups(&self) -> StorageResult<()> {
        Ok(())
    }

    /// Owner maintenance: drop one cold hash group directory whole, after the
    /// store proves from its id sidecar that the group holds nothing but
    /// `options.expected_only_id`. Never loads the group.
    ///
    /// The reclaim for the 2026-09-21 primary restart loop: one `metadata`
    /// group filled with 39 GB of superseded `keep_small:meters` snapshots
    /// that could no longer be loaded, let alone compacted. Default is
    /// unsupported; LastStore implements it, wrappers delegate. Dry run by
    /// default (`options.dry_run`).
    fn drop_dead_hash_group(
        &self,
        options: crate::storage::laststore::DeadHashGroupDropOptions,
    ) -> StorageResult<crate::storage::laststore::DeadHashGroupDropReport> {
        let _ = options;
        Err(StorageError::BackendError(
            "dead hash-group drop is not supported by this storage backend".to_string(),
        ))
    }

    /// Report committed atom successor-history that a stamp would retire.
    /// Does not write the sidecar. Default is unsupported.
    fn report_committed_successor_history(
        &self,
    ) -> StorageResult<crate::storage::laststore::StampCommittedSuccessorHistoryReport> {
        Err(StorageError::BackendError(
            "successor-history stamp is not supported by this storage backend".to_string(),
        ))
    }

    /// Stamp committed atom successor-history SHAs into pending purged
    /// retirements. Default is unsupported.
    fn stamp_pending_committed_successor_history(
        &self,
    ) -> StorageResult<crate::storage::laststore::StampCommittedSuccessorHistoryReport> {
        Err(StorageError::BackendError(
            "successor-history stamp is not supported by this storage backend".to_string(),
        ))
    }

    /// Owner maintenance: rewrite existing `ENC:` values in one collection to
    /// `ENB:` (+ deflate at the 256 B floor) under the same key.
    ///
    /// Default is unsupported. The encrypting wrapper implements it; capture
    /// wrappers must delegate inside `with_capture_suppressed` so the rewrite
    /// cannot absorb into the mutation log. LastStore itself has no crypto.
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
    /// only the seam knows which namespaces it seals; capture wrappers delegate
    /// inside `with_capture_suppressed`. LastStore itself has no crypto and so
    /// cannot tell an unsealed row from a plaintext-by-policy one.
    async fn reap_unsealed(
        &self,
        options: crate::storage::reap_unsealed::ReapUnsealedOptions,
    ) -> StorageResult<crate::storage::reap_unsealed::ReapUnsealedReport> {
        let _ = options;
        Err(StorageError::BackendError(
            "reap-unsealed is not supported by this storage backend".to_string(),
        ))
    }

    /// Bytes this collection currently occupies on disk, if the backend knows.
    ///
    /// Deliberately weaker than the `compact_collection` dry-run, which also
    /// reports `live_keys` and therefore walks the whole key index — on a
    /// 1024-group plane that loads every shard handle, which is the exact cost
    /// an unattended bloat probe must not pay. This is a stat walk of the
    /// collection directory: no shard loads, no body hydrate, no index rebuild.
    ///
    /// `None` means the backend has no per-collection placement to measure
    /// (in-memory, tests). Callers must treat `None` as "cannot tell" and fall
    /// back to whatever trigger they had, never as "zero bytes".
    fn collection_disk_bytes(&self, collection: &str) -> Option<u64> {
        let _ = collection;
        None
    }

    /// Filesystem allocation versus apparent record length for one collection.
    ///
    /// This is a metadata-only proportional-residue probe: it never opens a
    /// shard or enumerates keys. Backends without per-collection files return
    /// `None`.
    ///
    /// LastStore implements this as a recursive directory walk. Do not call it
    /// on `/api/status` / `SyncEngine::status` — that path uses a TTL cache
    /// and must never `read_dir` `tips` / `atoms` / locators inline.
    fn collection_disk_usage(&self, collection: &str) -> Option<CollectionDiskUsage> {
        let _ = collection;
        None
    }
}
