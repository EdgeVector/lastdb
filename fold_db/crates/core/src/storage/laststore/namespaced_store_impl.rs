//! `NamespacedStore` trait implementation for [`LastStoreNamespacedStore`].

use super::*;

#[async_trait]
impl NamespacedStore for LastStoreNamespacedStore {
    async fn restore_durability_barrier(&self) -> StorageResult<()> {
        let store = Arc::clone(&self.store);
        let high_water = self.high_water.clone();
        LastStoreKvStore::run_blocking(move || {
            store
                .flush_restore_parallel(64)
                .map_err(LastStoreKvStore::map_error)?;
            LastStoreKvStore::record_high_water(high_water.as_ref(), &store)
        })
        .await
    }

    async fn flush_written_keys(&self, keys: &[laststore::ShardKey]) -> StorageResult<()> {
        let store = Arc::clone(&self.store);
        let high_water = self.high_water.clone();
        let keys = keys.to_vec();
        // One scope, every collection. A second call would be a second barrier.
        let logical = Arc::clone(&self.logical);
        LastStoreKvStore::run_blocking(move || {
            store
                .flush_scope(&keys)
                .map_err(LastStoreKvStore::map_error)?;
            logical_path::apply_durable_through(&store, &logical);
            LastStoreKvStore::record_high_water(high_water.as_ref(), &store)
        })
        .await
    }

    fn raw_last_store(&self) -> Option<Arc<LastStore>> {
        Some(Arc::clone(&self.store))
    }

    fn logical_resident_set(&self) -> Option<Arc<Mutex<LogicalResidentSet>>> {
        Some(Arc::clone(&self.logical))
    }

    async fn drain_plane_residue(
        &self,
        options: PlaneResidueDrainOptions,
    ) -> StorageResult<PlaneResidueDrainReport> {
        self.drain_plane_residue_collection(options).await
    }

    async fn compact_collection(
        &self,
        options: CollectionCompactOptions,
    ) -> StorageResult<CollectionCompactReport> {
        self.compact_collection_admin(options).await
    }

    async fn compact_retired_groups(&self) -> StorageResult<()> {
        let store = Arc::clone(&self.store);
        LastStoreKvStore::run_blocking(move || {
            let sample = crate::memory_budget::sample_host_pressure();
            let pressure_high = store.host_pressure_is_high()
                || sample.is_some_and(crate::memory_budget::HostPressure::is_raw_high);
            let ram_bytes = sample.map_or(0, |sample| sample.ram_bytes);
            let gate = RetiredCompactGate {
                pressure_high,
                phys_footprint_bytes: crate::memory_budget::current_phys_footprint_bytes(),
                // Same stop footprint eviction uses while pressure is high:
                // 4 GiB, then the RAM minimum. Not the 8 GiB clear target,
                // and not the 7 GiB warm env.
                footprint_stop_bytes: crate::memory_budget::pressure_footprint_target_bytes(
                    ram_bytes,
                ),
            };
            let path = store.path().join(laststore::RETIRED_GROUPS_RECEIPT_FILE);
            store
                .compact_retired_receipt(&path, gate)
                .map_err(LastStoreKvStore::map_error)
        })
        .await
    }

    fn report_committed_successor_history(
        &self,
    ) -> StorageResult<StampCommittedSuccessorHistoryReport> {
        backup_manifest::report_committed_successor_history(self)
    }

    fn drop_dead_hash_group(
        &self,
        options: DeadHashGroupDropOptions,
    ) -> StorageResult<DeadHashGroupDropReport> {
        let collection = options.collection.trim();
        let id = options.expected_only_id.as_str();
        if collection.is_empty() || id.is_empty() {
            return Err(StorageError::BackendError(
                "drop-dead-hash-group: collection and expected id are required".to_string(),
            ));
        }
        let group = self.store.group_index_of(collection, id);
        let report = self
            .store
            .drop_hash_group_dir(collection, group, id, !options.dry_run)
            .map_err(LastStoreKvStore::map_error)?;
        Ok(DeadHashGroupDropReport {
            collection: report.collection,
            expected_only_id: options.expected_only_id,
            dry_run: options.dry_run,
            shard: report.shard,
            group: report.group,
            dir: report.dir.display().to_string(),
            segments: report.segments,
            on_disk_bytes: report.on_disk_bytes,
            ids: report.ids,
            dropped: report.dropped,
            already_absent: report.already_absent,
        })
    }

    fn stamp_pending_committed_successor_history(
        &self,
    ) -> StorageResult<StampCommittedSuccessorHistoryReport> {
        backup_manifest::stamp_pending_committed_successor_history(self)
    }

    fn collection_disk_bytes(&self, collection: &str) -> Option<u64> {
        Some(collection_dir_bytes(self.store.path(), collection))
    }

    fn collection_disk_usage(
        &self,
        collection: &str,
    ) -> Option<crate::storage::traits::CollectionDiskUsage> {
        let dir = self.store.path().join("data").join(collection);
        let usage = crate::mini_cutover::plane_roles::dir_size_bytes(&dir).ok()?;
        // Record-byte residue from the store's own counters: resident groups
        // answer from memory, cold groups from their id sidecar, and neither
        // reads a segment. A probe that cannot list the collection reports
        // the filesystem numbers alone rather than failing the whole read.
        let residue = self.store.collection_residue(collection).ok();
        Some(crate::storage::traits::CollectionDiskUsage {
            allocated_bytes: usage.allocated,
            apparent_bytes: usage.apparent,
            live_bytes: residue.map(|r| r.live_bytes),
            dead_bytes: residue.map(|r| r.dead_bytes),
            residue_unknown_bytes: residue.map_or(0, |r| r.unknown_bytes),
        })
    }

    async fn open_namespace(&self, name: &str) -> StorageResult<Arc<dyn KvStore>> {
        if name == LOGICAL_MAIN_COLLECTION {
            return Ok(Arc::new(LogicalMainLastStoreKvStore::with_logical(
                Arc::clone(&self.store),
                self.high_water.clone(),
                Arc::clone(&self.logical),
            )));
        }
        Ok(Arc::new(LastStoreKvStore::with_logical(
            Arc::clone(&self.store),
            name.to_string(),
            self.high_water.clone(),
            Arc::clone(&self.logical),
        )))
    }

    async fn list_namespaces(&self) -> StorageResult<Vec<String>> {
        let store = Arc::clone(&self.store);
        LastStoreKvStore::run_blocking(move || {
            store
                .collections_on_disk()
                .map_err(LastStoreKvStore::map_error)
        })
        .await
    }

    fn cold_shard_loads(&self) -> Option<u64> {
        Some(self.store.shard_loads())
    }

    fn walk_ids_visited(&self) -> Option<u64> {
        Some(self.store.walk_ids_visited())
    }

    fn read_cost(&self) -> Option<crate::storage::traits::ReadCostStats> {
        let warm = self.store.hash_group_warm_stats();
        let id_tiers = self.store.id_tier_stats();
        Some(crate::storage::traits::ReadCostStats {
            cold_shard_loads: self.store.shard_loads(),
            warm_resident_groups: warm.resident_groups as u64,
            warm_resident_bytes: warm.resident_bytes,
            warm_budget_bytes: warm.budget_bytes,
            warm_budget_handles: warm.budget_handles as u64,
            open_append_handles: warm.open_append_handles as u64,
            torn_transaction_rollbacks: self.store.torn_transaction_rollbacks(),
            torn_transaction_rollback_failures: self.store.torn_transaction_rollback_failures(),
            transaction_residency_refresh_failures: self
                .store
                .transaction_residency_refresh_failures(),
            id_tier_resident: id_tiers.resident,
            id_tier_key_cache_hits: id_tiers.key_cache_hits,
            id_tier_sidecar_hits: id_tiers.sidecar_hits,
            id_tier_live_scans: id_tiers.live_scans,
            key_cache_groups: warm.key_cache_groups as u64,
            key_cache_bytes: warm.key_cache_bytes,
            key_cache_budget_bytes: warm.key_cache_budget_bytes,
        })
    }

    fn trim_warm_cache_for_pressure(&self) -> StorageResult<Option<u64>> {
        self.store
            .trim_hash_group_warm_cache_for_pressure()
            .map(Some)
            .map_err(LastStoreKvStore::map_error)
    }

    fn warm_set_admission_stats(&self) -> Option<crate::storage::traits::WarmSetAdmissionStats> {
        let warm = self.store.hash_group_warm_stats();
        Some(crate::storage::traits::WarmSetAdmissionStats {
            in_flight_cold_load_count: warm.in_flight_cold_load_count,
            in_flight_cold_load_bytes: warm.in_flight_cold_load_bytes,
            eviction_events: warm.eviction_events,
            effective_warm_bytes: warm.budget_bytes,
        })
    }

    fn set_effective_warm_bytes(&self, bytes: u64) {
        self.store.set_effective_warm_bytes(bytes);
    }

    fn set_warm_drain_hold(&self, hold: bool) {
        self.store.set_warm_drain_hold(hold);
    }

    fn set_host_pressure_high(&self, high: bool) {
        self.store.set_host_pressure_high(high);
    }

    fn evict_warm_set_to_bytes(
        &self,
        target: u64,
    ) -> StorageResult<Option<crate::storage::traits::WarmSetEvictionReport>> {
        let report = self
            .store
            .evict_hash_group_warm_set_to_bytes(target)
            .map_err(LastStoreKvStore::map_error)?;
        Ok(Some(crate::storage::traits::WarmSetEvictionReport {
            groups_evicted: report.groups_evicted,
            bytes_before: report.bytes_before,
            bytes_after: report.bytes_after,
        }))
    }

    async fn delete_namespace(&self, name: &str) -> StorageResult<bool> {
        let store = Arc::clone(&self.store);
        let collection = name.to_string();
        let logical = Arc::clone(&self.logical);
        LastStoreKvStore::run_blocking(move || {
            // Keys-only walk of this collection, then batch delete — no body hydrate.
            let keys = store
                .list_prefix_keys(&collection, "")
                .map_err(LastStoreKvStore::map_error)?;
            if keys.is_empty() {
                return Ok(false);
            }
            let ops = keys
                .into_iter()
                .map(|id| TxnOp::delete(&collection, &id))
                .collect();
            direct_write_invalidation::after_direct_write(&logical, || {
                store.transaction(ops).map_err(LastStoreKvStore::map_error)
            })?;
            Ok(true)
        })
        .await
    }
}
