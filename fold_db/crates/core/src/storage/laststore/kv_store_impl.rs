//! `KvStore` trait implementation for [`LastStoreKvStore`].
// lint:file-size-ok moved verbatim from its original module; one cohesive trait impl per file

use super::*;

#[async_trait]
impl KvStore for LastStoreKvStore {
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        let store = Arc::clone(&self.store);
        let collection = self.collection.clone();
        let logical = Arc::clone(&self.logical);
        let key = key.to_vec();
        Self::run_blocking(move || logical_path::get(&store, &logical, &collection, &key)).await
    }

    /// Batch point-read — the co-key list path's secondary hydrate
    /// (`get_items` → `HashRangeQueryProcessor`) lands here with every atom id
    /// of a page at once.
    ///
    /// Batch point-read through the logical resident set.
    ///
    /// Keys that share one hash group share one unpublished open. A
    /// `get`-per-key loop paid one `load_point` per key; with `atoms` in
    /// `HashGroup` layout the ids of a single page scatter across groups, so a
    /// bounded warm set re-parsed a whole group segment per row.
    async fn get_many(&self, keys: Vec<Vec<u8>>) -> StorageResult<Vec<Option<Vec<u8>>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let store = Arc::clone(&self.store);
        let collection = self.collection.clone();
        let logical = Arc::clone(&self.logical);
        Self::run_blocking(move || logical_path::get_many(&store, &logical, &collection, &keys))
            .await
    }

    async fn put(&self, key: &[u8], value: Vec<u8>) -> StorageResult<()> {
        reject_enc_on_plaintext_catalog(&self.collection, &value)?;
        let store = Arc::clone(&self.store);
        let collection = self.collection.clone();
        let logical = Arc::clone(&self.logical);
        let key = key.to_vec();
        Self::run_blocking(move || logical_path::put(&store, &logical, &collection, &key, &value))
            .await
    }

    /// Native conditional replace: [`LastStore::compare_and_swap`] compares and
    /// writes under the id's transaction stripe, which every point write and
    /// transaction on this collection also takes.
    async fn compare_and_swap(
        &self,
        key: &[u8],
        expected: &[u8],
        new: Option<Vec<u8>>,
    ) -> StorageResult<bool> {
        if let Some(value) = &new {
            reject_enc_on_plaintext_catalog(&self.collection, value)?;
        }
        let store = Arc::clone(&self.store);
        let collection = self.collection.clone();
        let logical = Arc::clone(&self.logical);
        let id = Self::encode_key(key);
        let expected = expected.to_vec();
        Self::run_blocking(move || {
            let result = store
                .compare_and_swap(&collection, &id, Some(&expected), new.as_deref())
                .map_err(Self::map_error);
            logical
                .lock()
                .expect("poison")
                .forget_record(&collection, &id);
            result
        })
        .await
    }

    async fn delete(&self, key: &[u8]) -> StorageResult<bool> {
        let store = Arc::clone(&self.store);
        let collection = self.collection.clone();
        let logical = Arc::clone(&self.logical);
        let key = key.to_vec();
        Self::run_blocking(move || logical_path::delete(&store, &logical, &collection, &key)).await
    }

    async fn exists(&self, key: &[u8]) -> StorageResult<bool> {
        let store = Arc::clone(&self.store);
        let collection = self.collection.clone();
        let logical = Arc::clone(&self.logical);
        let key = key.to_vec();
        Self::run_blocking(move || logical_path::exists(&store, &logical, &collection, &key)).await
    }

    /// Batch existence probe through the logical resident set.
    ///
    /// Keys that share one hash group share one unpublished open. The probe
    /// does not copy bodies.
    async fn exists_many(&self, keys: Vec<Vec<u8>>) -> StorageResult<Vec<bool>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let store = Arc::clone(&self.store);
        let collection = self.collection.clone();
        let logical = Arc::clone(&self.logical);
        Self::run_blocking(move || logical_path::exists_many(&store, &logical, &collection, &keys))
            .await
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let store = Arc::clone(&self.store);
        let collection = self.collection.clone();
        let logical = Arc::clone(&self.logical);
        let prefix = prefix.to_vec();
        Self::run_blocking(move || {
            logical_path::scan_prefix(&store, &logical, &collection, &prefix, usize::MAX)
        })
        .await
    }

    async fn scan_prefix_partition_undecryptable(
        &self,
        prefix: &[u8],
    ) -> StorageResult<PartitionedScan> {
        partition_scan::scan_prefix_partitioned(self, prefix).await
    }

    async fn scan_prefix_keys(&self, prefix: &[u8]) -> StorageResult<Vec<Vec<u8>>> {
        let store = Arc::clone(&self.store);
        let collection = self.collection.clone();
        let logical = Arc::clone(&self.logical);
        let prefix = prefix.to_vec();
        Self::run_blocking(move || {
            logical_path::scan_prefix_keys(&store, &logical, &collection, &prefix, usize::MAX)
        })
        .await
    }

    async fn scan_prefix_paged(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let store = Arc::clone(&self.store);
        let collection = self.collection.clone();
        let logical = Arc::clone(&self.logical);
        let prefix = prefix.to_vec();
        Self::run_blocking(move || {
            logical_path::scan_prefix(&store, &logical, &collection, &prefix, limit)
        })
        .await
    }

    async fn scan_range(&self, start: &[u8], end: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let store = Arc::clone(&self.store);
        let collection = self.collection.clone();
        let start = start.to_vec();
        let end = end.to_vec();
        Self::run_blocking(move || {
            Self::range_rows_sync(&store, &collection, &start, &end, usize::MAX)
        })
        .await
    }

    async fn scan_range_paged(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let store = Arc::clone(&self.store);
        let collection = self.collection.clone();
        let start = start.to_vec();
        let end = end.to_vec();
        Self::run_blocking(move || Self::range_rows_sync(&store, &collection, &start, &end, limit))
            .await
    }

    async fn max_key_u64_after_marker(
        &self,
        prefix: &[u8],
        marker: &[u8],
    ) -> StorageResult<Option<u64>> {
        let store = Arc::clone(&self.store);
        let collection = self.collection.clone();
        let prefix = prefix.to_vec();
        let marker = marker.to_vec();
        Self::run_blocking(move || {
            Self::max_key_u64_after_marker_sync(&store, &collection, &prefix, &marker)
        })
        .await
    }

    async fn batch_put(&self, items: Vec<(Vec<u8>, Vec<u8>)>) -> StorageResult<()> {
        for (_key, value) in &items {
            reject_enc_on_plaintext_catalog(&self.collection, value)?;
        }
        let store = Arc::clone(&self.store);
        let high_water = self.high_water.clone();
        let collection = self.collection.clone();
        let logical = Arc::clone(&self.logical);
        let deferred = self.deferred_batch_flush;
        Self::run_blocking(move || {
            let ops = items
                .into_iter()
                .map(|(key, value)| logical_path::BatchOp::Put {
                    collection: collection.clone(),
                    key,
                    value,
                })
                .collect();
            logical_path::apply_batch(&store, &logical, ops, deferred)?;
            Self::record_high_water(high_water.as_ref(), &store)
        })
        .await
    }

    fn batch_put_has_durable_barrier(&self) -> bool {
        !self.deferred_batch_flush
    }

    async fn batch_mutate(&self, mutations: Vec<KvMutation>) -> StorageResult<()> {
        for mutation in &mutations {
            if let KvMutation::Put { value, .. } = mutation {
                reject_enc_on_plaintext_catalog(&self.collection, value)?;
            }
        }
        let store = Arc::clone(&self.store);
        let high_water = self.high_water.clone();
        let collection = self.collection.clone();
        let logical = Arc::clone(&self.logical);
        let deferred = self.deferred_batch_flush;
        Self::run_blocking(move || {
            let ops = mutations
                .into_iter()
                .map(|mutation| match mutation {
                    KvMutation::Put { key, value } => logical_path::BatchOp::Put {
                        collection: collection.clone(),
                        key,
                        value,
                    },
                    KvMutation::Delete { key } => logical_path::BatchOp::Delete {
                        collection: collection.clone(),
                        key,
                    },
                })
                .collect();
            logical_path::apply_batch(&store, &logical, ops, deferred)?;
            Self::record_high_water(high_water.as_ref(), &store)
        })
        .await
    }

    async fn restore_batch_put(&self, items: Vec<(Vec<u8>, Vec<u8>)>) -> StorageResult<()> {
        for (_key, value) in &items {
            reject_enc_on_plaintext_catalog(&self.collection, value)?;
        }
        let store = Arc::clone(&self.store);
        let high_water = self.high_water.clone();
        let collection = self.collection.clone();
        let logical = Arc::clone(&self.logical);
        Self::run_blocking(move || {
            let entries = items
                .into_iter()
                .map(|(key, value)| (collection.clone(), Self::encode_key(&key), value))
                .collect();
            direct_write_invalidation::after_direct_write(&logical, || {
                store
                    .restore_put_many_deferred(entries, 16)
                    .map_err(Self::map_error)
            })?;
            Self::record_high_water(high_water.as_ref(), &store)
        })
        .await
    }

    async fn scan_range_physical_paged(
        &self,
        start: &[u8],
        end: &[u8],
        cursor: Option<&PhysicalScanCursor>,
        limit: usize,
        max_handles: usize,
    ) -> StorageResult<PhysicalScanPage> {
        let store = Arc::clone(&self.store);
        let collection = self.collection.clone();
        let start = Self::encode_bound(start);
        let end = Self::encode_bound(end);
        let start_empty = start.is_empty();
        let vendor_cursor = cursor.map(|position| laststore::PhysicalRangeCursor {
            shard: position.shard,
            group_id: position.group_id,
            after_id: position.after_key.as_deref().map(Self::encode_key),
        });
        Self::run_blocking(move || {
            // Empty start is a whole-collection maintenance walk. Product
            // prefix/range reads still reject under LASTDB_READS_REQUIRE_PARTITION.
            // walk_all_groups_at_startup is the unbounded form; a finite end
            // cannot cover every encoded key.
            let page = if start_empty {
                store.walk_all_groups_at_startup(
                    &collection,
                    vendor_cursor.as_ref(),
                    limit,
                    max_handles,
                )
            } else {
                store.walk_all_groups(
                    &collection,
                    start.as_str()..end.as_str(),
                    vendor_cursor.as_ref(),
                    limit,
                    max_handles,
                    laststore::AllGroupsPurpose::Admin,
                )
            }
            .map_err(Self::map_error)?;
            let rows = Self::decode_rows(page.rows)?;
            let map_cursor = |position: laststore::PhysicalRangeCursor| {
                Ok::<PhysicalScanCursor, StorageError>(PhysicalScanCursor {
                    collection: Some(collection.clone()),
                    shard: position.shard,
                    group_id: position.group_id,
                    after_key: position
                        .after_id
                        .map(|id| Self::decode_key(&id))
                        .transpose()?,
                })
            };
            let next_cursor = page.next_cursor.map(map_cursor).transpose()?;
            let row_handle = page.row_handle.map(|(shard, group_id)| PhysicalScanCursor {
                collection: Some(collection),
                shard,
                group_id,
                after_key: None,
            });
            Ok(PhysicalScanPage {
                rows,
                next_cursor,
                row_handle,
                handles_visited: page.handles_visited,
                cold_shard_loads: page.cold_shard_loads,
            })
        })
        .await
    }

    async fn batch_delete(&self, keys: Vec<Vec<u8>>) -> StorageResult<()> {
        let store = Arc::clone(&self.store);
        let high_water = self.high_water.clone();
        let collection = self.collection.clone();
        let logical = Arc::clone(&self.logical);
        let deferred = self.deferred_batch_flush;
        Self::run_blocking(move || {
            let ops = keys
                .into_iter()
                .map(|key| logical_path::BatchOp::Delete {
                    collection: collection.clone(),
                    key,
                })
                .collect();
            logical_path::apply_batch(&store, &logical, ops, deferred)?;
            Self::record_high_water(high_water.as_ref(), &store)
        })
        .await
    }

    async fn flush(&self) -> StorageResult<()> {
        let store = Arc::clone(&self.store);
        let high_water = self.high_water.clone();
        let logical = Arc::clone(&self.logical);
        Self::run_blocking(move || {
            store.flush().map_err(Self::map_error)?;
            logical_path::apply_durable_through(&store, &logical);
            Self::record_high_water(high_water.as_ref(), &store)
        })
        .await
    }

    fn backend_name(&self) -> &'static str {
        "laststore"
    }

    fn execution_model(&self) -> ExecutionModel {
        ExecutionModel::SyncWrapped
    }

    fn flush_behavior(&self) -> FlushBehavior {
        FlushBehavior::Persists
    }
}
