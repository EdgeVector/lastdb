//! `KvStore` trait implementation for [`LastStoreKvStore`].

use super::*;

#[async_trait]
impl KvStore for LastStoreKvStore {
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        let store = Arc::clone(&self.store);
        let collection = self.collection.clone();
        let id = Self::encode_key(key);
        Self::run_blocking(move || {
            laststore::record_requested_keys(1);
            store
                .load_existing_point(&collection, &id)
                .map(|point| point.and_then(|point| point.body))
                .map_err(Self::map_error)
        })
        .await
    }

    /// Read one physical batch without a logical-value cache.
    async fn get_many(&self, keys: Vec<Vec<u8>>) -> StorageResult<Vec<Option<Vec<u8>>>> {
        let store = Arc::clone(&self.store);
        let collection = self.collection.clone();
        let ids: Vec<_> = keys.iter().map(|key| Self::encode_key(key)).collect();
        Self::run_blocking(move || {
            laststore::record_requested_keys(ids.len() as u64);
            store
                .load_existing_points(&collection, &ids)
                .map(|points| {
                    points
                        .into_iter()
                        .map(|point| point.and_then(|point| point.body))
                        .collect()
                })
                .map_err(Self::map_error)
        })
        .await
    }

    async fn put(&self, key: &[u8], value: Vec<u8>) -> StorageResult<()> {
        reject_enc_on_plaintext_catalog(&self.collection, &value)?;
        let store = Arc::clone(&self.store);
        let collection = self.collection.clone();
        let id = Self::encode_key(key);
        Self::run_blocking(move || store.put(&collection, &id, &value).map_err(Self::map_error))
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
        let id = Self::encode_key(key);
        let expected = expected.to_vec();
        Self::run_blocking(move || {
            store
                .compare_and_swap(&collection, &id, Some(&expected), new.as_deref())
                .map_err(Self::map_error)
        })
        .await
    }

    async fn delete(&self, key: &[u8]) -> StorageResult<bool> {
        let store = Arc::clone(&self.store);
        let collection = self.collection.clone();
        let id = Self::encode_key(key);
        Self::run_blocking(move || {
            let existed = store.exists(&collection, &id).map_err(Self::map_error)?;
            if existed {
                store.delete(&collection, &id).map_err(Self::map_error)?;
            }
            Ok(existed)
        })
        .await
    }

    async fn exists(&self, key: &[u8]) -> StorageResult<bool> {
        let store = Arc::clone(&self.store);
        let collection = self.collection.clone();
        let id = Self::encode_key(key);
        Self::run_blocking(move || {
            laststore::record_requested_keys(1);
            store
                .exists_points(&collection, &[id])
                .map(|present| present[0])
                .map_err(Self::map_error)
        })
        .await
    }

    /// Probe one physical key batch without body copies.
    async fn exists_many(&self, keys: Vec<Vec<u8>>) -> StorageResult<Vec<bool>> {
        let store = Arc::clone(&self.store);
        let collection = self.collection.clone();
        let ids: Vec<_> = keys.iter().map(|key| Self::encode_key(key)).collect();
        Self::run_blocking(move || {
            laststore::record_requested_keys(ids.len() as u64);
            store
                .exists_points(&collection, &ids)
                .map_err(Self::map_error)
        })
        .await
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let store = Arc::clone(&self.store);
        let collection = self.collection.clone();
        let prefix = prefix.to_vec();
        Self::run_blocking(move || Self::prefix_rows_sync(&store, &collection, &prefix, usize::MAX))
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
        let prefix = prefix.to_vec();
        Self::run_blocking(move || Self::prefix_keys_sync(&store, &collection, &prefix, usize::MAX))
            .await
    }

    async fn scan_prefix_paged(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let store = Arc::clone(&self.store);
        let collection = self.collection.clone();
        let prefix = prefix.to_vec();
        Self::run_blocking(move || Self::prefix_rows_sync(&store, &collection, &prefix, limit))
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
        let deferred = self.deferred_batch_flush;
        Self::run_blocking(move || {
            let ops = items
                .into_iter()
                .map(|(key, value)| TxnOp::put(&collection, &Self::encode_key(&key), value))
                .collect();
            if deferred {
                store.transaction_deferred(ops)
            } else {
                store.transaction(ops)
            }
            .map_err(Self::map_error)?;
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
        let deferred = self.deferred_batch_flush;
        Self::run_blocking(move || {
            let ops = mutations
                .into_iter()
                .map(|mutation| match mutation {
                    KvMutation::Put { key, value } => {
                        TxnOp::put(&collection, &Self::encode_key(&key), value)
                    }
                    KvMutation::Delete { key } => {
                        TxnOp::delete(&collection, &Self::encode_key(&key))
                    }
                })
                .collect();
            if deferred {
                store.transaction_deferred(ops)
            } else {
                store.transaction(ops)
            }
            .map_err(Self::map_error)?;
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
        Self::run_blocking(move || {
            let entries: Vec<_> = items
                .into_iter()
                .map(|(key, value)| (collection.clone(), Self::encode_key(&key), value))
                .collect();
            store
                .restore_put_many_deferred(entries, 16)
                .map_err(Self::map_error)?;
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
        let deferred = self.deferred_batch_flush;
        Self::run_blocking(move || {
            let ops = keys
                .into_iter()
                .map(|key| TxnOp::delete(&collection, &Self::encode_key(&key)))
                .collect();
            if deferred {
                store.transaction_deferred(ops)
            } else {
                store.transaction(ops)
            }
            .map_err(Self::map_error)?;
            Self::record_high_water(high_water.as_ref(), &store)
        })
        .await
    }

    async fn flush(&self) -> StorageResult<()> {
        let store = Arc::clone(&self.store);
        let high_water = self.high_water.clone();
        Self::run_blocking(move || {
            store.flush().map_err(Self::map_error)?;
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
