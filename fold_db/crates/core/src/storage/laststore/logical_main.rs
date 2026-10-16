//! The `main` logical-plane [`KvStore`]: routes each key to its physical collection.
// lint:file-size-ok moved verbatim from its original module; one cohesive family per file

use super::*;

mod forms;
mod physical_scan;

#[async_trait]
impl KvStore for LogicalMainLastStoreKvStore {
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        let store = Arc::clone(&self.store);
        let logical = Arc::clone(&self.logical);
        let key = key.to_vec();
        LastStoreKvStore::run_blocking(move || {
            for form in Self::read_forms(&key) {
                if let Some((value, collection, idx)) = Self::get_one_form(&store, &logical, &form)?
                {
                    dual_read_metrics::record_dual_read_get(Some(idx), Some(collection));
                    return Ok(Some(value));
                }
            }
            dual_read_metrics::record_dual_read_get(None, None);
            Ok(None)
        })
        .await
    }

    /// Batch point-read across the logical main store's candidate collections.
    ///
    /// Same first-candidate-wins semantics as [`Self::get`], but resolved in
    /// *rounds* rather than per key: round `r` asks each still-unresolved key's
    /// `r`-th candidate collection, grouping those keys by collection so each
    /// collection is hit with one [`LastStore::get_many`]. Keys resolved in an
    /// earlier round never reach a later one, so a hit in an earlier candidate
    /// still shadows a later one exactly as the sequential loop did.
    ///
    /// The previous implementation did a `get` per key per candidate
    /// collection. Every one of those resolved a shard handle and re-estimated
    /// warm-set residency, which is what made a few-hundred-key hydrate cost a
    /// few-hundred cold group loads under `HashGroup` layout.
    async fn get_many(&self, keys: Vec<Vec<u8>>) -> StorageResult<Vec<Option<Vec<u8>>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let store = Arc::clone(&self.store);
        let logical = Arc::clone(&self.logical);
        LastStoreKvStore::run_blocking(move || {
            let candidates: Vec<Vec<&'static str>> = keys
                .iter()
                .map(|key| main_collections_for_key(key))
                .collect();
            let max_rounds = candidates.iter().map(Vec::len).max().unwrap_or(0);

            let mut out: Vec<Option<Vec<u8>>> = vec![None; keys.len()];
            let mut unresolved: Vec<usize> = (0..keys.len()).collect();
            for round in 0..max_rounds {
                if unresolved.is_empty() {
                    break;
                }
                // Group this round's lookups by collection: one batched read
                // per collection instead of one `get` per key.
                let mut by_collection: BTreeMap<&'static str, Vec<usize>> = BTreeMap::new();
                for &slot in &unresolved {
                    if let Some(collection) = candidates[slot].get(round) {
                        by_collection.entry(collection).or_default().push(slot);
                    }
                }
                for (collection, slots) in by_collection {
                    let batch: Vec<Vec<u8>> =
                        slots.iter().map(|&slot| keys[slot].clone()).collect();
                    let bodies = logical_path::get_many(&store, &logical, collection, &batch)?;
                    for (slot, body) in slots.into_iter().zip(bodies) {
                        out[slot] = body;
                    }
                }
                unresolved.retain(|&slot| out[slot].is_none());
            }
            for slot in unresolved {
                let Some(twin) = Self::twin_bytes(&keys[slot]) else {
                    continue;
                };
                if let Some((value, collection, idx)) = Self::get_one_form(&store, &logical, &twin)?
                {
                    dual_read_metrics::record_dual_read_get(Some(idx), Some(collection));
                    out[slot] = Some(value);
                }
            }
            Ok(out)
        })
        .await
    }

    /// Write the canonical row, then displace any migration-era copy of it.
    ///
    /// Order is the crash guarantee — see the type docs. The deletes are a
    /// no-op on a post-cutover home (empty `converging_deletes`), so that home
    /// runs exactly the single `store.put` it ran before.
    async fn put(&self, key: &[u8], value: Vec<u8>) -> StorageResult<()> {
        let store = Arc::clone(&self.store);
        let logical = Arc::clone(&self.logical);
        let collection = main_collection_for_key(key).to_string();
        let superseded = self.converging_deletes(key);
        let key = key.to_vec();
        LastStoreKvStore::run_blocking(move || {
            logical_path::put(&store, &logical, &collection, &key, &value)?;
            for legacy in superseded {
                logical_path::delete(&store, &logical, legacy, &key)?;
            }
            Ok(())
        })
        .await
    }

    async fn delete(&self, key: &[u8]) -> StorageResult<bool> {
        let store = Arc::clone(&self.store);
        let logical = Arc::clone(&self.logical);
        let key = key.to_vec();
        LastStoreKvStore::run_blocking(move || {
            let mut existed = Self::delete_one_form(&store, &logical, &key)?;
            if let Some(twin) = Self::twin_bytes(&key) {
                existed |= Self::delete_one_form(&store, &logical, &twin)?;
            }
            Ok(existed)
        })
        .await
    }

    async fn exists(&self, key: &[u8]) -> StorageResult<bool> {
        let store = Arc::clone(&self.store);
        let logical = Arc::clone(&self.logical);
        let key = key.to_vec();
        LastStoreKvStore::run_blocking(move || {
            for form in Self::read_forms(&key) {
                if Self::exists_one_form(&store, &logical, &form)? {
                    return Ok(true);
                }
            }
            Ok(false)
        })
        .await
    }

    /// Batch existence probe across the logical main store's candidate
    /// collections, resolved in the same *rounds* as [`Self::get_many`]: round
    /// `r` asks each still-unfound key's `r`-th candidate collection, grouped so
    /// each collection is hit with one [`LastStore::exists_many`]. A key found
    /// in an earlier round never reaches a later one, matching the sequential
    /// short-circuit in [`Self::exists`].
    async fn exists_many(&self, keys: Vec<Vec<u8>>) -> StorageResult<Vec<bool>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let store = Arc::clone(&self.store);
        let logical = Arc::clone(&self.logical);
        LastStoreKvStore::run_blocking(move || {
            let candidates: Vec<Vec<&'static str>> = keys
                .iter()
                .map(|key| main_collections_for_key(key))
                .collect();
            let max_rounds = candidates.iter().map(Vec::len).max().unwrap_or(0);

            let mut out = vec![false; keys.len()];
            let mut unresolved: Vec<usize> = (0..keys.len()).collect();
            for round in 0..max_rounds {
                if unresolved.is_empty() {
                    break;
                }
                let mut by_collection: BTreeMap<&'static str, Vec<usize>> = BTreeMap::new();
                for &slot in &unresolved {
                    if let Some(collection) = candidates[slot].get(round) {
                        by_collection.entry(collection).or_default().push(slot);
                    }
                }
                for (collection, slots) in by_collection {
                    let batch: Vec<Vec<u8>> =
                        slots.iter().map(|&slot| keys[slot].clone()).collect();
                    let present = logical_path::exists_many(&store, &logical, collection, &batch)?;
                    for (slot, found) in slots.into_iter().zip(present) {
                        out[slot] = found;
                    }
                }
                unresolved.retain(|&slot| !out[slot]);
            }
            for slot in unresolved {
                let Some(twin) = Self::twin_bytes(&keys[slot]) else {
                    continue;
                };
                out[slot] = Self::exists_one_form(&store, &logical, &twin)?;
            }
            Ok(out)
        })
        .await
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let store = Arc::clone(&self.store);
        let logical = Arc::clone(&self.logical);
        let prefix = prefix.to_vec();
        LastStoreKvStore::run_blocking(move || {
            let mut rows = Self::walk_prefix_rows_with_twin(&store, &logical, &prefix, usize::MAX)?;
            Self::dedupe_and_sort_rows(&mut rows, true);
            Ok(rows)
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
        let logical = Arc::clone(&self.logical);
        let prefix = prefix.to_vec();
        LastStoreKvStore::run_blocking(move || {
            let mut keys = Self::walk_prefix_keys_with_twin(&store, &logical, &prefix, usize::MAX)?;
            Self::dedupe_and_sort_keys(&mut keys, true);
            Ok(keys)
        })
        .await
    }

    async fn scan_prefix_paged(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let store = Arc::clone(&self.store);
        let logical = Arc::clone(&self.logical);
        let prefix = prefix.to_vec();
        LastStoreKvStore::run_blocking(move || {
            let mut rows = Self::walk_prefix_rows_with_twin(&store, &logical, &prefix, limit)?;
            // Dedupe before truncating: a page must be filled with `limit`
            // distinct keys, not `limit` rows of which some are the same key.
            Self::dedupe_and_sort_rows(&mut rows, true);
            rows.truncate(limit);
            Ok(rows)
        })
        .await
    }

    async fn scan_range(&self, start: &[u8], end: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let store = Arc::clone(&self.store);
        let start = start.to_vec();
        let end = end.to_vec();
        LastStoreKvStore::run_blocking(move || {
            let mut rows = Vec::new();
            let collections = main_collections_for_range(&start, &end);
            let multi = collections.len() > 1;
            for collection in collections {
                rows.extend(LastStoreKvStore::range_rows_sync(
                    &store,
                    collection,
                    &start,
                    &end,
                    usize::MAX,
                )?);
            }
            Self::dedupe_and_sort_rows(&mut rows, multi);
            Ok(rows)
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
        let start = start.to_vec();
        let end = end.to_vec();
        LastStoreKvStore::run_blocking(move || {
            let mut rows = Vec::new();
            let collections = main_collections_for_range(&start, &end);
            let multi = collections.len() > 1;
            for collection in collections {
                rows.extend(LastStoreKvStore::range_rows_sync(
                    &store, collection, &start, &end, limit,
                )?);
            }
            Self::dedupe_and_sort_rows(&mut rows, multi);
            rows.truncate(limit);
            Ok(rows)
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
        let start = start.to_vec();
        let end = end.to_vec();
        let cursor = cursor.cloned();
        LastStoreKvStore::run_blocking(move || {
            physical_scan::scan_main_range(&store, &start, &end, cursor, limit, max_handles)
        })
        .await
    }

    /// Batch counterpart to [`Self::put`] — same displace-the-legacy-row
    /// contract, and the same ordering guarantee.
    ///
    /// Each key's put lands before that key's legacy deletes, matching the
    /// point-write order. A later failure restores every applied key to its
    /// previous body. The durability barrier syncs only the groups this batch
    /// wrote.
    async fn batch_put(&self, items: Vec<(Vec<u8>, Vec<u8>)>) -> StorageResult<()> {
        let store = Arc::clone(&self.store);
        let high_water = self.high_water.clone();
        let logical = Arc::clone(&self.logical);
        let deferred = self.deferred_batch_flush;
        let superseded: Vec<Vec<&'static str>> = items
            .iter()
            .map(|(key, _)| self.converging_deletes(key))
            .collect();
        LastStoreKvStore::run_blocking(move || {
            let mut ops = Vec::with_capacity(items.len());
            for ((key, value), legacy) in items.into_iter().zip(superseded) {
                ops.push(logical_path::BatchOp::Put {
                    collection: main_collection_for_key(&key).to_string(),
                    key: key.clone(),
                    value,
                });
                ops.extend(
                    legacy
                        .into_iter()
                        .map(|collection| logical_path::BatchOp::Delete {
                            collection: collection.to_string(),
                            key: key.clone(),
                        }),
                );
            }
            logical_path::apply_batch(&store, &logical, ops, deferred)?;
            LastStoreKvStore::record_high_water(high_water.as_ref(), &store)
        })
        .await
    }

    fn batch_put_has_durable_barrier(&self) -> bool {
        !self.deferred_batch_flush
    }

    async fn batch_mutate(&self, mutations: Vec<KvMutation>) -> StorageResult<()> {
        let store = Arc::clone(&self.store);
        let high_water = self.high_water.clone();
        let logical = Arc::clone(&self.logical);
        let deferred = self.deferred_batch_flush;
        let superseded: Vec<Vec<&'static str>> = mutations
            .iter()
            .map(|mutation| match mutation {
                KvMutation::Put { key, .. } => self.converging_deletes(key),
                KvMutation::Delete { .. } => Vec::new(),
            })
            .collect();
        LastStoreKvStore::run_blocking(move || {
            let mut ops = Vec::with_capacity(mutations.len());
            for (mutation, legacy) in mutations.into_iter().zip(superseded) {
                match mutation {
                    KvMutation::Put { key, value } => {
                        ops.push(logical_path::BatchOp::Put {
                            collection: main_collection_for_key(&key).to_string(),
                            key: key.clone(),
                            value,
                        });
                        ops.extend(legacy.into_iter().map(|collection| {
                            logical_path::BatchOp::Delete {
                                collection: collection.to_string(),
                                key: key.clone(),
                            }
                        }));
                    }
                    KvMutation::Delete { key } => {
                        let mut forms = vec![key.clone()];
                        if let Some(twin) = Self::twin_bytes(&key) {
                            forms.push(twin);
                        }
                        for form in forms {
                            for collection in main_collections_for_key(&form) {
                                ops.push(logical_path::BatchOp::Delete {
                                    collection: collection.to_string(),
                                    key: form.clone(),
                                });
                            }
                        }
                    }
                }
            }
            logical_path::apply_batch(&store, &logical, ops, deferred)?;
            LastStoreKvStore::record_high_water(high_water.as_ref(), &store)
        })
        .await
    }

    async fn restore_batch_put(&self, items: Vec<(Vec<u8>, Vec<u8>)>) -> StorageResult<()> {
        let store = Arc::clone(&self.store);
        let high_water = self.high_water.clone();
        let superseded: Vec<Vec<&'static str>> = items
            .iter()
            .map(|(key, _)| self.converging_deletes(key))
            .collect();
        let logical = Arc::clone(&self.logical);
        LastStoreKvStore::run_blocking(move || {
            let mut puts = Vec::with_capacity(items.len());
            let mut deletes = Vec::new();
            for ((key, value), legacy) in items.into_iter().zip(superseded) {
                let id = LastStoreKvStore::encode_key(&key);
                puts.push((main_collection_for_key(&key).to_string(), id.clone(), value));
                deletes.extend(
                    legacy
                        .into_iter()
                        .map(|collection| (collection, id.clone())),
                );
            }
            let ids: Vec<_> = puts.iter().map(|(_, id, _)| id.clone()).collect();
            direct_write_invalidation::after_direct_write(&logical, &ids, || {
                store
                    .restore_put_many_deferred(puts, 16)
                    .map_err(Self::map_error)?;
                for (collection, id) in deletes {
                    store.delete(collection, &id).map_err(Self::map_error)?;
                }
                Ok(())
            })?;
            LastStoreKvStore::record_high_water(high_water.as_ref(), &store)
        })
        .await
    }

    async fn batch_delete(&self, keys: Vec<Vec<u8>>) -> StorageResult<()> {
        let store = Arc::clone(&self.store);
        let high_water = self.high_water.clone();
        let logical = Arc::clone(&self.logical);
        let deferred = self.deferred_batch_flush;
        LastStoreKvStore::run_blocking(move || {
            let mut ops = Vec::new();
            for key in keys {
                let mut forms = vec![key.clone()];
                if let Some(twin) = Self::twin_bytes(&key) {
                    forms.push(twin);
                }
                for form in forms {
                    for collection in main_collections_for_key(&form) {
                        ops.push(logical_path::BatchOp::Delete {
                            collection: collection.to_string(),
                            key: form.clone(),
                        });
                    }
                }
            }
            logical_path::apply_batch(&store, &logical, ops, deferred)?;
            LastStoreKvStore::record_high_water(high_water.as_ref(), &store)
        })
        .await
    }

    async fn flush(&self) -> StorageResult<()> {
        let store = Arc::clone(&self.store);
        let high_water = self.high_water.clone();
        let logical = Arc::clone(&self.logical);
        LastStoreKvStore::run_blocking(move || {
            store.flush().map_err(Self::map_error)?;
            logical_path::apply_durable_through(&store, &logical);
            LastStoreKvStore::record_high_water(high_water.as_ref(), &store)
        })
        .await
    }

    fn backend_name(&self) -> &'static str {
        "laststore-logical-main"
    }

    fn execution_model(&self) -> ExecutionModel {
        ExecutionModel::SyncWrapped
    }

    fn flush_behavior(&self) -> FlushBehavior {
        FlushBehavior::Persists
    }
}
