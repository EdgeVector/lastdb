//! The `main` logical-plane [`KvStore`]: routes each key to its physical collection.
// lint:file-size-ok moved verbatim from its original module; one cohesive family per file

use super::*;

impl LogicalMainLastStoreKvStore {
    pub(super) fn with_logical(
        store: Arc<LastStore>,
        high_water: Option<Arc<LastStoreHighWaterFile>>,
        logical: Arc<Mutex<LogicalResidentSet>>,
    ) -> Self {
        Self {
            store,
            high_water,
            deferred_batch_flush: deferred_batch_flush_from_env(),
            logical,
            legacy_collections_on_disk: std::sync::OnceLock::new(),
        }
    }

    /// The legacy collections a write may have to displace a row from.
    ///
    /// Resolved on first use and cached; see the field docs for why that is
    /// safe. A `read_dir` failure resolves to "no legacy collections", which
    /// degrades to exactly today's non-converging write rather than to an
    /// error — reads stay correct either way, the rows just stay put.
    pub(super) fn legacy_collections_on_disk(&self) -> &std::collections::HashSet<String> {
        self.legacy_collections_on_disk.get_or_init(|| {
            let on_disk: std::collections::HashSet<String> = self
                .store
                .collections_on_disk()
                .unwrap_or_default()
                .into_iter()
                .collect();
            // Only the migration-era collections are candidates. The canonical
            // targets are where writes go and must never be deleted from.
            MAIN_KEY_PREFIX_COLLECTIONS
                .iter()
                .map(|(_, collection)| *collection)
                .chain(std::iter::once(LOGICAL_MAIN_COLLECTION))
                .filter(|collection| on_disk.contains(*collection))
                .map(str::to_string)
                .collect()
        })
    }

    /// Collections a write to `key` must delete the superseded row from.
    ///
    /// The first candidate is the canonical target — where the put goes — so
    /// only the tail is eligible, intersected with what the home carries.
    /// Empty on a post-cutover home, which is the fast path.
    pub(super) fn converging_deletes(&self, key: &[u8]) -> Vec<&'static str> {
        let on_disk = self.legacy_collections_on_disk();
        if on_disk.is_empty() {
            return Vec::new();
        }
        main_collections_for_key(key)
            .into_iter()
            .skip(1)
            .filter(|collection| on_disk.contains(*collection))
            .collect()
    }

    pub(super) fn map_error(error: laststore::Error) -> StorageError {
        LastStoreKvStore::map_error(error)
    }

    /// Collapse a multi-collection walk into one row per key, then order it.
    ///
    /// A key that exists in both the canonical and a legacy collection is one
    /// logical row, and `get` says so — it returns the first candidate, which
    /// is canonical. A scan that concatenated the collections would instead
    /// report the key twice, disagreeing with the point read about the same
    /// key and, worse, spending a *paged* scan's limit on the duplicate. That
    /// is how a page of 100 comes back holding 26 distinct records and how a
    /// count taken from a scan exceeds the enumeration that follows it.
    ///
    /// Rows arrive in candidate order, so keeping the first occurrence of each
    /// key gives scans exactly the precedence `get` already has.
    ///
    /// `multi_collection` is false when only one collection was walked, where
    /// duplicates are impossible and the dedupe would be pure cost.
    ///
    /// Costs no extra allocation. `slice::sort_by` is a **stable** sort, so
    /// equal keys keep the order they arrived in — candidate-collection order —
    /// and `dedup_by` keeps the first of each run. Preferring canonical falls
    /// out of that, with no per-key set to hold on a scan that may already be
    /// carrying millions of rows.
    pub(super) fn dedupe_and_sort_rows(rows: &mut Vec<(Vec<u8>, Vec<u8>)>, multi_collection: bool) {
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        if multi_collection {
            rows.dedup_by(|a, b| a.0 == b.0);
        }
    }

    /// Keys-only counterpart to [`Self::dedupe_and_sort_rows`].
    pub(super) fn dedupe_and_sort_keys(keys: &mut Vec<Vec<u8>>, multi_collection: bool) {
        keys.sort();
        if multi_collection {
            keys.dedup();
        }
    }

    pub(super) fn get_one_form(
        store: &LastStore,
        set: &Mutex<LogicalResidentSet>,
        key: &[u8],
    ) -> StorageResult<Option<(Vec<u8>, &'static str, usize)>> {
        let collections = main_collections_for_key(key);
        for (idx, collection) in collections.iter().enumerate() {
            if let Some(value) = logical_path::get(store, set, collection, key)? {
                return Ok(Some((value, collection, idx)));
            }
        }
        Ok(None)
    }

    pub(super) fn exists_one_form(
        store: &LastStore,
        set: &Mutex<LogicalResidentSet>,
        key: &[u8],
    ) -> StorageResult<bool> {
        let collections = main_collections_for_key(key);
        for collection in collections {
            if logical_path::exists(store, set, collection, key)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(super) fn delete_one_form(
        store: &LastStore,
        set: &Mutex<LogicalResidentSet>,
        key: &[u8],
    ) -> StorageResult<bool> {
        let collections = main_collections_for_key(key);
        let mut existed = false;
        for collection in collections {
            existed |= logical_path::delete(store, set, collection, key)?;
        }
        Ok(existed)
    }

    pub(super) fn twin_bytes(key: &[u8]) -> Option<Vec<u8>> {
        let key = std::str::from_utf8(key).ok()?;
        Some(form_twin(key)?.into_bytes())
    }

    /// Point-read order: the anchored write form first, then the colon tail.
    pub(super) fn read_forms(key: &[u8]) -> Vec<Vec<u8>> {
        let Some(twin) = Self::twin_bytes(key) else {
            return vec![key.to_vec()];
        };
        let Ok(text) = std::str::from_utf8(key) else {
            return vec![key.to_vec()];
        };
        if text.contains('\0') && form_twin(text).is_some() {
            vec![key.to_vec(), twin]
        } else {
            vec![twin, key.to_vec()]
        }
    }

    pub(super) fn prefix_rows_all_collections(
        store: &LastStore,
        set: &Mutex<LogicalResidentSet>,
        prefix: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut rows = Vec::new();
        for collection in main_collections_for_prefix(prefix) {
            rows.extend(logical_path::scan_prefix(
                store, set, collection, prefix, limit,
            )?);
        }
        Ok(rows)
    }

    pub(super) fn prefix_keys_all_collections(
        store: &LastStore,
        set: &Mutex<LogicalResidentSet>,
        prefix: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<Vec<u8>>> {
        let mut keys = Vec::new();
        for collection in main_collections_for_prefix(prefix) {
            keys.extend(logical_path::scan_prefix_keys(
                store, set, collection, prefix, limit,
            )?);
        }
        Ok(keys)
    }

    pub(super) fn merge_kind_twin_rows(
        requested_prefix: &[u8],
        primary: Vec<(Vec<u8>, Vec<u8>)>,
        twin: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let like = std::str::from_utf8(requested_prefix).unwrap_or("");
        let mut seen = std::collections::BTreeSet::new();
        let mut out = Vec::with_capacity(primary.len() + twin.len());
        for (key, value) in primary {
            if let Ok(text) = std::str::from_utf8(&key) {
                seen.insert(logical_row_id(text));
            }
            out.push((key, value));
        }
        for (key, value) in twin {
            let Ok(text) = std::str::from_utf8(&key) else {
                continue;
            };
            if !seen.insert(logical_row_id(text)) {
                continue;
            }
            out.push((rewrite_key_like(text, like).into_bytes(), value));
        }
        out
    }

    pub(super) fn merge_kind_twin_keys(
        requested_prefix: &[u8],
        primary: Vec<Vec<u8>>,
        twin: Vec<Vec<u8>>,
    ) -> Vec<Vec<u8>> {
        let like = std::str::from_utf8(requested_prefix).unwrap_or("");
        let mut seen = std::collections::BTreeSet::new();
        let mut out = Vec::with_capacity(primary.len() + twin.len());
        for key in primary {
            if let Ok(text) = std::str::from_utf8(&key) {
                seen.insert(logical_row_id(text));
            }
            out.push(key);
        }
        for key in twin {
            let Ok(text) = std::str::from_utf8(&key) else {
                continue;
            };
            if !seen.insert(logical_row_id(text)) {
                continue;
            }
            out.push(rewrite_key_like(text, like).into_bytes());
        }
        out
    }

    pub(super) fn walk_prefix_rows_with_twin(
        store: &LastStore,
        set: &Mutex<LogicalResidentSet>,
        prefix: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let primary = Self::prefix_rows_all_collections(store, set, prefix, limit)?;
        let Some(twin) = Self::twin_bytes(prefix) else {
            return Ok(primary);
        };
        let extra = Self::prefix_rows_all_collections(store, set, &twin, limit)?;
        Ok(Self::merge_kind_twin_rows(prefix, primary, extra))
    }

    pub(super) fn walk_prefix_keys_with_twin(
        store: &LastStore,
        set: &Mutex<LogicalResidentSet>,
        prefix: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<Vec<u8>>> {
        let primary = Self::prefix_keys_all_collections(store, set, prefix, limit)?;
        let Some(twin) = Self::twin_bytes(prefix) else {
            return Ok(primary);
        };
        let extra = Self::prefix_keys_all_collections(store, set, &twin, limit)?;
        Ok(Self::merge_kind_twin_keys(prefix, primary, extra))
    }
}

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

    // lint:fn-size-ok moved verbatim from its original module; no logic change
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
            let collections = main_collections_for_range(&start, &end);
            if collections.is_empty() || limit == 0 || max_handles == 0 {
                return Ok(PhysicalScanPage {
                    next_cursor: cursor,
                    ..Default::default()
                });
            }
            let start_collection = cursor
                .as_ref()
                .and_then(|position| position.collection.as_deref())
                .and_then(|wanted| collections.iter().position(|name| *name == wanted))
                .unwrap_or(0);
            let start_id = LastStoreKvStore::encode_bound(&start);
            let end_id = LastStoreKvStore::encode_bound(&end);
            let mut rows = Vec::new();
            let mut handles_left = max_handles;
            let mut handles_visited = 0u64;
            let mut cold_shard_loads = 0u64;
            let mut row_handle = None;

            for (offset, collection) in collections[start_collection..].iter().enumerate() {
                let collection_index = start_collection + offset;
                let vendor_cursor = cursor
                    .as_ref()
                    .filter(|position| position.collection.as_deref() == Some(*collection))
                    .map(|position| laststore::PhysicalRangeCursor {
                        shard: position.shard,
                        group_id: position.group_id,
                        after_id: position
                            .after_key
                            .as_deref()
                            .map(LastStoreKvStore::encode_key),
                    });
                let page = if start_id.is_empty() {
                    store.walk_all_groups_at_startup(
                        collection,
                        vendor_cursor.as_ref(),
                        limit - rows.len(),
                        handles_left,
                    )
                } else {
                    store.walk_all_groups(
                        collection,
                        start_id.as_str()..end_id.as_str(),
                        vendor_cursor.as_ref(),
                        limit - rows.len(),
                        handles_left,
                        laststore::AllGroupsPurpose::Admin,
                    )
                }
                .map_err(LastStoreKvStore::map_error)?;
                handles_visited = handles_visited.saturating_add(page.handles_visited);
                cold_shard_loads = cold_shard_loads.saturating_add(page.cold_shard_loads);
                handles_left = handles_left.saturating_sub(page.handles_visited as usize);
                if let Some((shard, group_id)) = page.row_handle {
                    row_handle = Some(PhysicalScanCursor {
                        collection: Some((*collection).to_string()),
                        shard,
                        group_id,
                        after_key: None,
                    });
                }
                rows.extend(LastStoreKvStore::decode_rows(page.rows)?);
                if let Some(position) = page.next_cursor {
                    return Ok(PhysicalScanPage {
                        rows,
                        next_cursor: Some(PhysicalScanCursor {
                            collection: Some((*collection).to_string()),
                            shard: position.shard,
                            group_id: position.group_id,
                            after_key: position
                                .after_id
                                .map(|id| LastStoreKvStore::decode_key(&id))
                                .transpose()?,
                        }),
                        row_handle,
                        handles_visited,
                        cold_shard_loads,
                    });
                }
                if rows.len() >= limit || handles_left == 0 {
                    let next_cursor =
                        collections
                            .get(collection_index + 1)
                            .map(|next| PhysicalScanCursor {
                                collection: Some((**next).to_string()),
                                ..Default::default()
                            });
                    return Ok(PhysicalScanPage {
                        rows,
                        next_cursor,
                        row_handle,
                        handles_visited,
                        cold_shard_loads,
                    });
                }
            }

            Ok(PhysicalScanPage {
                rows,
                next_cursor: None,
                row_handle,
                handles_visited,
                cold_shard_loads,
            })
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
            direct_write_invalidation::after_direct_write(&logical, || {
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
