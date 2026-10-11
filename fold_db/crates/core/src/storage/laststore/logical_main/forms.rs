use super::*;

impl LogicalMainLastStoreKvStore {
    pub(in crate::storage::laststore) fn with_logical(
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
    pub(in crate::storage::laststore) fn legacy_collections_on_disk(
        &self,
    ) -> &std::collections::HashSet<String> {
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
    pub(in crate::storage::laststore) fn converging_deletes(
        &self,
        key: &[u8],
    ) -> Vec<&'static str> {
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

    pub(in crate::storage::laststore) fn map_error(error: laststore::Error) -> StorageError {
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
    pub(in crate::storage::laststore) fn dedupe_and_sort_rows(
        rows: &mut Vec<(Vec<u8>, Vec<u8>)>,
        multi_collection: bool,
    ) {
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        if multi_collection {
            rows.dedup_by(|a, b| a.0 == b.0);
        }
    }

    /// Keys-only counterpart to [`Self::dedupe_and_sort_rows`].
    pub(in crate::storage::laststore) fn dedupe_and_sort_keys(
        keys: &mut Vec<Vec<u8>>,
        multi_collection: bool,
    ) {
        keys.sort();
        if multi_collection {
            keys.dedup();
        }
    }

    pub(in crate::storage::laststore) fn get_one_form(
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

    pub(in crate::storage::laststore) fn exists_one_form(
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

    pub(in crate::storage::laststore) fn delete_one_form(
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

    pub(in crate::storage::laststore) fn twin_bytes(key: &[u8]) -> Option<Vec<u8>> {
        let key = std::str::from_utf8(key).ok()?;
        Some(form_twin(key)?.into_bytes())
    }

    /// Point-read order: the anchored write form first, then the colon tail.
    pub(in crate::storage::laststore) fn read_forms(key: &[u8]) -> Vec<Vec<u8>> {
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

    pub(in crate::storage::laststore) fn prefix_rows_all_collections(
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

    pub(in crate::storage::laststore) fn prefix_keys_all_collections(
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

    pub(in crate::storage::laststore) fn merge_kind_twin_rows(
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

    pub(in crate::storage::laststore) fn merge_kind_twin_keys(
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

    pub(in crate::storage::laststore) fn walk_prefix_rows_with_twin(
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

    pub(in crate::storage::laststore) fn walk_prefix_keys_with_twin(
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
