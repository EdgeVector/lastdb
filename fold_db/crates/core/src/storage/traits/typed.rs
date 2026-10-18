//! `TypedKvStore`: JSON serialize/deserialize helper over any `KvStore`.

use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;

use super::*;

/// Adapter that wraps a KvStore and provides high-level typed storage
/// operations
///
/// This provides a convenient API on top of KvStore for storing/retrieving
/// typed Rust structs (serialized as JSON or bincode)
pub struct TypedKvStore<S: KvStore + ?Sized> {
    inner: Arc<S>,
}

impl<S: KvStore + ?Sized> TypedKvStore<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self { inner: store }
    }

    /// Get a reference to the underlying KvStore
    pub fn inner(&self) -> &Arc<S> {
        &self.inner
    }
}

impl<S: KvStore + ?Sized + 'static> TypedKvStore<S> {
    /// Decode raw `(key, value)` rows into `(String, T)` items.
    /// Shared by all scan helpers so we don't maintain four copy-pasted loops.
    /// [`Self::scan_items_with_prefix_paged`], but one undecodable row does not
    /// fail the page. Returns the decoded rows and the keys of the rows that
    /// did not decode. A reaper uses it so one damaged queue record cannot
    /// stop the whole maintenance page.
    pub async fn scan_items_with_prefix_paged_lenient<T: DeserializeOwned + Send + Sync>(
        &self,
        prefix: &str,
        limit: usize,
    ) -> StorageResult<(Vec<(String, T)>, Vec<String>)> {
        let rows = self
            .inner
            .scan_prefix_paged(prefix.as_bytes(), limit)
            .await?;
        let mut items = Vec::with_capacity(rows.len());
        let mut undecodable = Vec::new();
        for (key_bytes, value_bytes) in rows {
            let key = String::from_utf8_lossy(&key_bytes).into_owned();
            match serde_json::from_slice(&value_bytes) {
                Ok(value) => items.push((key, value)),
                Err(_) => undecodable.push(key),
            }
        }
        Ok((items, undecodable))
    }

    fn decode_rows<T: DeserializeOwned>(
        results: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> StorageResult<Vec<(String, T)>> {
        let mut items = Vec::with_capacity(results.len());
        for (key_bytes, value_bytes) in results {
            let key = String::from_utf8_lossy(&key_bytes).into_owned();
            let value = serde_json::from_slice(&value_bytes).map_err(|e| {
                StorageError::SerializationError(format!("Failed to deserialize {key}: {e}"))
            })?;
            items.push((key, value));
        }
        Ok(items)
    }

    /// Store a typed item
    pub async fn put_item<T: Serialize + Send + Sync>(
        &self,
        key: &str,
        item: &T,
    ) -> StorageResult<()> {
        let bytes = serde_json::to_vec(item)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;
        self.inner.put(key.as_bytes(), bytes).await
    }

    /// Get a typed item
    pub async fn get_item<T: DeserializeOwned + Send + Sync>(
        &self,
        key: &str,
    ) -> StorageResult<Option<T>> {
        let mut raw = self.inner.get(key.as_bytes()).await?;
        if raw.is_none() {
            if let Some(alias) = crate::kind_partition::form_twin(key) {
                raw = self.inner.get(alias.as_bytes()).await?;
            }
        }
        raw.map(|bytes| {
            serde_json::from_slice(&bytes)
                .map_err(|error| StorageError::SerializationError(error.to_string()))
        })
        .transpose()
    }

    /// Get multiple typed items, preserving input order.
    ///
    /// Read exact keys first. A key returned under a broad kind prefix can
    /// name the other current flat layout, so resolve that alias only on a miss.
    pub async fn get_items<T: DeserializeOwned + Send + Sync>(
        &self,
        keys: &[String],
    ) -> StorageResult<Vec<Option<T>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let primary = keys.iter().map(|key| key.as_bytes().to_vec()).collect();
        let mut raw = self.inner.get_many(primary).await?;
        let (missing, aliases) =
            missing_aliases(keys, |index| raw.get(index).is_some_and(Option::is_some));
        if !aliases.is_empty() {
            let alias_values = self.inner.get_many(aliases).await?;
            for (index, value) in missing.into_iter().zip(alias_values) {
                if let Some(slot) = raw.get_mut(index) {
                    *slot = value;
                }
            }
        }
        let mut out = Vec::with_capacity(keys.len());
        for (key, bytes) in keys.iter().zip(raw) {
            match bytes {
                Some(bytes) => {
                    let item = serde_json::from_slice(&bytes).map_err(|e| {
                        StorageError::SerializationError(format!(
                            "Failed to deserialize {key}: {e}"
                        ))
                    })?;
                    out.push(Some(item));
                }
                None => out.push(None),
            }
        }
        Ok(out)
    }

    /// Delete an item
    pub async fn delete_item(&self, key: &str) -> StorageResult<bool> {
        if self.inner.delete(key.as_bytes()).await? {
            return Ok(true);
        }
        match crate::kind_partition::form_twin(key) {
            Some(alias) => self.inner.delete(alias.as_bytes()).await,
            None => Ok(false),
        }
    }

    /// List all keys with a given prefix
    pub async fn list_keys_with_prefix(&self, prefix: &str) -> StorageResult<Vec<String>> {
        let keys = self.inner.scan_prefix_keys(prefix.as_bytes()).await?;
        Ok(keys
            .into_iter()
            .map(|k| String::from_utf8_lossy(&k).into_owned())
            .collect())
    }

    /// Get all items with a given prefix
    pub async fn scan_items_with_prefix<T: DeserializeOwned + Send + Sync>(
        &self,
        prefix: &str,
    ) -> StorageResult<Vec<(String, T)>> {
        let rows = self.inner.scan_prefix(prefix.as_bytes()).await?;
        Self::decode_rows(rows)
    }

    /// Scan a prefix, **partitioning** rows into those that deserialize cleanly
    /// and the keys of rows whose value is present but not decodable as `T`.
    ///
    /// This is the typed-layer analogue of
    /// [`scan_prefix_partition_undecryptable`](KvStore::scan_prefix_partition_undecryptable):
    /// the non-aborting, *reporting* counterpart to
    /// [`Self::scan_items_with_prefix`], which fails the whole scan on the first
    /// undecodable row. That layer partitions on **decrypt** failure; this one
    /// partitions on **deserialize** failure, which is a distinct fault (an
    /// empty row, a legacy `ENC:` value read through a plain seam, or a
    /// truncated segment all decrypt fine and then fail `serde_json`).
    ///
    /// **Won't-undo — this must stay opt-in, per call site.** Do not "fix" an
    /// aborting scan by making [`Self::decode_rows`] skip bad rows globally: a
    /// scan that silently returns fewer rows than exist is *selective record
    /// hiding*, the exact shape of a read-integrity defect where callers are
    /// served short with a `200 OK`. Callers of this method must surface what
    /// was skipped — count it, log the keys, or both — never drop it quietly.
    ///
    /// Same fail-open/fail-closed rule as the decrypt-layer method: only use
    /// this on namespaces that are **re-derivable**, where a temporarily missing
    /// entry is recoverable. Never use it for capability, ACL, or revocation
    /// records, where a dropped row silently grants access and must fail CLOSED.
    pub async fn scan_items_with_prefix_partition_undecodable<T: DeserializeOwned + Send + Sync>(
        &self,
        prefix: &str,
    ) -> StorageResult<PartitionedItems<T>> {
        // Catalog boot walks the whole namespace with prefix "". Under
        // LASTDB_READS_REQUIRE_PARTITION that is an UnanchoredRead on
        // `scan_prefix`. `scan_prefix_partition_undecryptable` is the
        // startup/admin walk (walk_all_groups when the prefix is empty).
        let scan = self
            .inner
            .scan_prefix_partition_undecryptable(prefix.as_bytes())
            .await?;
        let mut items = Vec::with_capacity(scan.rows.len());
        let mut undecodable = Vec::with_capacity(scan.undecryptable.len());
        for key_bytes in scan.undecryptable {
            let key = String::from_utf8_lossy(&key_bytes).into_owned();
            undecodable.push((key, "undecryptable at-rest row".to_string()));
        }
        for (key_bytes, value_bytes) in scan.rows {
            let key = String::from_utf8_lossy(&key_bytes).into_owned();
            match serde_json::from_slice(&value_bytes) {
                Ok(value) => items.push((key, value)),
                Err(e) => undecodable.push((key, e.to_string())),
            }
        }
        Ok(PartitionedItems { items, undecodable })
    }

    /// Get at most `limit` items with a given prefix, in ascending key order —
    /// the bounded, paginated counterpart to [`Self::scan_items_with_prefix`].
    /// Backs a paginated list query so the read no longer materializes the whole
    /// prefix; see [`KvStore::scan_prefix_paged`] for the boundedness contract.
    pub async fn scan_items_with_prefix_paged<T: DeserializeOwned + Send + Sync>(
        &self,
        prefix: &str,
        limit: usize,
    ) -> StorageResult<Vec<(String, T)>> {
        let rows = self
            .inner
            .scan_prefix_paged(prefix.as_bytes(), limit)
            .await?;
        Self::decode_rows(rows)
    }

    /// Get all items whose key falls in the half-open range `start..end`
    /// (inclusive `start`, exclusive `end`), in ascending key order. The
    /// bounded counterpart to [`Self::scan_items_with_prefix`]; see
    /// [`KvStore::scan_range`] for the range semantics.
    pub async fn scan_items_in_range<T: DeserializeOwned + Send + Sync>(
        &self,
        start: &str,
        end: &str,
    ) -> StorageResult<Vec<(String, T)>> {
        Self::decode_rows(
            self.inner
                .scan_range(start.as_bytes(), end.as_bytes())
                .await?,
        )
    }

    /// Get at most `limit` items whose key falls in `start..end`, in ascending
    /// key order. See [`KvStore::scan_range_paged`] for the boundedness
    /// contract.
    pub async fn scan_items_in_range_paged<T: DeserializeOwned + Send + Sync>(
        &self,
        start: &str,
        end: &str,
        limit: usize,
    ) -> StorageResult<Vec<(String, T)>> {
        Self::decode_rows(
            self.inner
                .scan_range_paged(start.as_bytes(), end.as_bytes(), limit)
                .await?,
        )
    }

    /// Batch store items
    pub async fn batch_put_items<T: Serialize + Send + Sync>(
        &self,
        items: Vec<(String, T)>,
    ) -> StorageResult<()> {
        let serialized: Result<Vec<_>, StorageError> = items
            .into_iter()
            .map(|(k, v)| {
                let bytes = serde_json::to_vec(&v)
                    .map_err(|e| StorageError::SerializationError(e.to_string()))?;
                Ok::<(Vec<u8>, Vec<u8>), StorageError>((k.into_bytes(), bytes))
            })
            .collect();

        self.inner.batch_put(serialized?).await
    }

    /// Batch delete keys
    pub async fn batch_delete_keys(&self, keys: Vec<String>) -> StorageResult<()> {
        let mut selected: Vec<Vec<u8>> = keys.iter().map(|key| key.as_bytes().to_vec()).collect();
        let aliased: Vec<_> = keys
            .iter()
            .enumerate()
            .filter_map(|(index, key)| {
                crate::kind_partition::form_twin(key).map(|alias| (index, alias.into_bytes()))
            })
            .collect();
        if !aliased.is_empty() {
            let exact = aliased
                .iter()
                .map(|(index, _)| selected[*index].clone())
                .collect();
            let present = self.inner.exists_many(exact).await?;
            let missing: Vec<_> = aliased
                .into_iter()
                .zip(present)
                .filter(|(_, present)| !present)
                .map(|(entry, _)| entry)
                .collect();
            if !missing.is_empty() {
                let aliases = missing.iter().map(|(_, alias)| alias.clone()).collect();
                let present = self.inner.exists_many(aliases).await?;
                for ((index, alias), present) in missing.into_iter().zip(present) {
                    if present {
                        selected[index] = alias;
                    }
                }
            }
        }
        self.inner.batch_delete(selected).await
    }

    /// Check if key exists
    pub async fn exists_item(&self, key: &str) -> StorageResult<bool> {
        if self.inner.exists(key.as_bytes()).await? {
            return Ok(true);
        }
        match crate::kind_partition::form_twin(key) {
            Some(alias) => self.inner.exists(alias.as_bytes()).await,
            None => Ok(false),
        }
    }

    /// Whether each key exists, preserving input order. See
    /// [`KvStore::exists_many`] for why this is not a `get_items` in disguise.
    pub async fn exists_items(&self, keys: &[String]) -> StorageResult<Vec<bool>> {
        let raw_keys = keys.iter().map(|key| key.as_bytes().to_vec()).collect();
        let mut present = self.inner.exists_many(raw_keys).await?;
        let (missing, aliases) = missing_aliases(keys, |index| present.get(index) == Some(&true));
        if !aliases.is_empty() {
            let alias_present = self.inner.exists_many(aliases).await?;
            for (index, value) in missing.into_iter().zip(alias_present) {
                if let Some(slot) = present.get_mut(index) {
                    *slot = value;
                }
            }
        }
        Ok(present)
    }
}

/// Prefix results preserve the requested spelling. Flat keys can therefore
/// return through their other current layout; compound keys have no alias.
fn missing_aliases(keys: &[String], present: impl Fn(usize) -> bool) -> (Vec<usize>, Vec<Vec<u8>>) {
    let mut missing = Vec::new();
    let mut aliases = Vec::new();
    for (index, key) in keys.iter().enumerate() {
        if !present(index) {
            if let Some(alias) = crate::kind_partition::form_twin(key) {
                missing.push(index);
                aliases.push(alias.into_bytes());
            }
        }
    }
    (missing, aliases)
}
