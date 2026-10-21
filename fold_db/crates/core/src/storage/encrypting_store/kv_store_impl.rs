//! The `KvStore` trait implementation for `EncryptingKvStore`.

use super::fill::PagedSource;
use super::*;
use crate::storage::traits::YieldedScan;

#[async_trait]
impl KvStore for EncryptingKvStore {
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        match self.inner.get(key).await? {
            Some(stored) => self.open_or_discard(key, stored).await,
            None => Ok(None),
        }
    }

    /// Decrypt one physical batch without a second cache of logical values.
    async fn get_many(&self, keys: Vec<Vec<u8>>) -> StorageResult<Vec<Option<Vec<u8>>>> {
        let stored = self.inner.get_many(keys.clone()).await?;
        let mut values = Vec::with_capacity(keys.len());
        for (key, stored) in keys.into_iter().zip(stored) {
            let opened = match stored {
                Some(stored) => match self.open_or_discard(&key, stored).await {
                    Ok(opened) => opened,
                    Err(error) if Self::lossy_derived_key(&key) => {
                        tracing::warn!(error = %error, "get_many: skipping undecryptable derived-index row");
                        None
                    }
                    Err(error) => return Err(error),
                },
                None => None,
            };
            values.push(opened);
        }
        Ok(values)
    }

    async fn put(&self, key: &[u8], value: Vec<u8>) -> StorageResult<()> {
        let sealed = self.seal_value(key, &value).await?;
        self.inner.put(key, sealed).await
    }

    async fn delete(&self, key: &[u8]) -> StorageResult<bool> {
        self.inner.delete(key).await
    }

    async fn exists(&self, key: &[u8]) -> StorageResult<bool> {
        self.inner.exists(key).await
    }

    /// Existence checks do not decrypt values and retain one batch call.
    async fn exists_many(&self, keys: Vec<Vec<u8>>) -> StorageResult<Vec<bool>> {
        self.inner.exists_many(keys).await
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let results = self.inner.scan_prefix(prefix).await?;
        self.decrypt_rows(results).await
    }

    async fn scan_prefix_keys(&self, prefix: &[u8]) -> StorageResult<Vec<Vec<u8>>> {
        // Keys are plaintext; never decrypt values just to count/seq-seed.
        self.inner.scan_prefix_keys(prefix).await
    }

    async fn scan_prefix_paged(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        // Keys are plaintext at this layer (only values are encrypted), so the
        // prefix bound applies before decryption. Forward to the inner store's
        // paged scan so its native lazy bound (Sled) holds — we then decrypt only
        // the page's rows, not the whole prefix. A row with no envelope is
        // dropped and the page is refilled, so a short page is the range end.
        self.scan_paged_filled(PagedSource::Prefix(prefix), limit)
            .await
    }

    async fn scan_range(&self, start: &[u8], end: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        // Keep the range path symmetric with the explicit prefix/paged range
        // overrides: bounds apply to plaintext keys at the inner layer, then
        // this decorator decrypts only the matching rows.
        let results = self.inner.scan_range(start, end).await?;
        self.decrypt_rows(results).await
    }

    async fn scan_range_paged(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        // Keys are plaintext at this layer (only values are encrypted), so the
        // range bound applies before decryption. Forward to the inner store's
        // paged range scan so native lazy bounds hold. A row with no envelope
        // is dropped and the page is refilled, so a short page is the range end.
        self.scan_paged_filled(PagedSource::Range(start, end), limit)
            .await
    }

    /// The inner store names where the next read continues, from its raw rows,
    /// so a row dropped for want of an envelope does not change it.
    async fn scan_range_yielding(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        keys_per_row: usize,
    ) -> StorageResult<YieldedScan> {
        let raw = self
            .inner
            .scan_range_yielding(start, end, limit, keys_per_row)
            .await?;
        let rows = self.decrypt_rows(raw.rows).await?;
        Ok(YieldedScan {
            rows,
            resume: raw.resume,
        })
    }

    async fn max_key_u64_after_marker(
        &self,
        prefix: &[u8],
        marker: &[u8],
    ) -> StorageResult<Option<u64>> {
        // Keys stay plaintext at this decorator. The fold needs no value
        // hydration or decryption.
        self.inner.max_key_u64_after_marker(prefix, marker).await
    }

    async fn scan_range_physical_paged(
        &self,
        start: &[u8],
        end: &[u8],
        cursor: Option<&PhysicalScanCursor>,
        limit: usize,
        max_handles: usize,
    ) -> StorageResult<PhysicalScanPage> {
        let page = self
            .inner
            .scan_range_physical_paged(start, end, cursor, limit, max_handles)
            .await?;
        Ok(PhysicalScanPage {
            rows: self.decrypt_rows(page.rows).await?,
            next_cursor: page.next_cursor,
            row_handle: page.row_handle,
            handles_visited: page.handles_visited,
            cold_shard_loads: page.cold_shard_loads,
        })
    }

    // SECURITY (reviewer note): this lossy scan silently DROPS any undecryptable
    // row. That is safe ONLY for DERIVED, re-derivable indices — the sole caller
    // is the `emb:` embeddings index, rebuilt by re-embedding. Do NOT reuse it
    // over a primary / security-critical namespace (consent, the revocation
    // ledger, capabilities, ACLs): there a dropped row = selective record hiding
    // (an unreadable revocation row would silently un-revoke a capability — fail
    // OPEN). Those namespaces must fail CLOSED on an unreadable row instead (cf.
    // `fold_db_node`'s `RevocationStore::read_all`). See the trait-level note on
    // `KvStore::scan_prefix_lossy` for the allow-list rule.
    async fn scan_prefix_lossy(&self, prefix: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let results = self.inner.scan_prefix(prefix).await?;
        let mut decrypted_results = Vec::with_capacity(results.len());
        let mut skipped = 0usize;

        for (key, stored) in results {
            match self.open_or_discard(&key, stored).await {
                Ok(Some(plaintext)) => {
                    decrypted_results.push((key, plaintext));
                }
                // No envelope: absent, already counted and warned once.
                Ok(None) => {}
                Err(e) => {
                    // One undecryptable row must not fail the whole scan: this
                    // path backs a derived index that can be rebuilt. Skip it
                    // with a warning rather than wedging the caller (e.g. node
                    // startup restoring the embedding index).
                    skipped += 1;
                    tracing::warn!(
                        error = %e,
                        "scan_prefix_lossy: skipping undecryptable row (derived index, will be re-derived on next index)"
                    );
                }
            }
        }

        if skipped > 0 {
            tracing::warn!(
                skipped,
                kept = decrypted_results.len(),
                "scan_prefix_lossy: skipped {skipped} undecryptable row(s)"
            );
        }

        Ok(decrypted_results)
    }

    // Non-aborting, *reporting* scan: unlike `scan_prefix` (fails the whole
    // scan on the first poison row) and `scan_prefix_lossy` (silently drops
    // them), this returns the cleanly-decrypted rows AND the keys of every row
    // it could not decrypt, so the caller can report them loudly (snapshot
    // backup) or scrub them (maintenance delete). It never re-derives or
    // withholds a good row — the partition is exact.
    async fn scan_prefix_partition_undecryptable(
        &self,
        prefix: &[u8],
    ) -> StorageResult<PartitionedScan> {
        // An empty prefix is a startup/admin walk of the whole namespace
        // (boot decrypt proof, snapshot scrub). Product `scan_prefix("")`
        // still goes through the LastStore partition guard and rejects.
        // Page the explicit all-groups primitive instead.
        let results = if prefix.is_empty() {
            let mut rows = Vec::new();
            let mut cursor = None;
            loop {
                let page = self
                    .inner
                    .scan_range_physical_paged(
                        &[],
                        RESEAL_COLLECTION_END,
                        cursor.as_ref(),
                        RESEAL_PAGE_ROWS,
                        RESEAL_PAGE_HANDLES,
                    )
                    .await?;
                rows.extend(page.rows);
                match page.next_cursor {
                    Some(next) => cursor = Some(next),
                    None => break,
                }
            }
            rows
        } else {
            self.inner.scan_prefix(prefix).await?
        };
        let mut rows = Vec::with_capacity(results.len());
        let mut undecryptable = Vec::new();

        for (key, stored) in results {
            match self.open_or_discard(&key, stored).await {
                Ok(Some(plaintext)) => {
                    rows.push((key, plaintext));
                }
                // An un-enveloped row is absent, not undecryptable: it is not a
                // wrong-key row awaiting repair, so it is not reported for one.
                Ok(None) => {}
                Err(_) => undecryptable.push(key),
            }
        }

        Ok(PartitionedScan {
            rows,
            undecryptable,
        })
    }

    async fn batch_put(&self, items: Vec<(Vec<u8>, Vec<u8>)>) -> StorageResult<()> {
        let mut encrypted_items = Vec::with_capacity(items.len());
        for (key, value) in items {
            let sealed = self.seal_value(&key, &value).await?;
            encrypted_items.push((key, sealed));
        }
        self.inner.batch_put(encrypted_items).await
    }

    fn batch_put_has_durable_barrier(&self) -> bool {
        self.inner.batch_put_has_durable_barrier()
    }

    async fn batch_mutate(&self, mutations: Vec<KvMutation>) -> StorageResult<()> {
        let mut sealed = Vec::with_capacity(mutations.len());
        for mutation in mutations {
            sealed.push(match mutation {
                KvMutation::Put { key, value } => {
                    let value = self.seal_value(&key, &value).await?;
                    KvMutation::Put { key, value }
                }
                KvMutation::Delete { key } => KvMutation::Delete { key },
            });
        }
        self.inner.batch_mutate(sealed).await
    }

    async fn restore_batch_put(&self, items: Vec<(Vec<u8>, Vec<u8>)>) -> StorageResult<()> {
        let mut encrypted_items = Vec::with_capacity(items.len());

        for (key, value) in items {
            let sealed = self.seal_value(&key, &value).await?;
            encrypted_items.push((key, sealed));
        }

        self.inner.restore_batch_put(encrypted_items).await
    }

    async fn batch_delete(&self, keys: Vec<Vec<u8>>) -> StorageResult<()> {
        self.inner.batch_delete(keys).await
    }

    async fn flush(&self) -> StorageResult<()> {
        self.inner.flush().await
    }

    fn backend_name(&self) -> &'static str {
        "encrypting"
    }

    fn execution_model(&self) -> ExecutionModel {
        self.inner.execution_model()
    }

    fn flush_behavior(&self) -> FlushBehavior {
        self.inner.flush_behavior()
    }
}
