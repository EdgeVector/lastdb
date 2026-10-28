//! Keyed storage operations and bounded range defaults.
use super::*;
use crate::storage::error::{StorageError, StorageResult};
use async_trait::async_trait;

/// Core key-value storage trait
///
/// This is the fundamental storage interface that all backends must implement.
/// Operations are async so a backend can bridge blocking disk work.
#[async_trait]
pub trait KvStore: Send + Sync {
    /// Get a value by key
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>>;

    /// Get multiple values by key, preserving input order.
    ///
    /// The default implementation is correctness-first and delegates to
    /// [`Self::get`] one key at a time. Sync-wrapped local backends should
    /// override this to coalesce many point reads into one blocking section.
    async fn get_many(&self, keys: Vec<Vec<u8>>) -> StorageResult<Vec<Option<Vec<u8>>>> {
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            values.push(self.get(&key).await?);
        }
        Ok(values)
    }

    /// Put a key-value pair
    async fn put(&self, key: &[u8], value: Vec<u8>) -> StorageResult<()>;

    /// Delete a key
    async fn delete(&self, key: &[u8]) -> StorageResult<bool>;

    /// Check if a key exists
    async fn exists(&self, key: &[u8]) -> StorageResult<bool>;

    /// Whether each of `keys` exists, preserving input order.
    ///
    /// The batched counterpart to [`Self::exists`], and deliberately *not*
    /// spelled as `get_many(..).is_some()`: an existence probe over a page of
    /// atom keys must not pull a page of atom bodies into memory to answer it
    /// (bodies here run to `LASTDB_MAX_ATOM_CONTENT_BYTES`, so a 1024-key probe
    /// could materialize hundreds of MiB on a node whose resident budget is the
    /// binding constraint).
    ///
    /// The default implementation is correctness-first and delegates to
    /// [`Self::exists`] one key at a time. Sync-wrapped local backends should
    /// override it to resolve each owning shard once per shard.
    async fn exists_many(&self, keys: Vec<Vec<u8>>) -> StorageResult<Vec<bool>> {
        let mut found = Vec::with_capacity(keys.len());
        for key in keys {
            found.push(self.exists(&key).await?);
        }
        Ok(found)
    }

    /// Scan keys with a given prefix
    async fn scan_prefix(&self, prefix: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Scan **keys only** under `prefix` (ascending), discarding values.
    ///
    /// Used by the durable sync outbox counter seed so a multi-thousand outbox
    /// of multi-MB log entries cannot pin every value in RAM at once
    /// (re-enable thrash 2026-07-14: `outbox_meta` called `scan_prefix` and
    /// held ~all outbox payloads). The default implementation still materialises
    /// values via [`scan_prefix`] then drops them — backends with lazy iterators
    /// should override to never accumulate value buffers.
    async fn scan_prefix_keys(&self, prefix: &[u8]) -> StorageResult<Vec<Vec<u8>>> {
        Ok(self
            .scan_prefix(prefix)
            .await?
            .into_iter()
            .map(|(k, _)| k)
            .collect())
    }

    /// Scan **at most `limit`** records with a given prefix, in ascending key
    /// order — the bounded, paginated counterpart to
    /// [`scan_prefix`](Self::scan_prefix).
    ///
    /// This is the read primitive behind a paginated *list* query
    /// (`HashRangeFilter::Page`): a query that wants the first `limit` records of
    /// a field must not materialize the whole field's index. The page sits at the
    /// front of the key-ordered records, so reading only the first `offset+limit`
    /// records suffices — independent of the field's cardinality.
    ///
    /// The default implementation scans the full prefix, sorts by key (so it is
    /// correct on the hash-backed in-memory store, whose `scan_prefix` is
    /// unordered), then truncates. That is correct everywhere but only *bounds*
    /// the work on backends that override it with a native lazy scan, which takes
    /// only the first `limit` rows and never visits the rest.
    ///
    /// `limit == 0` yields an empty result without scanning.
    async fn scan_prefix_paged(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut rows = self.scan_prefix(prefix).await?;
        // `scan_prefix` is ordered on ordered backends but not on the hash-backed
        // in-memory store, so sort here to make the default deterministic and the
        // "page is the front of the ordered records" contract hold on any backend.
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        rows.truncate(limit);
        Ok(rows)
    }

    /// Scan keys in the half-open byte range `start..end` (inclusive `start`,
    /// exclusive `end`), in ascending key order.
    ///
    /// This is the bounded counterpart to [`scan_prefix`](Self::scan_prefix):
    /// it lets a caller fetch only the records whose keys fall in a contiguous
    /// range — the per-key molecule range/prefix read path uses it so a range
    /// query costs `O(matches)`, not `O(field cardinality)`.
    ///
    /// The default implementation derives the longest common prefix of `start`
    /// and `end`, scans that prefix, and filters the rows to `start..end` in
    /// memory. That keeps every backend correct with no per-backend work, and
    /// is already bounded for the common case (e.g. a date range
    /// `2025-01-01..2025-12-31` shares the `2025-` prefix). Backends with a
    /// native ordered range override this with a true range scan.
    ///
    /// `start >= end` yields an empty result (an inverted/empty range matches
    /// nothing) rather than scanning.
    async fn scan_range(&self, start: &[u8], end: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        if start >= end {
            return Ok(Vec::new());
        }
        // Longest common prefix bounds the scan; the in-memory filter then
        // narrows it to the exact half-open range.
        let common = start
            .iter()
            .zip(end.iter())
            .take_while(|(a, b)| a == b)
            .count();
        let rows = self.scan_prefix(&start[..common]).await?;
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = rows
            .into_iter()
            .filter(|(k, _)| k.as_slice() >= start && k.as_slice() < end)
            .collect();
        // The contract promises ascending key order. `scan_prefix` is ordered on
        // ordered backends but not on the hash-backed in-memory store, so sort
        // here to make the default deterministic regardless of backend.
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    /// Scan at most `limit` keys in the half-open byte range `start..end`
    /// (inclusive `start`, exclusive `end`), in ascending key order.
    ///
    /// This is the keyset-pagination counterpart to
    /// [`Self::scan_prefix_paged`]: a cursor page starts at the previous row's
    /// storage key and takes only the next `limit` ordered rows from the range.
    /// Backends with native ordered ranges should override this so the scan is
    /// lazy and bounded. The default remains correctness-first.
    async fn scan_range_paged(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut rows = self.scan_range(start, end).await?;
        rows.truncate(limit);
        Ok(rows)
    }

    /// Scan up to `limit` rows of `start..end` and name where to continue.
    ///
    /// For a walk over a range that holds many deleted keys. A backend whose
    /// reads have a key budget may stop short of `limit` rows when the budget
    /// runs low, in a run of deleted keys or between live rows: it then returns
    /// the rows it has and `resume`, the last key it examined, in place of a
    /// read-limit error. `keys_per_row` says how many keys the caller reads
    /// for each row it gets (its counters and bodies, in the same call): the
    /// backend stops while it still has them. A page that is short does not end
    /// the range; `resume` is `None` only at the end of the range. The default
    /// reads as [`Self::scan_range_paged`] and names the last row of a full
    /// page.
    async fn scan_range_yielding(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        keys_per_row: usize,
    ) -> StorageResult<YieldedScan> {
        let _ = keys_per_row;
        let rows = self.scan_range_paged(start, end, limit).await?;
        let resume = (limit > 0 && rows.len() >= limit)
            .then(|| rows.last().map(|(key, _)| key.clone()))
            .flatten();
        Ok(YieldedScan { rows, resume })
    }

    /// Scan one bounded page of a range for resumable maintenance.
    ///
    /// Product queries must use the globally ordered range methods above.
    /// Disk format v2 has no sharded backend. The pack store keeps this
    /// default (one logical keyset page with an `after_key` cursor), and the
    /// encrypting store forwards to its inner store.
    async fn scan_range_physical_paged(
        &self,
        start: &[u8],
        end: &[u8],
        cursor: Option<&PhysicalScanCursor>,
        limit: usize,
        max_handles: usize,
    ) -> StorageResult<PhysicalScanPage> {
        scan::keyset_physical_page(self, start, end, cursor, limit, max_handles).await
    }

    /// Scan keys with a given prefix, tolerating per-row read failures.
    ///
    /// Identical to [`scan_prefix`](Self::scan_prefix) for every plain
    /// backend (the default impl just delegates), and intended for callers
    /// that read a *derived, regenerable* index where one unreadable row must
    /// not fail the whole scan. The only failure mode this exists to absorb is
    /// an encrypting layer that holds a row it can't decrypt with any
    /// registered key (e.g. an org-scoped embedding row synced before the
    /// org's crypto provider was registered): such a row is skipped with a
    /// warning rather than propagated, so a single bad row can't wedge node
    /// startup. Stores backing authoritative data (atom content, metadata)
    /// keep using `scan_prefix`, which still fails loudly.
    ///
    /// **SECURITY — DERIVED, RE-DERIVABLE INDICES ONLY.** Silently dropping an
    /// unreadable row is *only* safe over a namespace that can be fully rebuilt
    /// from a source of truth (today the single caller is the `emb:` embeddings
    /// index, regenerated by re-embedding). Do **NOT** point this at a primary
    /// or security-critical namespace (consent grants, the revocation ledger,
    /// capability records, ACLs): there, a dropped row is *selective record
    /// hiding* — e.g. an unreadable revocation row would silently un-revoke a
    /// capability (fail OPEN). Such namespaces must fail CLOSED on an unreadable
    /// row instead (see `fold_db_node`'s `RevocationStore::read_all`, which
    /// trips an integrity flag and denies rather than dropping). If you need a
    /// lossy scan over a new namespace, confirm it is re-derivable first and
    /// add it to this allow-list note.
    async fn scan_prefix_lossy(&self, prefix: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_prefix(prefix).await
    }

    /// Scan a prefix, **partitioning** rows into those that read cleanly and
    /// the keys of rows whose at-rest value cannot be decrypted.
    ///
    /// This is the non-aborting, *reporting* counterpart to
    /// [`scan_prefix`](Self::scan_prefix) (which fails the whole scan on the
    /// first undecryptable row) and [`scan_prefix_lossy`](Self::scan_prefix_lossy)
    /// (which silently drops them). It underpins the two maintenance paths that
    /// must survive local at-rest "poison" rows without either aborting or
    /// hiding them: a snapshot backup that skips + counts undecryptable rows,
    /// and an explicit scrub that enumerates (and optionally deletes) them.
    ///
    /// The default implementation is for backends with no decrypt step: every
    /// row is returned as good and `undecryptable` is empty. The encrypting
    /// decorator overrides it to attempt decryption per row and collect the
    /// keys it cannot read. A decorator that merely forwards reads must forward
    /// this too, or its inherited default would shadow the encrypting override
    /// and silently re-abort on a poison row.
    async fn scan_prefix_partition_undecryptable(
        &self,
        prefix: &[u8],
    ) -> StorageResult<PartitionedScan> {
        Ok(PartitionedScan {
            rows: self.scan_prefix(prefix).await?,
            undecryptable: Vec::new(),
        })
    }

    /// Apply a batch of puts with the backend's documented failure and crash guarantees.
    async fn batch_put(&self, items: Vec<(Vec<u8>, Vec<u8>)>) -> StorageResult<()>;

    /// Apply ordered puts and deletes with one backend durability barrier.
    ///
    /// Each backend defines its operation-error rollback guarantee. No such
    /// guarantee implies cross-group WAL crash atomicity. Callers must make
    /// every possible crash prefix safe. The default fails closed because two
    /// separate batches cannot satisfy this contract.
    async fn batch_mutate(&self, _mutations: Vec<KvMutation>) -> StorageResult<()> {
        Err(StorageError::InvalidOperation(format!(
            "backend '{}' does not support ordered mixed durable batches",
            self.backend_name()
        )))
    }

    /// Apply one photograph-restore batch without requiring a durability
    /// barrier before this call returns.
    ///
    /// The restore owner MUST call
    /// [`NamespacedStore::restore_durability_barrier`] after every batch
    /// succeeds and before it exposes the restored store. Backends without a
    /// deferred write path keep the safe default and use their normal
    /// [`Self::batch_put`] contract. This method does not change normal
    /// acknowledged writes.
    async fn restore_batch_put(&self, items: Vec<(Vec<u8>, Vec<u8>)>) -> StorageResult<()> {
        self.batch_put(items).await
    }

    /// Batch delete operations
    async fn batch_delete(&self, keys: Vec<Vec<u8>>) -> StorageResult<()>;

    /// Flush pending operations to storage (no-op for backends that auto-flush)
    async fn flush(&self) -> StorageResult<()>;

    /// Get storage backend name (for debugging/metrics)
    fn backend_name(&self) -> &'static str;
}
