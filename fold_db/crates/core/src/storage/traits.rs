//! Backend-agnostic storage traits and the typed JSON adapter.
//!
//! ## Trait surface (what to implement vs what to use)
//!
//! | Type | Role |
//! |------|------|
//! | [`KvStore`] | Byte key/value ops. **Implement this** for a new backend. |
//! | [`NamespacedStore`] | Open/list/delete logical trees. **Implement this** too. |
//! | [`TypedKvStore`] | JSON serialize/deserialize helper on top of any `KvStore`. **Use, don't reimplement.** |
//! | [`ExecutionModel`] / [`FlushBehavior`] | Capability hints for callers (sync-wrapped vs true async). |
//! | [`PartitionedScan`] | Result of poison-aware scans (encrypting backends). |
//!
//! Scan variants on [`KvStore`] (`scan_prefix`, `_keys`, `_paged`, `scan_range`,
//! `_lossy`, `_partition_undecryptable`) are **one family** with different
//! failure/pagination contracts — not parallel APIs. Defaults are correct but
//! unbounded; ordered backends (Last Store) should override for lazy iterators.
//!
//! See the parent [`crate::storage`] module docs for the layering diagram
//! (traits → laststore/inmemory → encrypting wrappers).

use async_trait::async_trait;
use std::sync::Arc;

pub(crate) fn key_u64_after_last_marker(key: &[u8], marker: &[u8]) -> Option<u64> {
    if marker.is_empty() || key.len() < marker.len() {
        return None;
    }
    let marker_start = key.windows(marker.len()).rposition(|part| part == marker)?;
    let suffix = key.get(marker_start + marker.len()..)?;
    std::str::from_utf8(suffix).ok()?.parse().ok()
}

use super::error::{StorageError, StorageResult};

mod scan;
mod stats;
mod typed;

pub use scan::*;
pub use stats::*;
pub use typed::*;

/// Core key-value storage trait
///
/// This is the fundamental storage interface that all backends must implement.
/// Operations are async to support both local (Sled) and remote backends.
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

    /// Replace (`new = Some`) or delete (`new = None`) `key` only when its
    /// current raw value is exactly `expected`, as **one atomic step**.
    ///
    /// `Ok(true)` means the write applied; `Ok(false)` means the current value
    /// differed and nothing was written. No write to `key` through any other
    /// method of this store can land between the compare and the write.
    ///
    /// There is deliberately no get-then-put default: that pair is the race
    /// this method exists to close
    /// (`papercut-lastdb-reseal-check-then-put-race-20260923`). A backend
    /// without a native conditional write refuses instead, so a maintenance
    /// pass fails loudly rather than overwriting a concurrent write.
    async fn compare_and_swap(
        &self,
        key: &[u8],
        expected: &[u8],
        new: Option<Vec<u8>>,
    ) -> StorageResult<bool> {
        let _ = (key, expected, new);
        Err(StorageError::InvalidOperation(format!(
            "backend `{}` has no native compare_and_swap",
            self.backend_name()
        )))
    }

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
    /// (Sled) should override to never accumulate value buffers.
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
    /// the work on backends that override it with a native lazy scan: Sled's
    /// `scan_prefix` is a lazy ordered iterator, so its override takes only the
    /// first `limit` rows off the iterator and never visits the rest.
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
    /// native ordered range (Sled) override this with a true range scan.
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

    /// Greatest decimal `u64` suffix after the last `marker` among keys under
    /// `prefix`.
    ///
    /// The default is correctness-first and materializes the matching keys.
    /// Backends with physical key indexes should override this with a
    /// constant-memory fold that opens each physical group at most once.
    async fn max_key_u64_after_marker(
        &self,
        prefix: &[u8],
        marker: &[u8],
    ) -> StorageResult<Option<u64>> {
        let keys = self.scan_prefix_keys(prefix).await?;
        Ok(keys
            .iter()
            .filter_map(|key| key_u64_after_last_marker(key, marker))
            .max())
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

    /// Scan a range while bounding physical shard/group resolution.
    ///
    /// Product queries must use the globally ordered range methods above.
    /// This primitive exists for resumable maintenance over a global physical
    /// plane, where a logical first page would still resolve every hash group.
    /// The default keeps correctness for non-sharded backends with a logical
    /// key cursor. Local sharded backends should override it.
    async fn scan_range_physical_paged(
        &self,
        start: &[u8],
        end: &[u8],
        cursor: Option<&PhysicalScanCursor>,
        limit: usize,
        max_handles: usize,
    ) -> StorageResult<PhysicalScanPage> {
        if limit == 0 || max_handles == 0 || start >= end {
            return Ok(PhysicalScanPage {
                next_cursor: cursor.cloned(),
                ..Default::default()
            });
        }
        let resume = cursor
            .and_then(|position| position.after_key.as_deref())
            .unwrap_or(start);
        // `scan_range_paged` includes its start key. A resume page therefore
        // needs one slot for that duplicate plus one slot for lookahead.
        let lookahead = limit
            .saturating_add(1)
            .saturating_add(usize::from(cursor.is_some()));
        let mut rows = self.scan_range_paged(resume, end, lookahead).await?;
        if let Some(after) = cursor.and_then(|position| position.after_key.as_deref()) {
            rows.retain(|(key, _)| key.as_slice() != after);
        }
        let more_remaining = rows.len() > limit;
        rows.truncate(limit);
        let handle = PhysicalScanCursor::default();
        let next_cursor = more_remaining.then(|| PhysicalScanCursor {
            after_key: rows.last().map(|(key, _)| key.clone()),
            ..handle.clone()
        });
        Ok(PhysicalScanPage {
            row_handle: (!rows.is_empty()).then_some(handle),
            rows,
            next_cursor,
            handles_visited: 1,
            cold_shard_loads: 0,
        })
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

    /// Whether a successful `batch_put` has already flushed its writes to disk.
    /// The default is conservative. Backends must report their stored write mode.
    fn batch_put_has_durable_barrier(&self) -> bool {
        false
    }

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

    /// Get the execution model of this backend
    ///
    /// This describes whether the backend is truly async (network I/O)
    /// or sync wrapped in async (local I/O).
    fn execution_model(&self) -> ExecutionModel;

    /// Get the flush behavior of this backend
    ///
    /// This describes whether flush is a no-op (eventually consistent)
    /// or performs actual persistence (strongly consistent).
    fn flush_behavior(&self) -> FlushBehavior;
}

/// Namespace storage trait
///
/// Provides logical separation of data into "namespaces" or collections
/// (Last Store collections, or logical partitions in remote stores)
#[async_trait]
pub trait NamespacedStore: Send + Sync {
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

    /// Shared logical resident set, when this backend has one.
    fn logical_resident_set(
        &self,
    ) -> Option<Arc<std::sync::Mutex<crate::resident::LogicalResidentSet>>> {
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
