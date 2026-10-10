//! Last Store–backed [`KvStore`] / [`NamespacedStore`].
//!
//! ## Access model (document store, not table scan)
//!
//! Last Store keeps a per-shard **`BTreeMap<String, Loc>`** of document ids.
//! This adapter is a thin map onto that tree:
//!
//! | Trait op | Engine | Cost model |
//! |----------|--------|------------|
//! | `get` / `exists` | `BTreeMap::get` (+ optional value cache) | O(log N) point lookup |
//! | `put` / `delete` | index update + append | O(log N) + amortised group-commit |
//! | `scan_prefix*` | `list_prefix*` with the **real** id prefix | O(log N + k) B-tree range |
//! | `scan_range*` | `list_range*` half-open bounds | O(log N + k) B-tree range |
//! | `scan_prefix_keys` | `list_prefix_keys` — **ids only** | O(log N + k), **no body hydrate** |
//!
//! **Never** materialise the whole collection then filter in RAM. That was the
//! Phase-1 bring-up bug that turned every molecule prefix Query into a full
//! `main` Scan on multi-million-key homes.
//!
//! Body hydrate happens only for the ids the B-tree already selected (engine
//! `list_prefix_paged` / `list_range_paged` does id walk then `get` per id).
//! Keys-only paths never load bodies.
//!
//! ## Key encoding
//!
//! Trait keys are bytes; Last Store ids are strings. UTF-8 keys that do not
//! start with `b64:` are stored as themselves (production molecule keys are
//! UTF-8). Other keys use `b64:` + standard base64. Prefix/range navigation is
//! correct for UTF-8 keys (identity encoding preserves byte order as string
//! order for those ids). Binary/`b64:` prefix walks are best-effort on the
//! encoded id space — Mini's hot paths use UTF-8 keys.

mod absence_hints;
mod backup_descriptor;
mod backup_manifest;
mod compact_dispatch;
mod direct_write_invalidation;
pub mod dual_read_metrics;
pub mod high_water;
mod key_routing;
mod kv_store_impl;
mod logical_main;
pub(crate) mod logical_path;
mod namespaced_maintenance;
mod namespaced_store_impl;
mod partition_scan;
mod residue_types;

use super::encrypting_namespaced_store::LASTSTORE_PLAINTEXT_NAMESPACES;
use super::error::{StorageError, StorageResult};
use super::traits::{
    ExecutionModel, FlushBehavior, KvMutation, KvStore, NamespacedStore, PartitionedScan,
    PhysicalScanCursor, PhysicalScanPage,
};
use crate::hex::sha256_hex;
use crate::kind_partition::{colon_prefix_matches, form_twin, logical_row_id, rewrite_key_like};
use crate::mini_cutover::{
    classify_main_key, LEGACY_MAIN_MIGRATION_COLLECTIONS, MAIN_MIGRATION_COLLECTIONS,
};
use async_trait::async_trait;
pub use backup_descriptor::{
    assemble_descriptor_root, build_descriptor_pages, canonical_bytes,
    descriptor_entries_from_manifest, is_stored_bytes_sha256_hex, parse_descriptor_page,
    parse_retirement_receipt_v2, sign_carry_receipt, sign_descriptor_page,
    sign_retirement_receipt_v2, verify_carry_receipt, verify_descriptor_page,
    verify_retirement_receipt_v2, CanonicalRecord, CarryReceipt, DescriptorEntry,
    DescriptorObjectClass, DescriptorPage, DescriptorPageHeader, DescriptorRootView,
    EncryptedManifestCommitment, RetiredInstance, RetirementReceiptV2, SelectedAtomsRecord,
};
pub use backup_manifest::{
    apply_cas_proven_unbackable_retirement, apply_named_hole_exclusions,
    cas_proven_named_hole_shas, cas_proven_unbackable_atom_shas, classify_descriptor_chain_step,
    classify_manifest_chain_step, compute_backup_storage_footprint,
    enumerate_backup_publish_target_candidates, manifest_referenced_chunk_shas,
    manifest_sha256_hex, select_orphan_backup_chunk_shas, unbackable_manifest_chunk_count,
    validate_manifest_chain, validate_manifest_chain_before_packing, AtomPhotographCopyReport,
    BackupChunkRef, BackupChunkScan, BackupChunkUploadCandidate, BackupDeletionReceipt,
    BackupManifest, BackupManifestChainStep, BackupManifestRole, BackupNamedHole,
    BackupPackLocation, BackupStorageFootprint, CloudChunkPresence, DescriptorChainStep,
    StampCommittedSuccessorHistoryReport, UnresolvableChunk, DESCRIPTOR_VERSION, MANIFEST_VERSION,
    NAMED_HOLE_REASON_ABSENT_LOCAL_AND_CLOUD, PACKED_MANIFEST_VERSION,
    UNBACKABLE_RETIREMENT_REASON_ABSENT_LOCAL_AND_CLOUD,
};
use base64::{engine::general_purpose::STANDARD, Engine as _};
pub use dual_read_metrics::{
    apply_tip_residue_copy_page, classify_conflict_residue_copy, classify_index_residue_copy,
    classify_order_log_residue_copy, classify_protein_residue_copy, classify_tip_residue_copy,
    dual_read_metrics_reset, dual_read_metrics_snapshot, DualReadMetricsSnapshot,
    PlaneResidueCopyAction, TipResidueCopyAction, TipResidueCopyPageReport, CONFLICT_KEY_PREFIXES,
    INDEX_RESIDUE_KEY_PREFIXES, INDEX_RESIDUE_LEGACY_COLLECTIONS, ORDER_LOG_COLLECTIONS,
    ORDER_LOG_KEY_PREFIXES, PROTEIN_FAMILY_KEY_PREFIXES, TIP_RESIDUE_KEY_PREFIXES,
    TIP_RESIDUE_LEGACY_COLLECTIONS,
};
pub use high_water::{
    cloud_db_hash_for_store_uuid, high_water_path_for_store_root, read_backup_durability,
    read_cloud_db_hash, BackupDurability,
};

pub use compact_dispatch::COMPACT_NAMED_ONLY;
use high_water::LastStoreHighWaterFile;
pub(crate) use key_routing::MAIN_KEY_PREFIX_COLLECTIONS;
use key_routing::*;
use namespaced_maintenance::collection_dir_bytes;
pub use residue_types::*;

use laststore::{HashGroupKey, LastStore, LastStoreOptions, RetiredCompactGate, TxnOp};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::resident::{LogicalResidentSet, ResidentMetrics};
use uuid::Uuid;

const LOGICAL_MAIN_COLLECTION: &str = "main";
const TIPS_COLLECTION: &str = "tips";
const INDEXES_COLLECTION: &str = "indexes";

/// `indexes`-plane prefixes [`LastStoreNamespacedStore::reclaim_retired_index_residue`]
/// will delete.
///
/// The authority for *whether* these are retired is the engine flags in
/// `db_operations::atom_store::helpers`; this list is the storage layer's own
/// independent bound on what a caller may ask it to delete, so a drifting
/// caller cannot reach `schemaidx:` (its own verb), `schema_atoms:`, or `idx:`
/// (governed by no retirement flag).
pub const RECLAIMABLE_RETIRED_INDEX_PREFIXES: &[&str] = &["mhr:", "mhi:", "mhk:"];

/// Catalog collections are plaintext-by-policy (Trinity-only). Persisting an
/// An at-rest envelope there is what made Tom's primary unbootable on 2026-08-16:
/// `TypedKvStore` parsed ciphertext as JSON (`expected value at line 1 column 1`)
/// and `get_all_schemas` aborted `open_existing_store`. Reject at the LastStore
/// put so a mis-wired `EncryptingNamespacedStore::new()` (empty allowlist)
/// fails closed instead of poisoning the catalog.
fn reject_enc_on_plaintext_catalog(collection: &str, value: &[u8]) -> StorageResult<()> {
    // All at-rest envelopes are equally fatal on a plaintext catalog: the
    // 2026-08-16 brick was an `ENC:` body the catalog reader could not parse,
    // and `ENZ:` / binary `ENB:` bodies would fail identically.
    let enveloped = crate::crypto::is_sealed_at_rest(value);
    if enveloped && LASTSTORE_PLAINTEXT_NAMESPACES.contains(&collection) {
        return Err(StorageError::InvalidOperation(format!(
            "refusing to persist ENC:/ENZ:/ENB: envelope in plaintext catalog \
             collection {collection:?} — schema/catalog rows must stay JSON so \
             boot can deserialize them"
        )));
    }
    Ok(())
}

/// Env: defer the batch-transaction durability barrier (`1`/`true`/`yes`/`on`).
///
/// The resident-primary write-ack gate. When set, `batch_put` / `batch_delete`
/// apply their ops with [`LastStore::transaction_deferred`] instead of paying
/// a sync-every-open-shard [`LastStore::flush`] per batch. Durability then
/// rides the per-shard group-commit buffers and the background flusher
/// (`crate::fold_db_core::mutation_flush`) — the same crash window that policy
/// already documents for the ack path. Default off: every batch keeps today's
/// per-transaction barrier.
pub const TXN_DEFERRED_FLUSH_ENV: &str = "LASTDB_TXN_DEFERRED_FLUSH";

/// `LASTDB_TXN_DEFERRED_FLUSH` truthy → defer batch barriers.
fn deferred_batch_flush_from_env() -> bool {
    parse_deferred_batch_flush(std::env::var(TXN_DEFERRED_FLUSH_ENV).ok())
}

/// Pure parser behind [`deferred_batch_flush_from_env`] (unit-testable without
/// racing `std::env` across parallel test threads).
fn parse_deferred_batch_flush(raw: Option<String>) -> bool {
    raw.is_some_and(|s| env_flag::truthy(&s))
}

/// Last Store-backed [`KvStore`] implementation.
///
/// Namespace names map directly to Last Store collection names.
pub(crate) struct LastStoreKvStore {
    store: Arc<LastStore>,
    collection: String,
    high_water: Option<Arc<LastStoreHighWaterFile>>,
    /// See [`TXN_DEFERRED_FLUSH_ENV`]. Resolved once at construction.
    deferred_batch_flush: bool,
    logical: Arc<Mutex<LogicalResidentSet>>,
}

impl LastStoreKvStore {
    /// Upper bound on the up-front reservation for one hydrate batch.
    ///
    /// The batch itself is sized by what is still needed, but that figure is
    /// `usize::MAX` for the unbounded callers (`scan_prefix`, `list_range`), and
    /// reserving it is a capacity-overflow panic rather than a big allocation.
    /// The vectors still grow as far as the batch genuinely goes; this only
    /// caps the guess.
    const GC_HYDRATE_BATCH_RESERVE: usize = 2048;

    fn with_logical(
        store: Arc<LastStore>,
        collection: String,
        high_water: Option<Arc<LastStoreHighWaterFile>>,
        logical: Arc<Mutex<LogicalResidentSet>>,
    ) -> Self {
        Self {
            store,
            collection,
            high_water,
            deferred_batch_flush: deferred_batch_flush_from_env(),
            logical,
        }
    }

    async fn run_blocking<T, F>(work: F) -> StorageResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> StorageResult<T> + Send + 'static,
    {
        // Task-locals die across this hop. Capture the batch log on the caller.
        let batch_log = crate::durable_flush::current();
        let can_block = tokio::runtime::Handle::try_current().is_ok_and(|handle| {
            matches!(
                handle.runtime_flavor(),
                tokio::runtime::RuntimeFlavor::MultiThread
            )
        });
        let admit = crate::warm_admit::current_code();
        let observed_work = move || {
            laststore::with_admit_code(admit, || {
                let ((result, activity), placed) =
                    laststore::observe_placed_writes(|| laststore::observe_read_activity(work));
                (result, activity, placed)
            })
        };
        let (result, activity, placed) = if can_block {
            tokio::task::block_in_place(observed_work)
        } else {
            tokio::task::spawn_blocking(observed_work).await?
        };
        if let Some(log) = batch_log {
            log.record(placed);
        }
        // Restore attribution in the caller's task-local scope after the
        // synchronous storage call. A store-wide delta would charge peers.
        crate::request_phases::add_counter(
            crate::request_phases::RequestCounter::PartitionReadRejections,
            activity.partition_read_rejections,
        );
        crate::request_phases::add_counter(
            crate::request_phases::RequestCounter::AllGroupWalks,
            activity.all_group_walks,
        );
        result
    }

    pub(super) fn encode_key(key: &[u8]) -> String {
        if let Ok(key) = std::str::from_utf8(key) {
            if !key.starts_with("b64:") {
                return key.to_string();
            }
        }

        format!("b64:{}", STANDARD.encode(key))
    }

    pub(super) fn decode_key(id: &str) -> StorageResult<Vec<u8>> {
        let Some(encoded) = id.strip_prefix("b64:") else {
            return Ok(id.as_bytes().to_vec());
        };
        STANDARD
            .decode(encoded)
            .map_err(|e| StorageError::BackendError(format!("laststore key decode failed: {e}")))
    }

    /// Translate a Last Store failure into this crate's storage error.
    ///
    /// Takes the concrete [`laststore::Error`] rather than `impl Display` so a
    /// full disk can be recognized before the type is flattened into a string.
    /// `Display` reached every caller here, which meant `ENOSPC` — the one
    /// storage failure an operator can actually fix — arrived downstream
    /// indistinguishable from a backend defect and was reported as invalid
    /// caller data (Sentry `7620061902`).
    pub(super) fn map_error(error: laststore::Error) -> StorageError {
        match error {
            laststore::Error::Io(io) if StorageError::is_storage_full_io(&io) => {
                StorageError::StorageFull {
                    detail: io.to_string(),
                }
            }
            other => StorageError::BackendError(format!("laststore: {other}")),
        }
    }

    fn record_high_water(
        high_water: Option<&Arc<LastStoreHighWaterFile>>,
        store: &LastStore,
    ) -> StorageResult<()> {
        if let Some(high_water) = high_water {
            high_water.record_csn_high_water(store.csn_high_water())?;
        }
        Ok(())
    }

    /// Map trait byte bound → Last Store string id (same rules as put/get).
    fn encode_bound(key: &[u8]) -> String {
        Self::encode_key(key)
    }

    /// Choose the B-tree id prefix for a trait byte-prefix scan.
    ///
    /// - Empty → entire collection (caller asked to list all; tiny namespaces only).
    /// - UTF-8 and not `b64:`-reserved → **identity** id band (hot molecule path).
    /// - Binary / reserved `b64:`… → walk only escaped ids (`b64:…`), then
    ///   filter on **decoded** bytes. Never walk the whole collection.
    ///
    /// Returns `(id_prefix, filter_decoded_byte_prefix)`.
    fn id_prefix_for_scan(prefix: &[u8]) -> (String, bool) {
        if prefix.is_empty() {
            return (String::new(), false);
        }
        if let Ok(s) = std::str::from_utf8(prefix) {
            if !s.starts_with("b64:") {
                return (s.to_string(), false);
            }
        }
        ("b64:".to_string(), true)
    }

    fn decode_rows(rows: Vec<(String, Vec<u8>)>) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        rows.into_iter()
            .map(|(id, value)| Ok((Self::decode_key(&id)?, value)))
            .collect()
    }

    fn decode_ids(ids: Vec<String>) -> StorageResult<Vec<Vec<u8>>> {
        ids.into_iter().map(|id| Self::decode_key(&id)).collect()
    }

    /// B-tree prefix walk + hydrate only matched ids (engine does get-per-id).
    pub(super) fn prefix_rows_sync(
        store: &LastStore,
        collection: &str,
        prefix: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let (id_prefix, filter_decoded) = Self::id_prefix_for_scan(prefix);
        if limit == 0 {
            store
                .list_prefix_keys_paged(collection, &id_prefix, None, 0)
                .map_err(Self::map_error)?;
            return Ok(Vec::new());
        }
        if !filter_decoded {
            let rows = store
                .list_prefix_paged(collection, &id_prefix, None, limit)
                .map_err(Self::map_error)?;
            return Self::decode_rows(rows);
        }
        // Escaped band only: the `b64:` ids that decode under `prefix` are not
        // contiguous, so matches must be filtered on the decoded bytes.
        //
        // One keys pass, then hydrate only the matches. Paging this with
        // `list_prefix_paged` was quadratic: that call materializes the *whole*
        // `b64:` band across every group before applying its limit, so each
        // page re-walked the entire band (under `HashGroup`, every group in the
        // collection) to yield 256 more ids — and hydrated bodies for the
        // non-matching ids of each page too, only to discard them.
        let ids = store
            .list_prefix_keys_paged(collection, &id_prefix, None, usize::MAX)
            .map_err(Self::map_error)?;
        // An id deleted between the keys pass and the hydrate is dropped, not
        // an error: the old paged loop could not see such a row either, and a
        // concurrent delete must not fail an unrelated read.
        //
        // But dropping it must not SHORTEN the page, or "fewer rows than the
        // limit" stops meaning "the prefix is exhausted" — the signal every
        // keyset-paging caller stops on. So keep hydrating further matches
        // until the page is full or the matches run out. The extra work is
        // bounded by the number that actually vanished, and the id listing
        // above is done once either way. See `LastStore::list_range_paged` for
        // the same invariant on the range path, and for what a truncated
        // `gc-atoms` reference walk costs.
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut remaining = ids.into_iter();
        while out.len() < limit {
            let need = limit - out.len();
            // Reserve for the batch, NOT for `need`: the unbounded callers
            // (`scan_prefix`, `list_range`) pass `usize::MAX` as the limit, and
            // `Vec::with_capacity(usize::MAX)` is a capacity-overflow panic, not
            // a large allocation. Caught by
            // `storage_abstraction_tests::test_laststore_backend_basic_operations`,
            // whose `scan_prefix(b"b64:")` lands in exactly this branch.
            let reserve = need.min(Self::GC_HYDRATE_BATCH_RESERVE);
            let mut batch_ids = Vec::with_capacity(reserve);
            let mut batch_keys = Vec::with_capacity(reserve);
            for id in remaining.by_ref() {
                let key = Self::decode_key(&id)?;
                if key.starts_with(prefix) {
                    batch_ids.push(id);
                    batch_keys.push(key);
                    if batch_ids.len() == need {
                        break;
                    }
                }
            }
            if batch_ids.is_empty() {
                break;
            }
            let bodies = store
                .get_many(collection, &batch_ids)
                .map_err(Self::map_error)?;
            out.extend(
                batch_keys
                    .into_iter()
                    .zip(bodies)
                    .filter_map(|(key, body)| body.map(|body| (key, body))),
            );
        }
        Ok(out)
    }

    /// B-tree prefix walk — **keys only**, no body load.
    pub(super) fn prefix_keys_sync(
        store: &LastStore,
        collection: &str,
        prefix: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<Vec<u8>>> {
        let (id_prefix, filter_decoded) = Self::id_prefix_for_scan(prefix);
        if limit == 0 {
            store
                .list_prefix_keys_paged(collection, &id_prefix, None, 0)
                .map_err(Self::map_error)?;
            return Ok(Vec::new());
        }
        if !filter_decoded {
            let ids = store
                .list_prefix_keys_paged(collection, &id_prefix, None, limit)
                .map_err(Self::map_error)?;
            return Self::decode_ids(ids);
        }
        // One keys pass over the escaped band, then filter on decoded bytes —
        // see `prefix_rows_sync` for why paging this was quadratic.
        let ids = store
            .list_prefix_keys_paged(collection, &id_prefix, None, usize::MAX)
            .map_err(Self::map_error)?;
        let mut out = Vec::new();
        for id in ids {
            let key = Self::decode_key(&id)?;
            if key.starts_with(prefix) {
                out.push(key);
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    /// B-tree half-open range `[start, end)` + hydrate matched ids only.
    ///
    /// Hot path is UTF-8 identity keys (molecule / hash-range). Bounds are
    /// encoded the same way as put keys so order matches for that encoding.
    fn range_rows_sync(
        store: &LastStore,
        collection: &str,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        if limit == 0 || start >= end {
            return Ok(Vec::new());
        }
        let start_id = Self::encode_bound(start);
        let end_id = Self::encode_bound(end);
        if start_id >= end_id {
            return Ok(Vec::new());
        }
        let rows = store
            .list_range_paged(collection, &start_id, &end_id, limit)
            .map_err(Self::map_error)?;
        Self::decode_rows(rows)
    }

    /// Greatest numeric key suffix without hydrating row bodies.
    fn max_key_u64_after_marker_sync(
        store: &LastStore,
        collection: &str,
        prefix: &[u8],
        marker: &[u8],
    ) -> StorageResult<Option<u64>> {
        if marker.is_empty() {
            return Ok(None);
        }
        let (id_prefix, filter_decoded) = Self::id_prefix_for_scan(prefix);
        if !filter_decoded {
            if let Ok(marker) = std::str::from_utf8(marker) {
                return store
                    .max_u64_id_suffix(collection, &id_prefix, marker)
                    .map_err(Self::map_error);
            }
        }

        // Binary/reserved keys are not the sync pin-log production path. Keep
        // the adapter correct by using its decoded keys fallback.
        Ok(
            Self::prefix_keys_sync(store, collection, prefix, usize::MAX)?
                .iter()
                .filter_map(|key| super::traits::key_u64_after_last_marker(key, marker))
                .max(),
        )
    }
}

/// Logical `main` namespace over the target LastStore `atoms` + `tips`
/// collections, with read/delete compatibility for migration-era split
/// collections produced by early Mini cutover builds.
///
/// ## One logical key, one physical row
///
/// Reads, deletes and existence checks span every candidate collection for a
/// key ([`main_collections_for_key`]); writes target the canonical one alone.
/// A read-modify-write over a still-active migration-era row therefore reads it
/// from a legacy collection and writes the modified copy to `tips`, leaving the
/// original in place — one logical key with two physical rows, the stale one
/// first.
///
/// `get` is unaffected: it returns the first candidate, which is canonical. The
/// scans are what broke, because they concatenated the collections. They now
/// collapse to one row per key with that same precedence, so a scan and a point
/// read agree about what exists and a paged scan spends its limit on distinct
/// keys. See [`Self::dedupe_and_sort_rows`].
///
/// Converging the *physical* rows — having a write displace the legacy copy —
/// is done here, in [`Self::converging_deletes`]. It makes every write a
/// deleter, and a hydrating walk concurrent with a delete used to fail the whole
/// scan with `corrupt: id vanished during walk`. `LastStore::load_bodies_by_shard`
/// now skips a vanished id and counts it in `LastStore::walk_vanished_ids()`,
/// which is what unblocked it.
///
/// Converging is the ONLY thing that removes the second walk. Measured in
/// [`legacy_collection_fanout_cost_tests`]: an *absent* legacy collection
/// already costs nothing — `hash_groups_on_disk` returns early on the missing
/// directory — so the fan-out is paid only when the collection holds rows, and
/// that is exactly the state no emptiness check can detect. The pruned
/// absent/zero-hit collections avoid new fan-out, while still-populated legacy
/// collections continue paying an extra enumeration until their rows drain.
///
/// ## Why the converging point write is ordered, not transactional
///
/// `LastStore::transaction` restores prior values after a reported operation
/// failure, but it has no cross-group WAL crash transaction. Routing a point
/// `put` through it would add no stronger crash guarantee while adding a scoped
/// durability barrier to every single write.
///
/// So the crash guarantee comes from **order** instead: the canonical put lands
/// first, the legacy delete second. A crash between them leaves both rows, and
/// both-rows is precisely the state every read path above already handles —
/// canonical shadows legacy. The reverse order would have a crash window that
/// loses the row outright. `batch_put` restores each applied key to its previous
/// body when a later key fails, then syncs only the groups that batch wrote.
pub(crate) struct LogicalMainLastStoreKvStore {
    store: Arc<LastStore>,
    high_water: Option<Arc<LastStoreHighWaterFile>>,
    /// See [`TXN_DEFERRED_FLUSH_ENV`]. Resolved once at construction.
    deferred_batch_flush: bool,
    logical: Arc<Mutex<LogicalResidentSet>>,
    /// Legacy collections this home actually carries on disk, resolved once.
    ///
    /// [`LastStore::collections_on_disk`] is a `read_dir`, so it cannot run per
    /// write. Caching it for the life of the handle is sound because the set
    /// only ever *shrinks*: nothing writes to a legacy collection, and the
    /// converging delete is itself filtered through this set, so it can never
    /// create the directory it is gated on. A post-cutover home resolves to an
    /// empty set and keeps the plain-put fast path, untouched.
    legacy_collections_on_disk: std::sync::OnceLock<std::collections::HashSet<String>>,
}

/// Last Store-backed [`NamespacedStore`] implementation.
#[derive(Clone)]
pub struct LastStoreNamespacedStore {
    store: Arc<LastStore>,
    high_water: Option<Arc<LastStoreHighWaterFile>>,
    /// (path, mtime, len) → sha memo so a backup walk only re-hashes chunks
    /// whose file identity changed. Persisted next to the high-water marker
    /// when one is configured; process-local otherwise.
    chunk_sha_memo: Arc<backup_manifest::ChunkShaMemo>,
    /// Serialize compaction provenance, manifest cuts, and confirmed commits.
    atom_retirement_lock: Arc<std::sync::Mutex<()>>,
    /// Shared logical resident set for every namespace on this LastStore.
    logical: Arc<Mutex<LogicalResidentSet>>,
}
