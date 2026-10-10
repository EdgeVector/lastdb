//! Atom domain store.
//!
//! Owns the main storage namespace where atoms, molecules, and
//! mutation-event history keys live, plus a derived schema index namespace
//! for list-by-schema markers. External callers use
//! `DbOperations::atoms()` to reach these operations.
//!
//! Layout:
//! - [`types`] — molecule/key types
//! - [`helpers`] — pure key/order/page-index helpers + schema index codec
//! - [`molecules`] — per-key molecule store/load/delete
//! - [`filter`] — filtered loads + page index
//! - [`atoms`] — atom CRUD, history, schema listing
//! - [`tests`] — unit tests (`cfg(test)`)
//! - [`paged_1d_cost_tests`] — what a paged 1-D read costs in cold group
//!   loads (`cfg(test)`)
//! - [`hash_range_write_cost_tests`] — what one constant-partition HashRange
//!   write costs in a cold, populated HashGroup home, and what one primary
//!   page read costs (`cfg(test)`)

pub(crate) mod atom_ref_edges;
mod atom_ref_gc;
mod atoms;
mod blob_ref_edges;
mod codec;
mod construct;
mod content_maintenance;
mod delete_barriers;
mod dropped_schema_reap;
mod filter;
mod hard_erase_journal;
mod helpers;
mod keep_small;
mod key_encoding;
mod locks;
mod molecule_ref_edges;
mod molecules;
mod tip_version_backrefs;
mod types;

pub use atom_ref_edges::{
    AtomLiveRefCount, AtomRefAuditReport, AtomRefBackfillPhase, AtomRefBackfillReport,
    AtomRefBackfillStatus, AtomRefCrashResumeProof, AtomRefEdge, AtomRefEdgeLookup,
    AtomRefEdgeType, AtomRefHistoryUpgrade, AtomRefMoleculeManifest, AtomRefV1DrainReport,
    AtomRefV1DrainStatus, AtomRefV2AuditReport, AtomRefV2Count, AtomRefV2LookupBenchmark,
    AtomRefV2MoleculeAuditReport, AtomRefV2Transition, PendingAtomRef,
    ATOM_REF_MANIFEST_VERSION_HISTORY, ATOM_REF_MANIFEST_VERSION_TIPS, ATOM_REF_V2_ACTIVE_MARKER,
};
pub use atom_ref_gc::{
    AtomGcAuditDecision, AtomGcAuditResult, AtomGcCandidate, AtomGcCandidatePage,
    AtomGcReapOptions, AtomGcReapReport, DEFAULT_ATOM_GC_GRACE_WINDOW,
};
pub use blob_ref_edges::{BlobRefCompleteness, BlobRefEdge, BlobRefLookup, BLOB_REF_COMPLETE_KEY};
pub use content_maintenance::{
    RecompressAtomContentReport, RepackAtomContentReport, ResealAtomContentStats,
};
pub use dropped_schema_reap::{
    DroppedSchemaReapCursor, DroppedSchemaReapPhase, DroppedSchemaReapReport,
    DroppedSchemaTipProbe, DroppedSchemaTipProof, MAX_DROPPED_SCHEMA_REAP_OPS,
};
/// Retired derived-index prefixes the reclaim path may delete, derived from
/// this build's `HASH_RANGE_*_ENABLED` flags.
pub use helpers::{retired_index_reclaim_prefixes, retired_index_reclaim_prefixes_for};
pub use molecule_ref_edges::{
    MoleculeRefCompleteness, MoleculeRefEdge, MoleculeRefLookup, MOLECULE_REF_COMPLETE_KEY,
};
/// Full `mk:` prefix-scan counter (test/ops surface for zero-yield purge plan cost).
pub use molecules::mk_full_scans;
#[cfg(feature = "sharing")]
pub(crate) use molecules::MoleculeKeyDomain;
pub use tip_version_backrefs::{
    TipVersionBackref, TipVersionBackrefLookup, TipVersionBackrefReindexReport,
    TipVersionBackrefReindexStatus,
};
pub use types::MoleculeData;
#[cfg(feature = "cloud-sync")]
pub(crate) use types::MoleculeHeader;
pub(crate) use types::{
    ChangedKey, FilterLayout, HashKeyLookupRecord, MoleculeGenerationDelete,
    MoleculeGenerationPointer, MoleculeGenerationSlot, OneDSlot, PerKeyRecord,
};

use crate::{
    schema::types::field::build_storage_key,
    storage::{
        traits::{KvStore, NamespacedStore},
        TypedKvStore,
    },
};
use std::{
    collections::HashSet,
    hash::{DefaultHasher, Hash, Hasher},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use super::keep_small::{
    KeepSmallHardEraseDebit, KeepSmallHardEraseTotals, KeepSmallMeters, KeepSmallSchemaShard,
    MeterDomainTrust, MeterTrustPayload,
};

use helpers::schema_index_codec;

async fn lock_target_stripes(
    locks: &[Arc<tokio::sync::Mutex<()>>],
    target_ids: &[String],
) -> Vec<tokio::sync::OwnedMutexGuard<()>> {
    if target_ids.is_empty() {
        return Vec::new();
    }
    let mut stripes = target_ids
        .iter()
        .map(|target| {
            let mut hasher = DefaultHasher::new();
            target.hash(&mut hasher);
            hasher.finish() as usize % locks.len()
        })
        .collect::<Vec<_>>();
    stripes.sort_unstable();
    stripes.dedup();
    let mut guards = Vec::with_capacity(stripes.len());
    for stripe in stripes {
        guards.push(Arc::clone(&locks[stripe]).lock_owned().await);
    }
    guards
}

type ExactTipLocks =
    Arc<std::sync::Mutex<std::collections::HashMap<String, Arc<tokio::sync::Mutex<()>>>>>;

/// Domain store for atoms, molecules, mutation events, and sync conflicts.
///
/// Backed by the `main` namespace.
#[derive(Clone)]
pub struct AtomStore {
    main_store: Arc<TypedKvStore<dyn KvStore>>,
    schema_index_store: Arc<TypedKvStore<dyn KvStore>>,
    /// Owner namespace factory for physical-plane maintenance.
    ///
    /// Legacy constructors leave this unset. Production construction and the
    /// drain tests attach it explicitly.
    namespaced_store: Option<Arc<dyn NamespacedStore>>,
    resident_graph: Option<Arc<crate::resident::ResidentGraph>>,
    /// When set (plain packaging homes), atom `content` is sealed at rest
    /// while metadata stays plaintext. `None` leaves content unsealed (tests
    /// / legacy dual-read of plain content).
    content_key: Option<[u8; 32]>,
    /// New-write format for nested atom content. Readers accept both formats.
    atom_content_binary: bool,
    /// HashKey API → storage encoding (default plain; see design-lastdb-hashkey-blind-v1).
    key_codec: crate::atom::MoleculeKeyCodec,
    /// Durable bundle resolver. `None` keeps legacy/test construction intact.
    molecule_keys: Option<crate::db_operations::MoleculeKeyStore>,
    /// Atom body storage-key encoding (default [`crate::atom::AtomKeyEncoding::Flat`];
    /// see `design-lastdb-atom-key-partition-locality`).
    atom_keys_partition_prefixed: Arc<AtomicBool>,
    /// Cache successful marker checks by storage prefix. Completion markers
    /// are immutable during one process lifetime after read cutover.
    atom_ref_v2_read_ready: Arc<std::sync::Mutex<std::collections::HashSet<Option<String>>>>,
    /// Per-molecule commit guards: the SHORT critical section that makes
    /// (read `moc:` → build items → commit batch) atomic for one molecule.
    ///
    /// The molecule order log is a **dense** sequence — `mord:{M}:{seq}` with
    /// `moc:{M}` as its length — so the base is read at persist time and
    /// re-stamped in the same batch. Two writers interleaved between that read
    /// and that put would stamp the same seq and the second would clobber the
    /// first, silently: both `mk:` records survive, and only `SampleN`, which
    /// walks the log, sees the hole. The molecule header (`version`,
    /// `updated_at`) rides the same batch and is molecule-wide state too, so it
    /// is covered by the same guard rather than a second one.
    ///
    /// This is deliberately NOT the write gate. The long-held gate is
    /// `MutationManager::acquire_molecule_write_locks`, now per-`(molecule,
    /// hash, range)`; this one is taken *inside* the durable put and released
    /// as soon as the batch lands. Lock order is always key-then-molecule, so
    /// the two cannot deadlock.
    molecule_commit_locks:
        Arc<std::sync::Mutex<std::collections::HashMap<String, Arc<tokio::sync::RwLock<()>>>>>,
    /// Exact durable-tip locks. These make the read/compare/put winner rule
    /// atomic per `mk:` row without coupling writes to different slots.
    tip_commit_locks: ExactTipLocks,
    /// Exact memory-publication locks. A normal Put does not wait for a durable flush.
    tip_publication_locks: ExactTipLocks,
    /// A normal Delete enters this map before its memory ack. Peer replay and
    /// delayed local flushes consult it under the exact tip commit lock.
    pending_delete_barriers: Arc<
        std::sync::Mutex<
            std::collections::HashMap<String, crate::atom::delete_barrier::DeleteBarrier>,
        >,
    >,
    /// Incremental live-budget / churn meters. Point-read cheap.
    keep_small: Arc<KeepSmallMeters>,
    /// Metadata namespace used to persist [`KeepSmallMeters`] across reopen.
    keep_small_persist: Option<Arc<TypedKvStore<dyn KvStore>>>,
    /// Debounce clock for the keep-small projection: when the last durable
    /// write landed. `None` until the first write, so a fresh store persists
    /// once immediately and a reopen always sees a snapshot.
    keep_small_last_persist: Arc<std::sync::Mutex<Option<Instant>>>,
    /// Meters have changed since the last durable write. Set by every
    /// debounced call, cleared by the write that lands them.
    keep_small_dirty: Arc<AtomicBool>,
    /// Set by [`AtomStore::flush_keep_small_for_clean_stop`]; a metered write
    /// that lands afterwards persists at once with `clean_stop = false`.
    keep_small_clean_stop_written: Arc<AtomicBool>,
    /// Serialize whole-snapshot writes so an older export cannot land after a
    /// newer export under concurrent molecule commits.
    keep_small_persist_lock: Arc<tokio::sync::Mutex<()>>,
    /// Small durable cumulative debits since rollout. Snapshot writes use
    /// this value as a checkpoint under the persist lock.
    keep_small_hard_erase_totals: Arc<std::sync::Mutex<KeepSmallHardEraseTotals>>,
    /// Commit sequence is assigned under the snapshot persist lock.
    keep_small_hard_erase_next_seq: Arc<AtomicU64>,
    /// Highest committed debit applied to the live projection.
    keep_small_hard_erase_applied_seq: Arc<AtomicU64>,
    /// A failed or ambiguous journal commit blocks new deletes until repair.
    keep_small_hard_erase_fenced: Arc<AtomicBool>,
    /// A dirty boot left journal rows that a full store repair must resolve.
    keep_small_hard_erase_replay_skipped: Arc<AtomicBool>,
    /// Incremented when a new destructive intent becomes durable.
    keep_small_hard_erase_mutation_epoch: Arc<AtomicU64>,
    /// Current-process destructive intents that a repair must not cross.
    keep_small_hard_erase_pending: Arc<AtomicU64>,
    /// Pending intents found on boot. Only a full repair may retire them.
    keep_small_hard_erase_orphaned: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    /// Automatic atom-GC generation whose reference markers guard deletes.
    /// Zero means that no bounded delete sweep is active.
    automatic_gc_atoms_generation: Arc<AtomicU64>,
    /// Fixed lock stripes serialize body writes and automatic deletes without
    /// retaining one process object for every atom UUID in a full sweep.
    automatic_gc_atom_locks: Arc<Vec<Arc<tokio::sync::Mutex<()>>>>,
    /// Fixed stripes serialize read-modify-write updates of durable atom
    /// reference counts. They are separate from automatic-GC locks because a
    /// write can hold both sets while a collection sweep is active.
    atom_ref_count_locks: Arc<Vec<Arc<tokio::sync::Mutex<()>>>>,
    /// Serializes the one exact catalog-transition recovery registry row.
    catalog_atom_ref_transition_lock: Arc<tokio::sync::Mutex<()>>,
    /// Fixed target gates for molecule source transitions and reclaim.
    molecule_liveness_locks: Arc<Vec<Arc<tokio::sync::Mutex<()>>>>,
    /// Fixed target gates for blob source transitions and reclaim.
    blob_liveness_locks: Arc<Vec<Arc<tokio::sync::Mutex<()>>>>,
    /// Scopes this store's entries in the process-wide legacy-history memo.
    ///
    /// The memo caches "this `history:{mol}:` prefix is empty", and molecule
    /// uuids are `sha256("{schema}:{field}")` — deterministic, so two stores
    /// sharing a schema and field name derive the same prefix. Without this id
    /// one store's observation would answer another's question. See
    /// [`crate::atom::legacy_history_memo`].
    store_id: crate::atom::legacy_history_memo::StoreId,
}

impl AtomStore {
    /// This store's identity in the legacy-history memo.
    #[must_use]
    pub(crate) fn store_id(&self) -> crate::atom::legacy_history_memo::StoreId {
        self.store_id
    }

    /// HashKey encoding chokepoint (API hash → storage segment).
    #[must_use]
    pub(crate) fn key_codec(&self) -> &crate::atom::MoleculeKeyCodec {
        &self.key_codec
    }

    /// Map API HashKey to storage form (identity when encoding=plain).
    pub(crate) fn storage_hash(
        &self,
        molecule_uuid: &str,
        api_hash: &str,
    ) -> Result<String, crate::schema::SchemaError> {
        self.key_codec_for_molecule(molecule_uuid)
            .storage_hash(molecule_uuid, api_hash)
            .map_err(|e| crate::schema::SchemaError::InvalidData(e.to_string()))
    }

    /// Map API RangeKey to storage form (identity when range encoding=plain).
    pub(crate) fn storage_range(
        &self,
        molecule_uuid: &str,
        api_range: &str,
    ) -> Result<String, crate::schema::SchemaError> {
        self.key_codec_for_molecule(molecule_uuid)
            .storage_range(molecule_uuid, api_range)
            .map_err(|e| crate::schema::SchemaError::InvalidData(e.to_string()))
    }

    pub(crate) fn key_codec_for_molecule(
        &self,
        molecule_uuid: &str,
    ) -> crate::atom::MoleculeKeyCodec {
        let Some(store) = self.molecule_keys.as_ref() else {
            return self.key_codec.clone();
        };
        let cache = store.cache();
        let guard = cache
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.get(molecule_uuid).map_or_else(
            || self.key_codec.clone(),
            |bundle| bundle.key_codec(&self.key_codec),
        )
    }

    /// Access the underlying typed KV store. Crate-internal helper for
    /// code paths that need generic typed access (field loading, conflicts,
    /// org purge). Prefer [`Self::get_atoms_by_storage_keys`] for atom bodies.
    pub(crate) fn raw(&self) -> &Arc<TypedKvStore<dyn KvStore>> {
        &self.main_store
    }

    /// Flush all pending writes in the atom namespace to durable storage.
    pub async fn flush(&self) -> Result<(), crate::storage::StorageError> {
        self.main_store.inner().flush().await?;
        self.schema_index_store.inner().flush().await
    }

    /// Storage key of a **legacy** `schemaidx:` record for `(schema, atom_uuid)`.
    /// Kept so hard-removal (`purge_record`) can still delete leftover index
    /// copies written by older binaries.
    pub(crate) fn schema_index_key(
        storage_prefix: Option<&str>,
        schema_name: &str,
        atom_uuid: &str,
    ) -> String {
        build_storage_key(
            storage_prefix,
            &schema_index_codec::record_key(schema_name, atom_uuid),
        )
    }
}

/// Invalidate again on success, failure, or cancellation of a durable write.
/// No lock is held across IO; a cold read during IO cannot certify old data.
struct CoverageMutation {
    graph: Arc<crate::resident::ResidentGraph>,
    molecules: std::collections::BTreeSet<String>,
}
impl Drop for CoverageMutation {
    fn drop(&mut self) {
        for molecule in &self.molecules {
            self.graph.invalidate_molecule_coverage(molecule);
        }
    }
}
