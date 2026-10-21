//! Atom domain store.
//!
//! Owns the main storage namespace where atoms, molecules, and
//! mutation-event history keys live, plus a derived schema index namespace
//! for list-by-schema markers. External callers use
//! `DbOperations::atoms()` to reach these operations.
//!
//! Layout:
//! - [`types`] — molecule/atom storage types
//! - [`helpers`] — pure key/order/page-index helpers + schema index codec
//! - [`construct`] — `AtomStore` constructors and builder hooks
//! - [`counter`] — the atom counter planner: the only writer of tips, bodies and counters
//! - [`retention`] — queue, slot directory and release of kept conflict records
//! - [`time_queue`] — the time-bucketed queue layout, reader and retirement
//! - [`codec`] — atom body encode/decode and storage-key reads
//! - [`molecules`] — per-key molecule store/load/delete
//! - [`filter`] — filtered loads + page index
//! - [`atoms`] — atom CRUD, history, schema listing
//! - [`locks`] — molecule, tip and delete-barrier locks
//! - [`delete_barriers`] — delete-barrier reads and writes
//! - [`blob_counter`] — the blob counter, its dwell queue and the dwell pass
//! - [`molecule_ref_edges`], [`molecule_bindings`], [`molecule_counters`] — molecule
//!   binding records and the molecule counters
//! - [`fresh_home_edges`] — key format marker and root proofs of an empty home
//! - [`hard_erase_journal`], [`keep_small`] — maintenance passes

mod atoms;
pub(crate) mod blob_counter;
mod bounded_range;
mod catalog_bindings;
mod codec;
mod conflict_record;
mod construct;
pub(crate) mod counter;
mod delete_barriers;
mod filter;
mod fresh_home_edges;
mod hard_erase_journal;
mod helpers;
mod keep_small;
mod locks;
mod molecule_bindings;
mod molecule_counters;
mod molecule_drain;
mod molecule_ref_edges;
mod molecules;
mod retention;
mod retired_intent_retention;
mod schema_keys;
mod time_queue;
mod types;

pub(crate) use atoms::SealedAtom;
pub use blob_counter::{
    BlobCounter, BlobCounterReport, BlobCounterView, BlobRecountReport, DwellCursor, DwellGuard,
    DwellStep, NetStep, DWELL_CHUNK,
};
pub use bounded_range::WalkStats;
pub(crate) use bounded_range::{unsealed_run_error, Progress, RangeWalk, WalkTotals};
pub(crate) use conflict_record::{DurableTipDecision, TipConflict};
pub use counter::{
    counts_of_header, AtomCounterReport, AtomCounterView, Change, MoleculeCounts, MoleculeFault,
    RecountOptions, RecountReport, VerifyReport, VerifyRequest, VerifySummary,
};
pub(crate) use filter::SchemaKeyMembership;
pub(crate) use fresh_home_edges::CURRENT_NATIVE_INITIALIZATION_KEYS;
/// Historical derived-index prefixes the reclaim path may delete.
pub use helpers::retired_index_reclaim_prefixes;
pub use molecule_bindings::MoleculeBindings;
pub use molecule_counters::{molecule_view, MoleculeCountersView};
pub(crate) use molecule_drain::{
    valid_candidate_key as valid_drain_candidate_key, DrainCursor, DrainLimits,
    OutcomeKind as DrainOutcomeKind,
};
pub use molecule_drain::{MoleculeDrainReport, MoleculeDrainVerdict, RefusedMolecule};
pub use molecule_ref_edges::{MoleculeRefEdge, MoleculeRefLookup};
pub(crate) use molecules::MoleculeKeyDomain;
pub use retention::ConflictRetentionReport;
pub(crate) use retention::{record_digest, Lane, RecordSlot, RetentionCursor, StepOutcome};
pub(crate) use retired_intent_retention::{
    RetiredIntentRetentionGuard, RetiredIntentRetentionProof,
};
pub use types::MoleculeData;
pub(crate) use types::MoleculeHeader;
pub(crate) use types::{
    ChangedKey, FilterLayout, MoleculeGenerationDelete, MoleculeGenerationPointer,
    MoleculeGenerationSlot, MoleculeRangeForm, OneDSlot, PerKeyRecord,
};

use crate::storage::{
    traits::{KvStore, NamespacedStore},
    TypedKvStore,
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
    /// Owner namespace factory for physical-plane maintenance.
    ///
    /// Legacy constructors leave this unset. Production construction and the
    /// drain tests attach it explicitly.
    namespaced_store: Option<Arc<dyn NamespacedStore>>,
    /// Retired outcome roots commit exclusively; reclaim retains a shared fence
    /// through its final deletion. Every clone shares the same fence.
    retired_intent_retention_gate: Arc<tokio::sync::RwLock<()>>,
    /// Serialize protein membership selection and orphan reclamation across clones.
    protein_membership_gate: Arc<tokio::sync::Mutex<()>>,
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
    /// Totals and recent faults of the atom counter planner.
    counter_events: Arc<counter::CounterEvents>,
    /// Totals, the last queue reading and recent faults of the blob counters.
    blob_events: Arc<blob_counter::BlobEvents>,
    /// The retention window of kept conflict records and the janitor report.
    retention: Arc<retention::RetentionState>,
    /// The verifier: the run in progress, the last result and the watch of
    /// atoms that writers touch while a run reads. Memory only.
    counter_verify: Arc<counter::VerifyState>,
    /// The report and the wake signal of the molecule drain. Memory only.
    molecule_drain: Arc<molecule_drain::DrainState>,
    /// Bounded range reads made by the admin walks since this process started.
    range_walks: Arc<WalkTotals>,
    /// Sync device id of this node. Empty until a sync engine attaches. The
    /// tip decision treats a write from any other non-empty device as a write
    /// from a peer. See [`AtomStore::local_origin_device`].
    local_origin_device: Arc<std::sync::RwLock<String>>,
    /// Counts the record calls that wrote a conflict row. A reader that scans
    /// the conflict rows outside the write gate stamps its `mcc:` list only if
    /// this number did not change since before the scan. The stamp runs in a
    /// write scope, which orders it against every recorder.
    conflict_record_epoch: Arc<AtomicU64>,
    /// Fixed target gates for blob source transitions and reclaim.
    blob_liveness_locks: Arc<Vec<Arc<tokio::sync::Mutex<()>>>>,
    /// Scopes this store's entries in the process-wide legacy-history memo.
    ///
    /// The memo caches "this `history:{mol}:` prefix is empty", and molecule
    /// uuids are `sha256("{schema}:{field}")` — deterministic, so two stores
    /// sharing a schema and field name derive the same prefix. Without this id
    /// one store's observation would answer another's question. See
    /// [`crate::atom::history_event_memo`].
    store_id: crate::atom::history_event_memo::StoreId,
}

impl AtomStore {
    /// The planner report: bodies created and deleted, and counter faults.
    pub(crate) fn counter_report(&self) -> &counter::CounterEvents {
        &self.counter_events
    }

    /// The blob counter report: totals, the queue reading of the last dwell pass.
    #[must_use]
    pub fn blob_counter_report(&self) -> BlobCounterReport {
        self.blob_events.snapshot()
    }

    /// The retention window and the report of kept conflict records.
    pub(crate) fn retention_state(&self) -> &retention::RetentionState {
        &self.retention
    }

    /// The verifier state: the run in progress, the last result, the watch.
    pub(crate) fn counter_verify(&self) -> &counter::VerifyState {
        &self.counter_verify
    }

    /// Totals of the bounded range reads of the admin walks.
    pub(crate) fn range_walks(&self) -> &WalkTotals {
        &self.range_walks
    }

    /// This store's identity in the legacy-history memo.
    #[must_use]
    pub(crate) fn store_id(&self) -> crate::atom::history_event_memo::StoreId {
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
        self.key_codec_for_molecule(molecule_uuid)?
            .storage_hash(molecule_uuid, api_hash)
            .map_err(|e| crate::schema::SchemaError::InvalidData(e.to_string()))
    }

    /// Map API RangeKey to storage form (identity when range encoding=plain).
    pub(crate) fn storage_range(
        &self,
        molecule_uuid: &str,
        api_range: &str,
    ) -> Result<String, crate::schema::SchemaError> {
        self.key_codec_for_molecule(molecule_uuid)?
            .storage_range(molecule_uuid, api_range)
            .map_err(|e| crate::schema::SchemaError::InvalidData(e.to_string()))
    }

    pub(crate) fn key_codec_for_molecule(
        &self,
        molecule_uuid: &str,
    ) -> Result<crate::atom::MoleculeKeyCodec, crate::schema::SchemaError> {
        let Some(store) = self.molecule_keys.as_ref() else {
            return Ok(self.key_codec.clone());
        };
        Ok(store.cached_bundle(molecule_uuid)?.as_ref().map_or_else(
            || self.key_codec.clone(),
            |bundle| bundle.key_codec(&self.key_codec),
        ))
    }

    /// Access the underlying typed KV store. Crate-internal helper for
    /// code paths that need generic typed access (field loading, conflicts,
    /// org purge). Prefer [`Self::get_atoms_by_storage_keys`] for atom bodies.
    pub(crate) fn raw(&self) -> &Arc<TypedKvStore<dyn KvStore>> {
        &self.main_store
    }

    /// Flush all pending writes in the atom namespace to durable storage.
    pub async fn flush(&self) -> Result<(), crate::storage::StorageError> {
        self.main_store.inner().flush().await
    }
}
