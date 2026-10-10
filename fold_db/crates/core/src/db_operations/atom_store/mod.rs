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
mod content_maintenance;
mod delete_barriers;
mod dropped_schema_reap;
mod filter;
mod hard_erase_journal;
mod helpers;
mod keep_small;
mod locks;
mod molecule_ref_edges;
mod molecules;
mod offline_decode;
pub mod reap_keys;
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
#[cfg(any(feature = "sharing", test))]
pub(crate) use molecules::MoleculeKeyDomain;
pub use reap_keys::{
    compact_tip_edge_key, molecule_ref_target_prefix, reap_history_source_keys,
    reap_legacy_atom_edge, reap_tip_source_keys, reap_tip_sources, reap_version_source_keys,
    ReapSourceEdgeKeys, ReapTipSource, TipEdgeKey,
};
pub use tip_version_backrefs::{
    TipVersionBackref, TipVersionBackrefLookup, TipVersionBackrefReindexReport,
    TipVersionBackrefReindexStatus,
};
pub use types::MoleculeData;
#[cfg(any(test, feature = "cloud-sync"))]
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
    /// Open only the atom operation surface over an existing store stack.
    ///
    /// Admin proofs and audit workers use this constructor when they must not
    /// hydrate the complete [`crate::FoldDB`] schema and resident graph.
    pub async fn from_namespaced_store(
        store: Arc<dyn NamespacedStore>,
    ) -> Result<Self, crate::schema::SchemaError> {
        let main_store = Arc::new(TypedKvStore::new(
            store.open_namespace("main").await.map_err(|error| {
                crate::schema::SchemaError::InvalidData(format!(
                    "open atom main namespace: {error}"
                ))
            })?,
        ));
        let schema_index_store = Arc::new(TypedKvStore::new(
            store
                .open_namespace("schema_index")
                .await
                .map_err(|error| {
                    crate::schema::SchemaError::InvalidData(format!(
                        "open atom schema-index namespace: {error}"
                    ))
                })?,
        ));
        Ok(Self::new(main_store, schema_index_store).with_namespaced_store(store))
    }

    pub(crate) fn new(
        main_store: Arc<TypedKvStore<dyn KvStore>>,
        schema_index_store: Arc<TypedKvStore<dyn KvStore>>,
    ) -> Self {
        Self::new_with_content_key(main_store, schema_index_store, None)
    }

    pub(crate) fn new_with_content_key(
        main_store: Arc<TypedKvStore<dyn KvStore>>,
        schema_index_store: Arc<TypedKvStore<dyn KvStore>>,
        content_key: Option<[u8; 32]>,
    ) -> Self {
        Self::new_with_content_and_hash_key_codec(
            main_store,
            schema_index_store,
            content_key,
            crate::atom::MoleculeKeyCodec::plain(),
        )
    }

    pub(crate) fn new_with_content_and_hash_key_codec(
        main_store: Arc<TypedKvStore<dyn KvStore>>,
        schema_index_store: Arc<TypedKvStore<dyn KvStore>>,
        content_key: Option<[u8; 32]>,
        key_codec: crate::atom::MoleculeKeyCodec,
    ) -> Self {
        Self {
            main_store,
            schema_index_store,
            namespaced_store: None,
            resident_graph: None,
            content_key,
            atom_content_binary: crate::atom::atom_content_binary_enabled(),
            key_codec,
            molecule_keys: None,
            atom_keys_partition_prefixed: Arc::new(AtomicBool::new(
                crate::atom::AtomKeyEncoding::from_env_or_default().writes_partition_prefix(),
            )),
            atom_ref_v2_read_ready: Arc::default(),
            molecule_commit_locks: Arc::default(),
            tip_commit_locks: Arc::default(),
            tip_publication_locks: Arc::default(),
            pending_delete_barriers: Arc::default(),
            keep_small: Arc::new(KeepSmallMeters::default()),
            keep_small_persist: None,
            keep_small_last_persist: Arc::default(),
            keep_small_dirty: Arc::default(),
            keep_small_clean_stop_written: Arc::default(),
            keep_small_persist_lock: Arc::default(),
            keep_small_hard_erase_totals: Arc::default(),
            keep_small_hard_erase_next_seq: Arc::default(),
            keep_small_hard_erase_applied_seq: Arc::default(),
            keep_small_hard_erase_fenced: Arc::default(),
            keep_small_hard_erase_replay_skipped: Arc::default(),
            keep_small_hard_erase_mutation_epoch: Arc::default(),
            keep_small_hard_erase_pending: Arc::default(),
            keep_small_hard_erase_orphaned: Arc::default(),
            automatic_gc_atoms_generation: Arc::default(),
            automatic_gc_atom_locks: Arc::new(
                (0..256)
                    .map(|_| Arc::new(tokio::sync::Mutex::new(())))
                    .collect(),
            ),
            atom_ref_count_locks: Arc::new(
                (0..256)
                    .map(|_| Arc::new(tokio::sync::Mutex::new(())))
                    .collect(),
            ),
            catalog_atom_ref_transition_lock: Arc::default(),
            molecule_liveness_locks: Arc::new(
                (0..256)
                    .map(|_| Arc::new(tokio::sync::Mutex::new(())))
                    .collect(),
            ),
            blob_liveness_locks: Arc::new(
                (0..256)
                    .map(|_| Arc::new(tokio::sync::Mutex::new(())))
                    .collect(),
            ),
            store_id: crate::atom::legacy_history_memo::StoreId::next(),
        }
    }

    #[must_use]
    pub(crate) fn with_molecule_keys(
        mut self,
        molecule_keys: crate::db_operations::MoleculeKeyStore,
    ) -> Self {
        self.molecule_keys = Some(molecule_keys);
        self
    }

    pub(crate) fn with_resident_graph(
        mut self,
        resident: Arc<crate::resident::ResidentGraph>,
    ) -> Self {
        self.resident_graph = Some(resident);
        self
    }

    fn invalidate_coverage_during<'a>(
        &self,
        molecules: impl IntoIterator<Item = &'a str>,
        storage_prefix: Option<&str>,
    ) -> Option<CoverageMutation> {
        if storage_prefix.is_some() {
            return None;
        }
        let graph = self.resident_graph.as_ref()?;
        let molecules: std::collections::BTreeSet<String> =
            molecules.into_iter().map(str::to_string).collect();
        for molecule in &molecules {
            graph.invalidate_molecule_coverage(molecule);
        }
        Some(CoverageMutation {
            graph: Arc::clone(graph),
            molecules,
        })
    }

    #[must_use]
    pub(crate) fn with_namespaced_store(mut self, store: Arc<dyn NamespacedStore>) -> Self {
        self.namespaced_store = Some(store);
        self
    }

    /// Compact reverse-edge writes are always on. The builder remains for
    /// older call sites.
    #[must_use]
    pub fn with_atom_ref_v2_dual_write(self, _enabled: bool) -> Self {
        self
    }

    /// Compact reverse-edge reads are always on. The builder remains for
    /// older call sites.
    #[must_use]
    pub fn with_atom_ref_v2_reads(self, _enabled: bool) -> Self {
        self
    }

    /// Compact-only writes are always on. The builder remains for older
    /// call sites.
    #[must_use]
    pub fn with_atom_ref_v2_only_writes(self, _enabled: bool) -> Self {
        self
    }

    /// Whether this store writes the compact reverse-edge plane.
    #[must_use]
    pub fn atom_ref_v2_dual_write_enabled(&self) -> bool {
        true
    }

    /// Whether this store accepts compact reverse-edge absence after proof.
    #[must_use]
    pub fn atom_ref_v2_reads_enabled(&self) -> bool {
        true
    }

    /// Whether this store omits legacy reverse-edge puts.
    #[must_use]
    pub fn atom_ref_v2_only_writes_enabled(&self) -> bool {
        true
    }

    /// This store's identity in the legacy-history memo.
    #[must_use]
    pub(crate) fn store_id(&self) -> crate::atom::legacy_history_memo::StoreId {
        self.store_id
    }

    /// Override the atom body key encoding (tests and the migration driver).
    ///
    /// Production resolves it at construction and refreshes it at the
    /// photograph-restore barrier. All clones change as one unit.
    #[must_use]
    pub(crate) fn with_atom_key_encoding(self, encoding: crate::atom::AtomKeyEncoding) -> Self {
        self.atom_keys_partition_prefixed
            .store(encoding.writes_partition_prefix(), Ordering::Release);
        self
    }

    /// Atom body key encoding for this store.
    #[must_use]
    pub(crate) fn atom_key_encoding(&self) -> crate::atom::AtomKeyEncoding {
        if self.atom_keys_partition_prefixed.load(Ordering::Acquire) {
            crate::atom::AtomKeyEncoding::PartitionPrefix
        } else {
            crate::atom::AtomKeyEncoding::Flat
        }
    }

    /// Re-read the durable atom-layout marker after a photograph restore.
    ///
    /// A bootstrap target starts empty, so construction resolves `flat` before
    /// the photograph installs its marker and partition-prefixed atom bodies.
    /// All clones share the atomic setting, which lets the serving store adopt
    /// the restored layout before it applies or reads the mutation-log tail.
    #[cfg_attr(not(any(feature = "cloud-sync", test)), allow(dead_code))]
    pub(crate) async fn refresh_boot_encoding_after_restore(
        &self,
    ) -> Result<(), crate::storage::StorageError> {
        self.clone().resolve_boot_encoding().await.map(|_| ())
    }

    /// Resolve the encoding this home is actually written under, stamp it, and
    /// refuse to serve a mismatch. Called at construction and after restore.
    ///
    /// Resolution order is `env override → home marker → Flat` (see
    /// [`crate::atom::atom_key_codec`]). Two things happen on top of it:
    ///
    /// - **Stamp.** Resolving to `PartitionPrefix` records that fact at
    ///   [`ATOM_KEY_ENCODING_MARKER_KEY`], so the *next* boot of this home does
    ///   not depend on an environment variable surviving. One idempotent point
    ///   write, only when the marker is missing or disagrees.
    /// - **Gate.** Resolving to `Flat` probes for prefixed keys and returns an
    ///   error if any exist, instead of serving short pages. The doc comment
    ///   this replaces had it backwards — "an unreadable home is a worse failure
    ///   than an unapplied optimization" is true for a *flat* home, but on a
    ///   migrated home falling back to `Flat` is what *creates* the unreadable
    ///   home. After `--remove-flat` there is no flat key left at all, so the
    ///   same boot would read the whole store as empty.
    ///
    /// A fresh or flat home pays one bounded prefix probe that matches nothing.
    ///
    /// **Known limit:** the probe covers the personal namespace (`atom:mk:`).
    /// Org-scoped bodies live under `{storage_prefix}:atom:mk:` and are not
    /// reachable by a single prefix scan, and the org prefixes are not known at
    /// this point in boot. The marker — which is store-wide — is the mechanism
    /// that covers them; the probe is the backstop for homes migrated before
    /// this marker existed, which is exactly the unprefixed case (the primary's
    /// rekey ran with `storage_prefix: None`).
    pub(crate) async fn resolve_boot_encoding(self) -> Result<Self, crate::storage::StorageError> {
        use crate::atom::atom_key_codec::ATOM_KEY_ENCODING_ALLOW_FLAT_ENV;

        let allow_flat = env_flag::var_truthy(ATOM_KEY_ENCODING_ALLOW_FLAT_ENV);
        self.resolve_boot_encoding_with(crate::atom::AtomKeyEncoding::from_env(), allow_flat)
            .await
    }

    /// [`Self::resolve_boot_encoding`] with the two environment reads lifted
    /// into parameters.
    ///
    /// The seam exists for the tests: `std::env::set_var` is process-global and
    /// these tests run in parallel with every other test in the crate, so a test
    /// that set `LASTDB_ATOM_KEY_ENCODING` to prove one boot's behaviour would
    /// silently change another test's store addressing. The decision under test
    /// is "given what the environment and the home each said, what does this
    /// boot do" — which is exactly this signature.
    pub(crate) async fn resolve_boot_encoding_with(
        self,
        env_override: Option<crate::atom::AtomKeyEncoding>,
        allow_flat_on_prefixed_home: bool,
    ) -> Result<Self, crate::storage::StorageError> {
        use crate::atom::{
            atom_key_codec::{
                AtomKeyEncodingMarker, ATOM_KEY_ENCODING_ALLOW_FLAT_ENV, ATOM_KEY_ENCODING_ENV,
                ATOM_KEY_ENCODING_MARKER_KEY, ATOM_PREFIX,
            },
            molecule_key_codec, AtomKeyEncoding,
        };

        let marker = self.load_atom_key_encoding_marker().await?;
        let (encoding, source) = match env_override {
            Some(encoding) => (encoding, ATOM_KEY_ENCODING_ENV),
            None => match marker {
                Some(encoding) => (encoding, ATOM_KEY_ENCODING_MARKER_KEY),
                None => (AtomKeyEncoding::default(), "default"),
            },
        };
        tracing::info!(
            ?encoding,
            source,
            marker = ?marker,
            "resolved atom body storage-key encoding"
        );

        if encoding.writes_partition_prefix() {
            if marker != Some(encoding) {
                let stamp = AtomKeyEncodingMarker {
                    version: 1,
                    encoding: encoding.as_marker_str().to_string(),
                    stamped_at_unix: crate::clock::unix_secs(),
                };
                self.main_store
                    .put_item(ATOM_KEY_ENCODING_MARKER_KEY, &stamp)
                    .await?;
                tracing::info!(
                    key = ATOM_KEY_ENCODING_MARKER_KEY,
                    encoding = encoding.as_marker_str(),
                    "stamped the atom key encoding in the home; this boot no longer \
                     depends on {ATOM_KEY_ENCODING_ENV} surviving"
                );
            }
        } else {
            // One bounded probe: does anything in this home carry a partition
            // prefix? Empty on a fresh or flat home, so it costs a prefix scan
            // that matches nothing.
            let probe = format!("{ATOM_PREFIX}{}", molecule_key_codec::MK_PREFIX);
            let prefixed = self
                .main_store
                .inner()
                .scan_prefix_paged(probe.as_bytes(), 1)
                .await?;
            if let Some((key, _)) = prefixed.first() {
                let sample = String::from_utf8_lossy(key).into_owned();
                if allow_flat_on_prefixed_home {
                    tracing::error!(
                        sample_key = %sample,
                        "{ATOM_KEY_ENCODING_ALLOW_FLAT_ENV} is set: serving a home that holds \
                         partition-prefixed atom bodies under the flat encoding. Reads will \
                         SILENTLY OMIT every prefixed-only body."
                    );
                } else {
                    return Err(crate::storage::StorageError::ConfigurationError(format!(
                        "atom key encoding mismatch: this home holds partition-prefixed atom \
                         bodies (e.g. {sample}) but the boot resolved to `flat` (source: \
                         {source}). Serving would silently omit every body that has no flat \
                         key. Set {ATOM_KEY_ENCODING_ENV}=partition_prefix (this boot will then \
                         stamp {ATOM_KEY_ENCODING_MARKER_KEY} so future boots do not need it), \
                         or set {ATOM_KEY_ENCODING_ALLOW_FLAT_ENV}=1 to accept a knowingly \
                         partial view."
                    )));
                }
            }
        }

        self.atom_keys_partition_prefixed
            .store(encoding.writes_partition_prefix(), Ordering::Release);
        self.hydrate_automatic_gc_atoms_generation().await?;
        Ok(self)
    }

    /// The encoding recorded in the home, if any. A marker that fails to decode
    /// is treated as absent (with a warn) rather than as `flat`: the boot gate
    /// below still catches a migrated home, so an unreadable marker degrades to
    /// "refuse", not to "serve short".
    async fn load_atom_key_encoding_marker(
        &self,
    ) -> Result<Option<crate::atom::AtomKeyEncoding>, crate::storage::StorageError> {
        use crate::atom::atom_key_codec::ATOM_KEY_ENCODING_MARKER_KEY;

        let raw = match self
            .main_store
            .get_item::<crate::atom::AtomKeyEncodingMarker>(ATOM_KEY_ENCODING_MARKER_KEY)
            .await
        {
            Ok(raw) => raw,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    key = ATOM_KEY_ENCODING_MARKER_KEY,
                    "atom key encoding marker did not decode; treating as absent"
                );
                None
            }
        };
        let Some(raw) = raw else { return Ok(None) };
        let parsed = crate::atom::AtomKeyEncoding::from_marker_str(&raw.encoding);
        if parsed.is_none() {
            tracing::warn!(
                encoding = %raw.encoding,
                key = ATOM_KEY_ENCODING_MARKER_KEY,
                "atom key encoding marker names an unknown encoding; treating as absent"
            );
        }
        Ok(parsed)
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

    /// Warm an existing molecule key bundle without creating storage state.
    /// Missing bundles are legacy molecules and keep the node-level codec.
    pub(crate) async fn load_molecule_key_bundle(
        &self,
        molecule_uuid: &str,
    ) -> Result<(), crate::schema::SchemaError> {
        if let Some(store) = self.molecule_keys.as_ref() {
            store.load(molecule_uuid).await.map_err(|error| {
                crate::schema::SchemaError::InvalidData(format!(
                    "load molecule key bundle for {molecule_uuid}: {error}"
                ))
            })?;
        }
        Ok(())
    }

    /// Open atom JSON from durable storage (content field dual-read).
    pub(crate) async fn open_atom_value(
        &self,
        mut atom_value: serde_json::Value,
    ) -> Result<serde_json::Value, crate::schema::SchemaError> {
        let bundle_key = self.take_atom_bundle_key(&mut atom_value).await?;
        if let Some(key) = bundle_key.as_ref().or(self.content_key.as_ref()) {
            crate::atom::open_atom_json(key, &mut atom_value).map_err(|e| {
                crate::schema::SchemaError::InvalidData(format!("open atom content: {e}"))
            })?;
        }
        Ok(atom_value)
    }

    async fn take_atom_bundle_key(
        &self,
        atom_header: &mut serde_json::Value,
    ) -> Result<Option<[u8; 32]>, crate::schema::SchemaError> {
        let bundle_molecule = atom_header
            .as_object_mut()
            .and_then(|object| object.remove("molecule_key_bundle"))
            .and_then(|value| value.as_str().map(str::to_owned));
        let (Some(store), Some(molecule_uuid)) =
            (self.molecule_keys.as_ref(), bundle_molecule.as_deref())
        else {
            return Ok(None);
        };
        Ok(Some(
            store
                .load(molecule_uuid)
                .await
                .map_err(|error| {
                    crate::schema::SchemaError::InvalidData(format!(
                        "load molecule key bundle {molecule_uuid}: {error}"
                    ))
                })?
                .ok_or_else(|| {
                    crate::schema::SchemaError::InvalidData(format!(
                        "atom names missing molecule key bundle {molecule_uuid}"
                    ))
                })?
                .content_dek(),
        ))
    }

    pub(crate) async fn decode_atom(
        &self,
        atom_value: serde_json::Value,
    ) -> Result<crate::atom::Atom, crate::schema::SchemaError> {
        let opened = self.open_atom_value(atom_value).await?;
        serde_json::from_value(opened)
            .map_err(|e| crate::schema::SchemaError::InvalidData(format!("decode atom: {e}")))
    }

    /// Decode either a legacy JSON atom row or the binary `ATB:` container.
    pub(crate) async fn decode_atom_bytes(
        &self,
        stored: &[u8],
    ) -> Result<crate::atom::Atom, crate::schema::SchemaError> {
        if let Some((mut header, _)) = crate::atom::parse_atom_binary_row(stored).map_err(|e| {
            crate::schema::SchemaError::InvalidData(format!("decode binary atom row: {e}"))
        })? {
            let bundle_key = self.take_atom_bundle_key(&mut header).await?;
            let key = bundle_key
                .as_ref()
                .or(self.content_key.as_ref())
                .ok_or_else(|| {
                    crate::schema::SchemaError::InvalidData(
                        "binary atom row requires an atom content key".into(),
                    )
                })?;
            let mut opened = crate::atom::open_atom_binary_row(key, stored)
                .map_err(|e| {
                    crate::schema::SchemaError::InvalidData(format!(
                        "open binary atom content: {e}"
                    ))
                })?
                .expect("binary parser already matched");
            if let Some(object) = opened.as_object_mut() {
                object.remove("molecule_key_bundle");
            }
            return serde_json::from_value(opened)
                .map_err(|e| crate::schema::SchemaError::InvalidData(format!("decode atom: {e}")));
        }
        let value = serde_json::from_slice(stored).map_err(|e| {
            crate::schema::SchemaError::InvalidData(format!("decode atom row JSON: {e}"))
        })?;
        self.decode_atom(value).await
    }

    /// Encode an atom for durable storage, using the binary content container
    /// when its rollout switch is on.
    pub(crate) async fn encode_atom_bytes(
        &self,
        atom: &crate::atom::Atom,
        molecule_uuid: Option<&str>,
    ) -> Result<Vec<u8>, crate::schema::SchemaError> {
        self.encode_atom_bytes_with_binary(atom, molecule_uuid, self.atom_content_binary)
            .await
    }

    pub(crate) async fn encode_atom_bytes_with_binary(
        &self,
        atom: &crate::atom::Atom,
        molecule_uuid: Option<&str>,
        binary: bool,
    ) -> Result<Vec<u8>, crate::schema::SchemaError> {
        let mut value = serde_json::to_value(atom)
            .map_err(|e| crate::schema::SchemaError::InvalidData(format!("serialize atom: {e}")))?;
        let bundle_key = if let (Some(store), Some(molecule_uuid)) =
            (self.molecule_keys.as_ref(), molecule_uuid)
        {
            if store.is_enabled() {
                let bundle = store.ensure(molecule_uuid).await.map_err(|error| {
                    crate::schema::SchemaError::InvalidData(format!(
                        "ensure molecule key bundle {molecule_uuid}: {error}"
                    ))
                })?;
                value
                    .as_object_mut()
                    .expect("Atom serializes as an object")
                    .insert(
                        "molecule_key_bundle".to_string(),
                        serde_json::Value::String(molecule_uuid.to_string()),
                    );
                Some(bundle.content_dek())
            } else {
                None
            }
        } else {
            None
        };
        let key = bundle_key.as_ref().or(self.content_key.as_ref());
        if binary {
            let key = key.ok_or_else(|| {
                crate::schema::SchemaError::InvalidData(
                    "binary atom row requires an atom content key".into(),
                )
            })?;
            return crate::atom::seal_atom_binary_row(key, &value).map_err(|error| {
                crate::schema::SchemaError::InvalidData(format!(
                    "seal binary atom content: {error}"
                ))
            });
        }
        if let Some(key) = key {
            crate::atom::seal_atom_json(key, &mut value).map_err(|error| {
                crate::schema::SchemaError::InvalidData(format!("seal atom content: {error}"))
            })?;
        }
        serde_json::to_vec(&value).map_err(|e| {
            crate::schema::SchemaError::InvalidData(format!("serialize stored atom: {e}"))
        })
    }

    /// Batch-load atoms by full storage keys (`atom:{uuid}` or
    /// `{prefix}:atom:{uuid}`), opening content-seal when configured.
    ///
    /// **Must** be used instead of `raw().get_items::<Atom>(…)` — deserializing
    /// sealed rows straight into [`Atom`] leaves `content` as `ENC:…` ciphertext
    /// and leaks into query / hash-key paths.
    pub async fn get_atoms_by_storage_keys(
        &self,
        keys: &[String],
    ) -> Result<Vec<Option<crate::atom::Atom>>, crate::schema::SchemaError> {
        let forms: Vec<Vec<String>> = keys
            .iter()
            .map(|key| crate::kind_partition::read_forms(key))
            .collect();
        let lookup: Vec<Vec<u8>> = forms
            .iter()
            .flatten()
            .map(|key| key.as_bytes().to_vec())
            .collect();
        let raw = self
            .main_store
            .inner()
            .get_many(lookup)
            .await
            .map_err(|e| {
                crate::schema::SchemaError::InvalidData(format!("Failed to fetch atom batch: {e}"))
            })?;
        let mut raw_iter = raw.into_iter();
        let mut out = Vec::with_capacity(keys.len());
        for key_forms in forms {
            let mut hit = None;
            for _ in &key_forms {
                let item = raw_iter.next().flatten();
                if hit.is_none() {
                    hit = item;
                }
            }
            out.push(match hit {
                Some(v) => Some(self.decode_atom_bytes(&v).await?),
                None => None,
            });
        }
        Ok(out)
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
