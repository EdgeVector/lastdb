use std::path::PathBuf;
use std::sync::Arc;

use tracing::info;

use crate::db_operations::DbOperations;
use crate::schema::SchemaCore;
use crate::storage::{LastStoreNamespacedStore, StorageError};

use super::FoldDB;
use crate::fold_db_core::mutation_manager::MutationManager;
use crate::fold_db_core::query::QueryExecutor;
#[cfg(feature = "cloud-sync")]
use crate::fold_db_core::sync_coordinator::SyncCoordinator;

impl FoldDB {
    /// Creates a new FoldDB instance with the specified storage path.
    ///
    /// This is a convenience path for tests and other in-process callers
    /// that do not have a persistent node identity (e.g., ephemeral
    /// `tempdir` storage). It generates a fresh Ed25519 signing keypair
    /// on the fly. **Production callers must go through
    /// [`crate::fold_db_core::factory::create_fold_db`] and pass in a
    /// signer loaded from the node's persistent identity** — otherwise
    /// every boot produces a different signing key and molecule
    /// signatures will not match the node's public identity.
    pub async fn new(path: &str) -> Result<Self, StorageError> {
        let store = Arc::new(LastStoreNamespacedStore::open(std::path::Path::new(path))?);
        Self::initialize_from_store(
            store as Arc<dyn crate::storage::traits::NamespacedStore>,
            path,
            None,
        )
        .await
    }

    /// Create an embedded database whose store carries the durable
    /// high-water sidecar, the way the Mini node factory opens a home.
    ///
    /// The sidecar is where the store keeps its backup bookkeeping — the
    /// committed CSN high water and the pending purged-atom retirement
    /// record. Verbs that write that record, such as `compact --collection
    /// atoms`, refuse a store opened without it (`FoldDB::new`), so an
    /// in-process caller that wants the whole delete → reclaim chain opens
    /// through here. Layout: `<path>/laststore_high_water.json`, matching
    /// `high_water_path_for_store_root`.
    pub async fn new_with_high_water(path: &str) -> Result<Self, StorageError> {
        let root = std::path::Path::new(path);
        let store = Arc::new(LastStoreNamespacedStore::open_with_high_water(
            root,
            crate::storage::laststore::high_water_path_for_store_root(root),
        )?);
        Self::initialize_from_store(
            store as Arc<dyn crate::storage::traits::NamespacedStore>,
            path,
            None,
        )
        .await
    }

    /// Create an embedded database with durable per-molecule key bundles.
    ///
    /// The caller must supply the same wrap key each time it opens this path.
    pub async fn new_with_molecule_wrap_key(
        path: &str,
        molecule_wrap_key: [u8; 32],
    ) -> Result<Self, StorageError> {
        let store = Arc::new(LastStoreNamespacedStore::open(std::path::Path::new(path))?);
        Self::initialize_from_store(
            store as Arc<dyn crate::storage::traits::NamespacedStore>,
            path,
            Some(molecule_wrap_key),
        )
        .await
    }

    /// Generate a signing keypair for in-process / test callers that do
    /// not have a persistent node identity. Wraps the error so callers
    /// get a single failure mode.
    fn generate_ephemeral_signer() -> Result<Arc<crate::security::Ed25519KeyPair>, StorageError> {
        let keypair = crate::security::Ed25519KeyPair::generate().map_err(|e| {
            StorageError::BackendError(format!("ephemeral signer generation failed: {e}"))
        })?;
        Ok(Arc::new(keypair))
    }

    /// Test / in-process init: open a namespaced store path with an ephemeral signer.
    /// Production boots go through the factory → [`FoldDB::initialize_from_init`].
    async fn initialize_from_store(
        store: Arc<dyn crate::storage::traits::NamespacedStore>,
        db_path: &str,
        molecule_wrap_key: Option<[u8; 32]>,
    ) -> Result<Self, StorageError> {
        let signer = Self::generate_ephemeral_signer()?;
        Self::initialize_from_store_with_signer(store, db_path, molecule_wrap_key, signer).await
    }

    /// Reopen a test store with the same signer that a production node keeps
    /// across boots. Cold mutation tests must not silently rotate the author
    /// identity and reset its per-writer clock.
    pub(crate) async fn new_with_test_signer(
        path: &str,
        signer: Arc<crate::security::Ed25519KeyPair>,
    ) -> Result<Self, StorageError> {
        let store = Arc::new(LastStoreNamespacedStore::open(std::path::Path::new(path))?);
        Self::initialize_from_store_with_signer(
            store as Arc<dyn crate::storage::traits::NamespacedStore>,
            path,
            None,
            signer,
        )
        .await
    }

    async fn initialize_from_store_with_signer(
        store: Arc<dyn crate::storage::traits::NamespacedStore>,
        db_path: &str,
        molecule_wrap_key: Option<[u8; 32]>,
        signer: Arc<crate::security::Ed25519KeyPair>,
    ) -> Result<Self, StorageError> {
        tracing::info!(
            target: "fold_node::database",
            "Using DbOperations with storage abstraction layer (Last Store backend)"
        );

        #[cfg(feature = "cloud-sync")]
        let (store, mutation_log_capture) = {
            let mutation_log_capture =
                Arc::new(crate::sync::capture::MutationLogCaptureRouter::default());
            let store: Arc<dyn crate::storage::traits::NamespacedStore> = Arc::new(
                crate::sync::capture::MutationLogCaptureNamespacedStore::new(
                    store,
                    Arc::clone(&mutation_log_capture),
                ),
            );
            (store, Some(mutation_log_capture))
        };

        let db_ops = Arc::new(
            DbOperations::from_namespaced_store_with_atom_and_molecule_keys(
                store,
                None,
                crate::atom::MoleculeKeyCodec::plain(),
                molecule_wrap_key,
            )
            .await?,
        );

        tracing::info!(
            target: "fold_node::database",
            "Storage abstraction active - using Last Store backend"
        );

        Self::initialize_from_init(FoldDbInit {
            db_ops,
            db_path: db_path.to_string(),
            signer,
            search_outbox_inbox: None,
            #[cfg(feature = "cloud-sync")]
            mutation_log_capture,
            #[cfg(feature = "cloud-sync")]
            packing_slots: None,
        })
        .await
    }

    /// Restore apply path: wrap an already-opened LastStore (S0 just installed)
    /// with the serving MutationManager so [`SyncEngine::restore_mutation_log_after_s0`]
    /// can call [`FoldDB::set_sync_engine`].
    ///
    /// Molecule-key encodings match Mini (`from_env_or_default` + identity E2E
    /// index/ope keys). Capture is off: restore applies, it does not re-log.
    #[cfg(feature = "cloud-sync")]
    pub async fn for_restore(
        store: Arc<dyn crate::storage::traits::NamespacedStore>,
        db_path: &str,
        signer: Arc<crate::security::Ed25519KeyPair>,
        e2e_keys: &crate::crypto::E2eKeys,
    ) -> Result<Self, StorageError> {
        let hash_key_encoding = crate::atom::HashKeyEncoding::from_env_or_default();
        let range_key_encoding = crate::atom::RangeKeyEncoding::from_env_or_default();
        let hash_key_codec = crate::atom::MoleculeKeyCodec::with_encodings(
            hash_key_encoding,
            range_key_encoding,
            Some(e2e_keys.index_key()),
            Some(e2e_keys.ope_key()),
        );
        // Same at-rest ENC seam Mini factory uses for plain HashGroup homes.
        // Without it TypedKvStore parses ciphertext as JSON:
        // `Serialization error: expected value at line 1 column 1`.
        let crypto: Arc<dyn crate::crypto::CryptoProvider> = Arc::new(
            crate::crypto::LocalCryptoProvider::from_key(e2e_keys.encryption_key()),
        );
        let store: Arc<dyn crate::storage::traits::NamespacedStore> = Arc::new(
            crate::storage::EncryptingNamespacedStore::with_plaintext_namespaces(
                store,
                crypto,
                crate::storage::LASTSTORE_PLAINTEXT_NAMESPACES
                    .iter()
                    .copied()
                    .map(str::to_string)
                    .collect(),
            ),
        );
        // Mini HashGroup+Plain seals atom `content` under the account E2E
        // key (factory `atom_content_key`). Passing None writes plain-string
        // bodies; dest `LASTDB_ATOM_CONTENT_STRICT=1` then fails HashKey with
        // "atom content is not sealed … got plain-string".
        let db_ops = Arc::new(
            DbOperations::from_namespaced_store_with_atom_and_molecule_keys(
                store,
                Some(e2e_keys.encryption_key()),
                hash_key_codec,
                Some(e2e_keys.encryption_key()),
            )
            .await?,
        );
        Self::initialize_from_init(FoldDbInit {
            db_ops,
            db_path: db_path.to_string(),
            signer,
            search_outbox_inbox: None,
            mutation_log_capture: None,
            packing_slots: None,
        })
        .await
    }

    /// Production / factory initializer.
    ///
    /// `signer` is the Ed25519 keypair used to sign molecule mutations.
    /// It is shared with the sync engine so merged-molecule writes during
    /// replay carry the same node identity as direct writes via
    /// `MutationManager`. Production callers (via the factory) must load
    /// this from the node's persistent identity so signatures match the
    /// node's public key — see the module docs on [`FoldDB::new`] for why.
    pub async fn initialize_from_init(init: FoldDbInit) -> Result<Self, StorageError> {
        let FoldDbInit {
            db_ops,
            db_path: _db_path,
            signer,
            search_outbox_inbox,
            #[cfg(feature = "cloud-sync")]
            mutation_log_capture,
            #[cfg(feature = "cloud-sync")]
            packing_slots,
        } = init;
        // Initialize pending task tracker
        let pending_tasks = Arc::new(super::super::pending_task_tracker::PendingTaskTracker::new());

        let schema_manager = Arc::new(
            SchemaCore::new(Arc::clone(&db_ops))
                .await
                .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))?,
        );

        // Create QueryExecutor for handling all query operations
        let query_executor = QueryExecutor::new(&db_ops, Arc::clone(&schema_manager));
        info!("Created QueryExecutor for query operations");

        // Point-load the device clock once at boot. A mutation ACK must not
        // read this metadata row or flush its namespace on every request.
        let author_clock_state_key = crate::schema::types::author_clock::mutation_author_clock_key(
            &signer.public_key_base64(),
        );
        let author_clock_state = db_ops
            .metadata()
            .get_typed::<crate::schema::types::MutationAuthorClockState>(&author_clock_state_key)
            .await
            .map_err(|error| {
                StorageError::BackendError(format!(
                    "failed to load mutation author clock at boot: {error}"
                ))
            })?
            .unwrap_or_default();

        // Create MutationManager for handling all mutation operations.
        // The signer was loaded and validated by the caller (in
        // production, from the node's persistent identity). It is shared
        // with the sync engine so merged-molecule writes trace to the
        // same node identity as direct writes via `MutationManager`.
        let mutation_manager = Arc::new(MutationManager::new(
            Arc::clone(&db_ops),
            Arc::clone(&schema_manager),
            Arc::clone(&signer),
            Arc::clone(&pending_tasks),
            search_outbox_inbox,
            author_clock_state,
        ));
        #[cfg(feature = "cloud-sync")]
        if let Some(router) = mutation_log_capture.as_ref() {
            mutation_manager.set_capture_router(Arc::clone(router));
        }

        info!("Created MutationManager for mutation operations");

        // Memory-first mutations: skip sync fsync on finalize by default;
        // periodic flusher + shutdown cover durability (sled-like cadence).
        let flush_policy = super::super::mutation_flush::MutationFlushPolicy::from_env();
        let background_ms = flush_policy
            .background_interval
            .map(|d| d.as_millis() as u64);
        info!(
            target: "fold_node::database",
            sync_on_finalize = flush_policy.sync_on_finalize,
            background_flush_ms = ?background_ms,
            sync_flush_env = super::super::mutation_flush::MUTATION_SYNC_FLUSH_ENV,
            background_env = super::super::mutation_flush::BACKGROUND_FLUSH_MS_ENV,
            "mutation flush policy (memory-first by default; flush later)"
        );
        let background_flush = match flush_policy.background_interval {
            Some(interval) => super::super::mutation_flush::BackgroundFlushTask::spawn(
                Arc::clone(&db_ops),
                interval,
            ),
            None => super::super::mutation_flush::BackgroundFlushTask::disabled(),
        };

        // Resident-primary policy controls memory and admission. Schema persist
        // lanes own mutation atoms and tips, so no second writer drains the
        // same dirty graph in production.
        let resident_policy = crate::resident::ResidentPolicy::from_env();
        info!(
            target: "fold_node::database",
            mode = resident_policy.mode.as_str(),
            budget_bytes = resident_policy.budget_bytes,
            persist_ms = ?resident_policy.persist_interval.map(|d| d.as_millis() as u64),
            "resident policy (T0 graph budget + persist cadence)"
        );
        // The resident graph budget above is one of several this process
        // chooses; nothing used to compute their sum. Log the whole accounted
        // number against the RSS guard here, at boot, so a configuration that
        // cannot fit is visible BEFORE the guard kills the node rather than
        // after (2026-07-29 restart loop, brain
        // `lastdb-resident-write-hard-dirty-bytes-cap`).
        crate::memory_budget::process_memory_budget().log_at_boot();
        let background_persist = crate::resident::BackgroundPersistTask::disabled();

        // Opt-in bounded reaper for the unbounded `protein:` class — see
        // `super::super::protein_reaper` for why a manual verb alone leaves
        // future leaks unreaped.
        let protein_reaper_policy = super::super::protein_reaper::ProteinReaperPolicy::from_env();
        info!(
            target: "fold_node::database",
            reaper_ms = ?protein_reaper_policy.interval.map(|d| d.as_millis() as u64),
            "protein reaper policy (opt-in bounded background gc-proteins cadence)"
        );
        let background_protein_reaper = match protein_reaper_policy.interval {
            Some(interval) => super::super::protein_reaper::BackgroundProteinReaperTask::spawn(
                Arc::clone(&db_ops),
                interval,
            ),
            None => super::super::protein_reaper::BackgroundProteinReaperTask::disabled(),
        };

        // Opt-in bounded reclaim for legacy tip-version chains left after tip
        // history became write-opt-in. See `super::super::tip_history_drain`.
        let tip_history_drain_policy =
            super::super::tip_history_drain::TipHistoryDrainPolicy::from_env();
        info!(
            target: "fold_node::database",
            drain_ms = ?tip_history_drain_policy.interval.map(|d| d.as_millis() as u64),
            max_keys = tip_history_drain_policy.max_keys,
            "tip-history drain policy (opt-in bounded background tv-chain reclaim)"
        );
        let background_tip_history_drain = match tip_history_drain_policy.interval {
            Some(interval) => {
                super::super::tip_history_drain::BackgroundTipHistoryDrainTask::spawn(
                    Arc::clone(&db_ops),
                    interval,
                    tip_history_drain_policy.max_keys,
                )
            }
            None => super::super::tip_history_drain::BackgroundTipHistoryDrainTask::disabled(),
        };

        // Opt-in bounded janitor that frees `atom:` bodies live Delete
        // converge leaves unreferenced. See `super::super::atom_reclaim_janitor`
        // — local-only, no exclusive barrier, does not gate ACK. Distinct from
        // the cloud-sync `sync_coordinator.start_automatic_gc_atoms` cadence
        // below, which is coupled to the backup publish lock and byte-budget
        // gated (cloud keep-set shrink, out of scope here).
        let atom_reclaim_janitor_policy =
            super::super::atom_reclaim_janitor::AtomReclaimJanitorPolicy::from_env();
        info!(
            target: "fold_node::database",
            reclaim_ms = ?atom_reclaim_janitor_policy.interval.map(|d| d.as_millis() as u64),
            "atom reclaim janitor policy (opt-in bounded background gc-atoms cadence after tip converge)"
        );
        let background_atom_reclaim_janitor = match atom_reclaim_janitor_policy.interval {
            Some(interval) => {
                super::super::atom_reclaim_janitor::BackgroundAtomReclaimJanitorTask::spawn(
                    Arc::clone(&db_ops),
                    Arc::clone(&mutation_manager),
                    interval,
                )
            }
            None => {
                super::super::atom_reclaim_janitor::BackgroundAtomReclaimJanitorTask::disabled()
            }
        };

        #[cfg(feature = "cloud-sync")]
        let sync_coordinator = {
            let coordinator = match packing_slots {
                Some((backup, pause)) => SyncCoordinator::new_with_packing(backup, pause),
                None => SyncCoordinator::new(),
            };
            coordinator.start_automatic_gc_atoms(Arc::clone(&db_ops));
            // Plane self-compaction is not a cloud feature. Start it here,
            // beside gc-atoms, so a node booted without `cloud_sync.json`
            // reclaims `tips` / `atoms` / the capture-free planes on the same
            // expected trigger a cloud-synced node does. When an engine is set
            // later the cadence steps that engine's compactor instead, on the
            // same packing lock.
            coordinator.start_automatic_plane_compaction(&db_ops);
            coordinator
        };

        Ok(Self {
            schema_manager,
            db_ops,
            query_executor,
            mutation_manager,
            pending_tasks,
            background_flush,
            background_persist,
            background_protein_reaper,
            background_tip_history_drain,
            background_atom_reclaim_janitor,
            #[cfg(feature = "cloud-sync")]
            sync_coordinator,
            #[cfg(feature = "cloud-sync")]
            mutation_log_capture,
            config_store: std::sync::RwLock::new(None),
            #[cfg(feature = "cloud-sync")]
            signer,
        })
    }
}

/// Bundled inputs for [`FoldDB::initialize_from_init`].
///
/// Created at the production / test factory boundary and consumed
/// exactly once. Keeps the initializer signature short while the
/// surrounding subsystems (mutation manager, sync engine) each pull what
/// they need by name.
pub struct FoldDbInit {
    pub db_ops: Arc<DbOperations>,
    /// Currently unused inside the initializer body; retained so callers
    /// can name the FOLDDB_HOME path without juggling separate threads
    /// of state.
    pub db_path: String,
    pub signer: Arc<crate::security::Ed25519KeyPair>,
    pub search_outbox_inbox: Option<PathBuf>,
    #[cfg(feature = "cloud-sync")]
    pub(crate) mutation_log_capture: Option<Arc<crate::sync::capture::MutationLogCaptureRouter>>,
    /// Packing lock from a `SyncEngine` created before FoldDB (factory
    /// cloud-on-at-boot). `None` means the coordinator creates the process
    /// slot and a later `start_sync_engine_runtime` must take it.
    #[cfg(feature = "cloud-sync")]
    pub(crate) packing_slots: Option<(
        crate::sync::capture::plane_compactor::BackupPublishTargetSlot,
        Arc<tokio::sync::Mutex<Option<u64>>>,
    )>,
}

impl FoldDbInit {
    /// Configure an in-process local database without cloud mutation capture.
    /// Factory boots attach their capture router separately inside the crate.
    pub fn local(
        db_ops: Arc<DbOperations>,
        db_path: String,
        signer: Arc<crate::security::Ed25519KeyPair>,
    ) -> Self {
        Self {
            db_ops,
            db_path,
            signer,
            search_outbox_inbox: None,
            #[cfg(feature = "cloud-sync")]
            mutation_log_capture: None,
            #[cfg(feature = "cloud-sync")]
            packing_slots: None,
        }
    }
}
