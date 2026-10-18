use std::path::PathBuf;
use std::sync::Arc;

use futures::FutureExt;
use tracing::info;

use crate::db_operations::DbOperations;
use crate::schema::SchemaCore;
use crate::storage::{LastStoreNamespacedStore, StorageError};

use super::FoldDB;
use crate::fold_db_core::mutation_manager::MutationManager;
use crate::fold_db_core::query::QueryExecutor;

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
            mutation_log_capture,
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
        let restore_home = std::path::Path::new(db_path)
            .parent()
            .filter(|home| !home.as_os_str().is_empty())
            .ok_or_else(|| {
                StorageError::BackendError("restore data path has no destination home".into())
            })?;
        let restore_inbox =
            crate::db_operations::search_index::search_inbox_dir_for_home(restore_home);
        let mut db = Self::initialize_from_init(FoldDbInit {
            db_ops,
            db_path: db_path.to_string(),
            signer,
            search_outbox_inbox: Some(restore_inbox),
            mutation_log_capture: None,
            packing_slots: None,
        })
        .await?;
        db.preserve_retired_restore_intents = true;
        Ok(db)
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
            mutation_log_capture,
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

        if crate::fold_db_core::query::hash_range_query::preload_receiver_controls(&db_ops)
            .boxed()
            .await
            .is_err()
        {
            tracing::warn!(
                "native receiver controls remain unknown after startup preload; queries retry their ordinary read"
            );
        }

        let author_clock_state = Self::load_author_clock_state(&db_ops, &signer).await?;

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
        if let Some(router) = mutation_log_capture.as_ref() {
            mutation_manager.set_capture_router(Arc::clone(router));
        }

        info!("Created MutationManager for mutation operations");

        let background_flush = Self::start_background_flush(&db_ops);

        let background_persist = Self::log_resident_policy();

        let background_protein_reaper = Self::start_protein_reaper(&db_ops);

        let background_tip_history_drain = Self::start_tip_history_drain(&db_ops);

        let background_atom_reclaim_janitor =
            Self::start_atom_reclaim_janitor(&db_ops, &mutation_manager);

        let sync_coordinator = Self::start_sync_coordinator(&db_ops, packing_slots);

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
            sync_coordinator,
            mutation_log_capture,
            preserve_retired_restore_intents: false,
            config_store: std::sync::RwLock::new(None),
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
    pub(crate) mutation_log_capture: Option<Arc<crate::sync::capture::MutationLogCaptureRouter>>,
    /// Packing lock from a `SyncEngine` created before FoldDB (factory
    /// cloud-on-at-boot). `None` means the coordinator creates the process
    /// slot and a later `start_sync_engine_runtime` must take it.
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
            mutation_log_capture: None,
            packing_slots: None,
        }
    }
}
