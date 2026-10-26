use std::path::PathBuf;
use std::sync::Arc;

use futures::FutureExt;
use tracing::info;

use crate::db_operations::DbOperations;
use crate::schema::SchemaCore;
use crate::storage::disk_pack::PackNamespacedStore;
use crate::storage::StorageError;

use super::FoldDB;
use crate::fold_db_core::mutation_manager::MutationManager;
use crate::fold_db_core::query::QueryExecutor;

impl FoldDB {
    /// Creates a new FoldDB instance in a new home at the specified path.
    ///
    /// This is a native convenience path for in-process callers
    /// that do not have a persistent node identity (e.g., ephemeral
    /// `tempdir` storage). It generates a fresh Ed25519 signing keypair
    /// and a fresh node encryption key on the fly, so the path must be
    /// absent or empty and no later open can read the home.
    /// **Production callers must go through
    /// [`crate::fold_db_core::factory::create_fold_db`] and pass in a
    /// signer loaded from the node's persistent identity** — otherwise
    /// every boot produces a different signing key and molecule
    /// signatures will not match the node's public identity.
    pub async fn new(path: &str) -> Result<Self, StorageError> {
        let store = Arc::new(PackNamespacedStore::create_ephemeral(
            std::path::Path::new(path),
        )?);
        Self::initialize_from_store(
            store as Arc<dyn crate::storage::traits::NamespacedStore>,
            None,
        )
        .await
    }

    /// Create an embedded database with durable per-molecule key bundles.
    ///
    /// The caller must supply the same node key and wrap key each time it
    /// opens this path. A new home is format v2, whose run home keys derive
    /// from the node key.
    pub async fn new_with_molecule_wrap_key(
        path: &str,
        node_key: &[u8; 32],
        molecule_wrap_key: [u8; 32],
    ) -> Result<Self, StorageError> {
        let store = Arc::new(PackNamespacedStore::open_keyed(
            std::path::Path::new(path),
            Some(node_key),
        )?);
        Self::initialize_from_store(
            store as Arc<dyn crate::storage::traits::NamespacedStore>,
            Some(molecule_wrap_key),
        )
        .await
    }

    /// Generate a signing keypair for in-process callers that do
    /// not have a persistent node identity. Wraps the error so callers
    /// get a single failure mode.
    fn generate_ephemeral_signer() -> Result<Arc<crate::security::Ed25519KeyPair>, StorageError> {
        let keypair = crate::security::Ed25519KeyPair::generate().map_err(|e| {
            StorageError::BackendError(format!("ephemeral signer generation failed: {e}"))
        })?;
        Ok(Arc::new(keypair))
    }

    /// In-process init: open native storage with an ephemeral signer.
    /// Production boots go through the factory → [`FoldDB::initialize_from_init`].
    async fn initialize_from_store(
        store: Arc<dyn crate::storage::traits::NamespacedStore>,
        molecule_wrap_key: Option<[u8; 32]>,
    ) -> Result<Self, StorageError> {
        let signer = Self::generate_ephemeral_signer()?;
        tracing::info!(
            target: "fold_node::database",
            "Using native packed storage with the core logical model"
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
            "Native packed storage is active"
        );

        Self::initialize_from_init(FoldDbInit {
            db_ops,
            signer,
            search_outbox_inbox: None,
            mutation_log_capture,
            offline_maintenance: false,
        })
        .await
    }

    /// Wrap an existing local restore store with normal key and native validation.
    /// This local initializer cannot attach an engine or contact a cloud service.
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
        // Restore uses the same fixed CAS-only wrapper as normal startup.
        let crypto: Arc<dyn crate::crypto::CryptoProvider> = Arc::new(
            crate::crypto::LocalCryptoProvider::from_key(e2e_keys.encryption_key()),
        );
        let store: Arc<dyn crate::storage::traits::NamespacedStore> = Arc::new(
            crate::storage::EncryptingNamespacedStore::new(store, crypto),
        );
        // Keyed atom reads require content encrypted with this account's E2E
        // key. Pass the same content key as the normal native constructor.
        crate::db_operations::atom_store::counter::restore_validation::validate_pending(
            store.as_ref(),
        )
        .await
        .map_err(|error| StorageError::InvalidOperation(error.to_string()))?;
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
        let db = Self::initialize_from_init(FoldDbInit {
            db_ops,
            signer,
            search_outbox_inbox: Some(restore_inbox),
            mutation_log_capture: None,
            offline_maintenance: true,
        })
        .await?;
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
            signer,
            search_outbox_inbox,
            mutation_log_capture,
            offline_maintenance,
        } = init;
        if !offline_maintenance {
            db_ops.atoms().freeze_replay_cutoff().await?;
        }
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
            signer,
            Arc::clone(&pending_tasks),
            search_outbox_inbox,
            author_clock_state,
        ));
        if let Some(router) = mutation_log_capture.as_ref() {
            mutation_manager.set_capture_router(Arc::clone(router));
        }

        info!("Created MutationManager for mutation operations");

        let background_flush = Self::start_background_flush(&db_ops, offline_maintenance);

        Self::log_resident_policy();

        let background_protein_reaper = Self::start_protein_reaper(&db_ops, offline_maintenance);

        let background_conflict_janitor =
            Self::start_conflict_janitor(&db_ops, offline_maintenance);

        let background_molecule_drain = Self::start_molecule_drain(&db_ops, offline_maintenance);

        Ok(Self {
            schema_manager,
            db_ops,
            query_executor,
            mutation_manager,
            pending_tasks,
            background_flush,
            background_protein_reaper,
            background_conflict_janitor,
            background_molecule_drain,
            mutation_log_capture,
        })
    }
}

/// Bundled inputs for [`FoldDB::initialize_from_init`].
///
/// Created at the factory or embedded constructor boundary and consumed
/// exactly once. Keeps the initializer signature short while the
/// surrounding subsystems (mutation manager, sync engine) each pull what
/// they need by name.
pub struct FoldDbInit {
    pub(crate) offline_maintenance: bool,
    pub db_ops: Arc<DbOperations>,
    pub signer: Arc<crate::security::Ed25519KeyPair>,
    pub search_outbox_inbox: Option<PathBuf>,
    pub(crate) mutation_log_capture: Option<Arc<crate::sync::capture::MutationLogCaptureRouter>>,
}
