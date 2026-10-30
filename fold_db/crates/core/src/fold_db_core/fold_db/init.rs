use std::path::PathBuf;
use std::sync::Arc;

use futures::FutureExt;
use tracing::info;

use crate::db_operations::DbOperations;
use crate::schema::SchemaCore;
use crate::storage::StorageError;

use super::FoldDB;
use crate::fold_db_core::mutation_manager::MutationManager;
use crate::fold_db_core::query::QueryExecutor;

impl FoldDB {
    /// Production / factory initializer.
    ///
    /// `signer` is the Ed25519 keypair used to sign molecule mutations.
    /// It is shared with the sync engine so merged-molecule writes during
    /// replay carry the same node identity as direct writes via
    /// `MutationManager`. Production callers (via the factory) must load
    /// this from the node's persistent identity so signatures match the
    /// node's public key.
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

        let background_atom_history = super::super::atom_history::BackgroundAtomHistory::start(
            Arc::clone(&db_ops),
            offline_maintenance,
        )
        .await;
        if !offline_maintenance {
            info!(target: "fold_node::database", "native writes complete on disk before acknowledgment");
        }

        Self::log_memory_budget();

        let background_protein_reaper = Self::start_protein_reaper(&db_ops, offline_maintenance);

        let background_conflict_janitor =
            Self::start_conflict_janitor(&db_ops, offline_maintenance);

        let background_molecule_drain = Self::start_molecule_drain(&db_ops, offline_maintenance);

        Ok(Self {
            app_schema_conversion_active: Arc::new(std::sync::atomic::AtomicBool::new(
                super::app_schema_conversion::active(&db_ops).await?,
            )),
            schema_manager,
            db_ops: Arc::clone(&db_ops),
            query_executor,
            mutation_manager,
            pending_tasks,
            background_protein_reaper,
            background_conflict_janitor,
            background_atom_history,
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
