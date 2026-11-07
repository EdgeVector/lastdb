use std::path::PathBuf;
use std::sync::Arc;

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
    /// `signer` is the Ed25519 keypair used to sign field key mutations.
    /// It is shared with the sync engine so merged-field-key writes during
    /// replay carry the same node identity as direct writes via
    /// `MutationManager`. Production callers (via the factory) must load
    /// this from the node's persistent identity so signatures match the
    /// node's public key.
    pub async fn initialize_from_init(init: FoldDbInit) -> Result<Self, StorageError> {
        let FoldDbInit {
            db_ops,
            signer,
            search_outbox_inbox,
            offline_maintenance,
        } = init;
        // Initialize pending task tracker
        let pending_tasks = Arc::new(super::super::pending_task_tracker::PendingTaskTracker::new());

        let schema_manager = Arc::new(
            SchemaCore::new(Arc::clone(&db_ops))
                .await
                .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))?,
        );
        if !offline_maintenance {
            db_ops.records().freeze_replay_cutoff().await?;
        }

        // Create QueryExecutor for handling all query operations
        let query_executor = QueryExecutor::new(&db_ops, Arc::clone(&schema_manager));
        info!("Created QueryExecutor for query operations");

        let author_clock_state = Self::load_author_clock_state(&db_ops, &signer).await?;

        // Create MutationManager for handling all mutation operations.
        // The signer was loaded and validated by the caller (in
        // production, from the node's persistent identity). It is shared
        // with the sync engine so merged-field-key writes trace to the
        // same node identity as direct writes via `MutationManager`.
        let mutation_manager = Arc::new(MutationManager::new(
            Arc::clone(&db_ops),
            Arc::clone(&schema_manager),
            signer,
            Arc::clone(&pending_tasks),
            search_outbox_inbox,
            author_clock_state,
        ));

        info!("Created MutationManager for mutation operations");

        if !offline_maintenance {
            info!(target: "fold_node::database", "native writes complete on disk before acknowledgment");
        }

        Self::log_memory_budget();

        Ok(Self {
            schema_manager,
            db_ops: Arc::clone(&db_ops),
            query_executor,
            mutation_manager,
            pending_tasks,
            record_blob_source: std::sync::RwLock::new(None),
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
}
