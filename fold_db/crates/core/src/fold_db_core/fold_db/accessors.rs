use std::sync::Arc;

use crate::db_operations::DbOperations;
use crate::schema::SchemaCore;
use crate::storage::StorageError;

use super::FoldDB;
use crate::fold_db_core::mutation_manager::MutationManager;
use crate::fold_db_core::query::QueryExecutor;
use crate::fold_db_core::query::{HashRangeWatch, HashRangeWatchBounds};

impl FoldDB {
    /// Retrieves or generates and persists the node identifier.
    pub async fn get_node_id(&self) -> Result<String, StorageError> {
        self.db_ops
            .metadata()
            .get_node_id()
            .await
            .map_err(|e| StorageError::BackendError(e.to_string()))
    }

    /// Returns a clone of the config store, if available.
    pub fn config_store(&self) -> Option<crate::storage::NodeConfigStore> {
        self.config_store.read().unwrap().clone()
    }

    /// Set the config store (called by the factory).
    pub fn set_config_store(&self, store: crate::storage::NodeConfigStore) {
        *self.config_store.write().unwrap() = Some(store);
    }

    /// Returns a reference to the database operations Arc
    pub fn db_ops(&self) -> &Arc<DbOperations> {
        &self.db_ops
    }

    /// Returns a reference to the query executor
    pub fn query_executor(&self) -> &QueryExecutor {
        &self.query_executor
    }

    /// Start a live watch for one HashRange partition.
    pub fn watch_hash_range(
        &self,
        schema: impl Into<String>,
        hash: impl Into<String>,
        bounds: Option<HashRangeWatchBounds>,
    ) -> HashRangeWatch {
        self.query_executor.watch_hash_range(schema, hash, bounds)
    }

    /// Start a watch without range bounds.
    pub fn watch_partition(
        &self,
        schema: impl Into<String>,
        hash: impl Into<String>,
    ) -> HashRangeWatch {
        self.query_executor.watch_partition(schema, hash)
    }

    /// Start a watch with inclusive-start/exclusive-end range bounds.
    pub fn watch_range(
        &self,
        schema: impl Into<String>,
        hash: impl Into<String>,
        start: Option<String>,
        end: Option<String>,
    ) -> HashRangeWatch {
        self.query_executor.watch_range(schema, hash, start, end)
    }

    /// Returns a reference to the pending task tracker
    pub fn pending_tasks(&self) -> &Arc<super::super::pending_task_tracker::PendingTaskTracker> {
        &self.pending_tasks
    }

    /// Get the schema manager for testing schema functionality
    pub fn schema_manager(&self) -> Arc<SchemaCore> {
        Arc::clone(&self.schema_manager)
    }

    /// Get the mutation manager for testing mutation functionality
    pub fn mutation_manager(&self) -> &MutationManager {
        &self.mutation_manager
    }

    /// Resident graph (T0 rehydrate / hit metrics). Shared via DbOperations.
    pub fn resident(&self) -> &std::sync::Arc<crate::resident::ResidentGraph> {
        self.db_ops.resident()
    }
}
