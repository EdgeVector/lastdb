use std::sync::Arc;

use crate::db_operations::DbOperations;
use crate::schema::SchemaCore;
use crate::storage::StorageError;

use super::FoldDB;
use crate::fold_db_core::mutation_manager::MutationManager;
use crate::fold_db_core::query::QueryExecutor;

impl FoldDB {
    /// Retrieves or generates and persists the node identifier.
    pub async fn get_node_id(&self) -> Result<String, StorageError> {
        self.db_ops
            .metadata()
            .get_node_id()
            .await
            .map_err(|e| StorageError::BackendError(e.to_string()))
    }

    /// Returns a reference to the database operations Arc
    pub fn db_ops(&self) -> &Arc<DbOperations> {
        &self.db_ops
    }

    /// Returns a reference to the query executor
    pub fn query_executor(&self) -> &QueryExecutor {
        &self.query_executor
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
}
