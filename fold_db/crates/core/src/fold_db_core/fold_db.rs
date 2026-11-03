//! FoldDB Core - Main database coordinator
//!
//! This module contains the main FoldDB struct that manages schemas,
//! permissions, data storage, and runtime service wiring.

use std::sync::Arc;

use super::mutation_manager::MutationManager;
use super::query::QueryExecutor;
use crate::db_operations::DbOperations;
use crate::schema::SchemaCore;

mod accessors;
mod admin_db;
mod init;
mod init_boot;
mod lifecycle;
mod record_files;
mod schema_drop;

pub use init::FoldDbInit;
pub use record_files::RecordFileState;

/// The main database coordinator that manages schemas, permissions, and data storage.
pub struct FoldDB {
    pub(crate) schema_manager: Arc<SchemaCore>,
    /// Shared database operations with storage abstraction
    pub(crate) db_ops: Arc<DbOperations>,
    /// Query executor for handling all query operations
    pub(crate) query_executor: QueryExecutor,
    /// Mutation manager for handling all mutation operations.
    pub(crate) mutation_manager: Arc<MutationManager>,
    /// Tracker for pending background tasks
    pub(crate) pending_tasks: Arc<super::pending_task_tracker::PendingTaskTracker>,
    pub(crate) background_field_history: super::field_history::BackgroundFieldHistory,
}

impl Drop for FoldDB {
    fn drop(&mut self) {
        // Request cancellation before the storage fields drop. Graceful
        // shutdown awaits this task; Drop cannot await from a synchronous
        // destructor and uses the best-effort fallback.
        self.background_field_history.abort();
    }
}
