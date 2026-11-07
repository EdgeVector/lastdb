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
    /// Downloads a record file that is not local; `None` when no cloud backup
    /// is configured. The node registers it after boot.
    pub(crate) record_blob_source:
        std::sync::RwLock<Option<Arc<dyn super::record_blob_source::RecordBlobSource>>>,
}
