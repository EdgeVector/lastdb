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
mod catalog_gc;
mod catalog_membership;
mod catalog_share;
mod init;
mod init_boot;
mod lifecycle;
mod local_file_blobs;
mod maintenance_admission;

mod record_conflicts;
mod schema_drop;

pub use catalog_gc::CatalogDeleteReclaimResult;
pub use catalog_share::DbSchemaShareResult;

pub use init::FoldDbInit;
pub use local_file_blobs::LocalFileBlobPut;
pub use maintenance_admission::LocalMutationAdmission;

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
    /// Periodic release of kept conflict records. See [`super::conflict_janitor`].
    pub(crate) background_conflict_janitor: super::conflict_janitor::BackgroundConflictJanitorTask,
    pub(crate) background_field_history: super::field_history::BackgroundFieldHistory,
    /// Mutation admission gate. Maintenance and local file-blob writes take
    /// an admission; conflict metadata fences new mutations with it.
    mutation_log_capture: Option<Arc<crate::fold_db_core::admission::MutationLogCaptureRouter>>,
}

impl Drop for FoldDB {
    fn drop(&mut self) {
        // Request cancellation before the storage fields drop. Graceful
        // shutdown awaits this task; Drop cannot await from a synchronous
        // destructor and uses the best-effort fallback.
        self.background_conflict_janitor.stop();
        self.background_field_history.abort();
    }
}
