//! FoldDB Core - Main database coordinator
//!
//! This module contains the main FoldDB struct that manages schemas,
//! permissions, data storage, and runtime service wiring.

use std::sync::Arc;

use super::mutation_manager::MutationManager;
use super::query::QueryExecutor;
use super::sync_coordinator::SyncCoordinator;
use crate::db_operations::DbOperations;
use crate::schema::SchemaCore;

mod accessors;
mod admin_db;
mod catalog_gc;
mod catalog_membership;
mod catalog_share;
mod conflict_metadata;
mod file_blobs;
mod hashrange_key_repair;
mod init;
mod init_boot;
mod lifecycle;
mod local_file_blobs;
mod maintenance_admission;
mod provenance;
mod schema_api;
mod schema_drop;
mod sync;

pub use catalog_gc::CatalogDeleteReclaimResult;
pub use catalog_share::DbSchemaShareResult;
pub use file_blobs::{
    PersonalFileBlobFork, PersonalFileBlobForkResult, PersonalFileBlobWrite,
    PersonalFileBlobWriteResult,
};
pub use hashrange_key_repair::HashRangeKeyFieldRepairReport;
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
    /// Periodic storage flusher (memory-first mutation durability).
    /// See [`super::mutation_flush`].
    pub(crate) background_flush: super::mutation_flush::BackgroundFlushTask,
    /// Periodic orphan `protein:` reaper. See [`super::protein_reaper`].
    pub(crate) background_protein_reaper: super::protein_reaper::BackgroundProteinReaperTask,
    /// Periodic release of kept conflict records. See [`super::conflict_janitor`].
    pub(crate) background_conflict_janitor: super::conflict_janitor::BackgroundConflictJanitorTask,
    /// Paced removal of dead molecules. See [`super::molecule_drain`].
    pub(crate) background_molecule_drain: super::molecule_drain::BackgroundMoleculeDrainTask,
    /// Owns local maintenance cadences and their shared packing lock.
    /// The optional cloud engine is absent during the native cutover.
    sync_coordinator: SyncCoordinator,
    /// Post-commit mutation-log capture engine slot. Present on factory-built
    /// stores even while Cloud Sync is off so live enable can start capture.
    mutation_log_capture: Option<Arc<crate::sync::capture::MutationLogCaptureRouter>>,
    /// Exclusive one-shot restore policy; never enabled by a serving factory.
    preserve_retired_restore_intents: bool,
    /// Optional configuration store for runtime node config.
    /// Uses RwLock for interior mutability so FoldDB doesn't need &mut self.
    config_store: std::sync::RwLock<Option<crate::storage::NodeConfigStore>>,
    /// Signing keypair for molecule signatures. Cloned at construction into
    /// `MutationManager` (for atom signatures). Held on `FoldDB` so
    /// `start_sync_engine_runtime` can pass it to `SyncEngine::new` when sync
    /// activates after boot.
    pub(crate) signer: Arc<crate::security::Ed25519KeyPair>,
}

impl Drop for FoldDB {
    fn drop(&mut self) {
        // Request cancellation before the storage fields drop. Graceful
        // shutdown awaits this task; Drop cannot await from a synchronous
        // destructor and uses the best-effort fallback.
        self.background_flush.abort();
        self.mutation_manager.persist_lanes().stop();
        self.background_protein_reaper.stop();
        self.background_conflict_janitor.stop();
        self.background_molecule_drain.stop();
        // Abort the background sync task to prevent tokio panic:
        // "Cannot drop a runtime in a context where blocking is not allowed"
        self.sync_coordinator.abort_task();
    }
}
