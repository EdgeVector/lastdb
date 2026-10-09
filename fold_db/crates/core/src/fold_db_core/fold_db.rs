//! FoldDB Core - Main database coordinator
//!
//! This module contains the main FoldDB struct that manages schemas,
//! permissions, data storage, and runtime service wiring.

use std::sync::Arc;

use super::mutation_manager::MutationManager;
use super::query::QueryExecutor;
#[cfg(feature = "cloud-sync")]
use super::sync_coordinator::SyncCoordinator;
use crate::db_operations::DbOperations;
use crate::schema::SchemaCore;

mod accessors;
mod admin_db;
mod catalog_gc;
mod catalog_share;
#[cfg(feature = "cloud-sync")]
mod file_blobs;
mod hashrange_key_repair;
mod init;
mod lifecycle;
#[cfg(feature = "sharing")]
mod local_file_blobs;
mod provenance;
mod schema_api;
#[cfg(feature = "cloud-sync")]
mod sync;

pub use catalog_gc::{CatalogDeleteReclaimResult, CatalogReclaimResult};
pub use catalog_share::DbSchemaShareResult;
#[cfg(feature = "cloud-sync")]
pub use file_blobs::{
    PersonalFileBlobFork, PersonalFileBlobForkResult, PersonalFileBlobWrite,
    PersonalFileBlobWriteResult,
};
pub use hashrange_key_repair::HashRangeKeyFieldRepairReport;
pub use init::FoldDbInit;
#[cfg(feature = "sharing")]
pub use local_file_blobs::{
    local_file_blob_max_bytes, LocalFileBlobPut, LOCAL_FILE_BLOB_MAX_BYTES_DEFAULT,
    LOCAL_FILE_BLOB_MAX_BYTES_ENV,
};

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
    /// Periodic resident-graph dirty drain (T1). See [`crate::resident`].
    pub(crate) background_persist: crate::resident::BackgroundPersistTask,
    /// Periodic orphan `protein:` reaper. See [`super::protein_reaper`].
    pub(crate) background_protein_reaper: super::protein_reaper::BackgroundProteinReaperTask,
    /// Periodic bounded tip-history chain drain. See [`super::tip_history_drain`].
    pub(crate) background_tip_history_drain:
        super::tip_history_drain::BackgroundTipHistoryDrainTask,
    /// Periodic atom-reclaim janitor after live Delete converge. See
    /// [`super::atom_reclaim_janitor`].
    pub(crate) background_atom_reclaim_janitor:
        super::atom_reclaim_janitor::BackgroundAtomReclaimJanitorTask,
    /// Coordinates the optional cloud sync engine lifecycle.
    /// In local mode this holds no engine and all sync operations are no-ops.
    #[cfg(feature = "cloud-sync")]
    sync_coordinator: SyncCoordinator,
    /// Post-commit mutation-log capture engine slot. Present on factory-built
    /// stores even while Cloud Sync is off so live enable can start capture.
    #[cfg(feature = "cloud-sync")]
    mutation_log_capture: Option<Arc<crate::sync::capture::MutationLogCaptureRouter>>,
    /// Optional configuration store for runtime node config.
    /// Uses RwLock for interior mutability so FoldDB doesn't need &mut self.
    config_store: std::sync::RwLock<Option<crate::storage::NodeConfigStore>>,
    /// Signing keypair for molecule signatures. Cloned at construction into
    /// `MutationManager` (for atom signatures). Held on `FoldDB` so
    /// `start_sync_engine_runtime` can pass it to `SyncEngine::new` when sync
    /// activates after boot.
    #[cfg(feature = "cloud-sync")]
    pub(crate) signer: Arc<crate::security::Ed25519KeyPair>,
}

impl Drop for FoldDB {
    fn drop(&mut self) {
        // Request cancellation before the storage fields drop. Graceful
        // shutdown awaits this task; Drop cannot await from a synchronous
        // destructor and uses the best-effort fallback.
        self.background_flush.abort();
        self.background_persist.stop();
        self.mutation_manager.persist_lanes().stop();
        self.background_protein_reaper.stop();
        self.background_tip_history_drain.stop();
        self.background_atom_reclaim_janitor.stop();
        // Abort the background sync task to prevent tokio panic:
        // "Cannot drop a runtime in a context where blocking is not allowed"
        #[cfg(feature = "cloud-sync")]
        self.sync_coordinator.abort_task();
    }
}
