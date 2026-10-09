//! FoldDB Core — embedded database coordinator.
//!
//! Layout:
//! - [`factory`] — production boot (local store stack, decrypt proof, sync attach)
//! - [`fold_db`] — `FoldDB` struct + accessors / lifecycle / admin surfaces
//! - [`mutation_manager`] — write path
//! - [`query`] — query executor + hash-range paths
//! - [`purge`] — schema/data purge
//! - [`orchestration`] — coordination helpers
//! - [`pending_task_tracker`] — background task bookkeeping
//! - [`sync_coordinator`] — cloud sync lifecycle (`cloud-sync` feature)

pub mod atom_reclaim_janitor;
pub mod factory;
pub mod fold_db;
pub mod mutation_flush;
pub mod orchestration;
pub mod pending_task_tracker;
pub mod protein_reaper;
pub mod query;
pub mod tip_history_drain;

pub mod mutation_manager;
pub mod purge;
#[cfg(feature = "cloud-sync")]
pub mod sync_coordinator;

pub use query::QueryExecutor;
pub use query::{HashRangeWatch, HashRangeWatchBounds, HashRangeWatchError, HashRangeWatchEvent};

pub use atom_reclaim_janitor::{
    AtomReclaimJanitorPolicy, BackgroundAtomReclaimJanitorTask, ATOM_RECLAIM_MS_ENV,
    DEFAULT_ATOM_RECLAIM_MS,
};
pub use mutation_flush::{
    mutation_sync_flush_enabled, BackgroundFlushTask, MutationFlushPolicy, BACKGROUND_FLUSH_MS_ENV,
    DEFAULT_BACKGROUND_FLUSH_MS, MUTATION_SYNC_FLUSH_ENV,
};
pub use mutation_manager::{MutationManager, PurgeStats};
pub use protein_reaper::{
    BackgroundProteinReaperTask, ProteinReaperPolicy, DEFAULT_PROTEIN_REAPER_MS,
    PROTEIN_REAPER_MS_ENV,
};
#[cfg(feature = "cloud-sync")]
pub use sync_coordinator::SyncCoordinator;
pub use tip_history_drain::{
    BackgroundTipHistoryDrainTask, TipHistoryDrainPolicy, DEFAULT_TIP_HISTORY_DRAIN_MAX_KEYS,
    DEFAULT_TIP_HISTORY_DRAIN_MS, TIP_HISTORY_DRAIN_MAX_KEYS_ENV, TIP_HISTORY_DRAIN_MS_ENV,
};

pub use fold_db::{
    CatalogDeleteReclaimResult, CatalogReclaimResult, DbSchemaShareResult, FoldDB,
    HashRangeKeyFieldRepairReport,
};
