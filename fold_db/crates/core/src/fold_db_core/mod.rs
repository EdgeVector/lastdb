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

mod atom_history;
mod background_task;
pub mod conflict_janitor;
mod conflict_janitor_pace;
pub mod conflict_janitor_policy;
pub mod factory;
pub mod fold_db;
pub mod orchestration;
pub mod pending_task_tracker;

pub mod query;

pub mod mutation_manager;
pub mod purge;

pub use query::QueryExecutor;
pub use query::{HashRangeWatch, HashRangeWatchBounds, HashRangeWatchError, HashRangeWatchEvent};

pub use conflict_janitor::BackgroundConflictJanitorTask;
pub use conflict_janitor_policy::{
    ConflictJanitorPolicy, CONFLICT_JANITOR_DUTY_PCT_ENV, CONFLICT_JANITOR_MS_ENV,
    CONFLICT_JANITOR_NET_SECS_ENV, DEFAULT_CONFLICT_JANITOR_DUTY_PCT, DEFAULT_CONFLICT_JANITOR_MS,
};
pub use mutation_manager::{MutationManager, PurgeStats};

pub use fold_db::{CatalogDeleteReclaimResult, DbSchemaShareResult, FoldDB};
