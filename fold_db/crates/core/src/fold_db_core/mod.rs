//! FoldDB Core — embedded database coordinator.
//!
//! Layout:
//! - [`factory`] — production boot (local store stack, decrypt proof, sync attach)
//! - [`fold_db`] — `FoldDB` struct + accessors / lifecycle / admin surfaces
//! - [`mutation_manager`] — write path
//! - [`query`] — query executor + hash-range paths
//! - [`purge`] — schema/data purge
//! - [`pending_task_tracker`] — background task bookkeeping

pub mod factory;
pub mod fold_db;
pub mod pending_task_tracker;

pub mod query;

pub mod mutation_manager;
pub mod purge;

pub use query::QueryExecutor;

pub use mutation_manager::MutationManager;

pub use fold_db::FoldDB;
