//! Daemon adapter for the encrypted cloud mutation log.
//!
//! Resolve a database by its registered id. Authorization determines the
//! permitted writes. Snapshot recovery installs into that database and the
//! sync loop acknowledges only after the delivered prefix is durable.

mod accepted;
mod account;
mod apply;
mod cloud;
mod disk;
mod files;
mod http;
mod members;
mod native;
mod native_reset;
mod owner_recovery;
mod paths;
mod received;
pub(crate) mod received_access;
mod registry;
mod restore;
mod retention;
mod retry;
mod run;
mod session;
mod snapshot;
mod status;
mod turn;
mod upload;
mod wire;

pub use native_reset::install_prepared_native_recovery;
pub use owner_recovery::{recover_owned, RecoveredPrefix};
pub use retention::{reclaim as reclaim_snapshot_log, ReclaimReport};
pub(crate) use snapshot::should_stage_initial_snapshot;
pub use snapshot::{
    request as request_snapshot, restore_base as restore_snapshot_base, RestoredBase,
};
pub use turn::spawn;
