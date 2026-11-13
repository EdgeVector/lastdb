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
mod owner_recovery;
mod paths;
mod received;
pub(crate) mod received_access;
mod registry;
mod restore;
mod retry;
mod run;
mod session;
mod status;
mod turn;
mod upload;
mod wire;

pub use owner_recovery::{recover_owned, RecoveredPrefix};
pub use turn::spawn;
