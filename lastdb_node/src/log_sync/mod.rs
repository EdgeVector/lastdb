//! Daemon adapter for the encrypted cloud mutation log.
//!
//! Resolve a database by its registered id. Authorization determines the
//! permitted writes. Snapshot recovery installs into that database and the
//! sync loop acknowledges only after the delivered prefix is durable.

mod accepted;
mod account;
mod apply;
mod cloud;
mod cutover;
mod disk;
mod files;
mod http;
mod members;
mod native;
mod owner_recovery;
mod paths;
mod received;
mod registry;
mod restore;
mod retry;
mod run;
mod session;
mod status;
mod turn;
mod upload;
mod wire;

pub(crate) use cutover::legacy_resume_forbidden;
pub use owner_recovery::recover_owned;
pub use turn::spawn;
