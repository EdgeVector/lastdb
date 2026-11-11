//! Shared-sync step for the unified mutation log.
//!
//! One participant proposes an effect. [`drive`] applies that effect through
//! an injected [`MutationLog`] and returns the next participant state.
//! Personal writers and share participants use this same step. [`drive`] is
//! the only step. There is no flag and no second path. This crate does not
//! read the process environment, open a socket, or call Exemem.
//! It depends on the [`MutationLog`] port, not on an adapter.
//!
//! Removing a participant is `revoke` on the log. This step does not rotate
//! keys, re-key a database, or move an epoch.

#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

mod drive;
mod error;
mod model;
mod step;

pub use drive::drive;
pub use error::SyncError;
pub use model::{Outcome, SyncEvent, SyncRole, SyncState};
pub use mutation_log::MutationLog;

#[cfg(test)]
mod tests;
