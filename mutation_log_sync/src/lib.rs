//! Shared-sync step for the unified mutation log.
//!
//! One participant proposes an effect. [`drive`] applies that effect through
//! an injected [`MutationLog`] and returns the next participant state.
//! Personal writers and share participants use this same step. There is no
//! format parameter and no contributor cap.
//!
//! [`select_path`] is the flag. `enabled == false` returns
//! [`SharedSyncPath::Existing`], which is the `lastdb_node` shared-sync
//! worker. That worker is not called from here. The shell reads
//! `MutationLogConfig` and passes `is_enabled`. This crate does not read the
//! process environment, open a socket, or call Exemem.
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

/// Which shared-sync implementation a caller runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SharedSyncPath<L> {
    /// The current `lastdb_node` shared-sync worker. No log is constructed.
    Existing,
    /// One mutation log, stepped by [`drive`].
    Log(L),
}

/// Install the log path only when `enabled` is true.
///
/// The builder runs only for [`SharedSyncPath::Log`]. A disabled flag does
/// not call it, so the existing worker stays the path and no log is built.
#[must_use]
pub fn select_path<L>(enabled: bool, build_log: impl FnOnce() -> L) -> SharedSyncPath<L> {
    if enabled {
        SharedSyncPath::Log(build_log())
    } else {
        SharedSyncPath::Existing
    }
}

#[cfg(test)]
mod tests;
