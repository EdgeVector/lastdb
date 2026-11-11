//! Shared-sync step for the unified mutation log.
//!
//! One participant proposes an effect. [`drive`] applies that effect through
//! an injected [`MutationLog`] and returns the next participant state.
//! Personal devices and share participants use this same step. There is no
//! format parameter and no contributor cap.
//!
//! [`select_path`] is the flag. A disabled [`MutationLogConfig`] returns
//! [`SharedSyncPath::Existing`], which is the `lastdb_node` shared-sync
//! worker. That worker is not called from here, and this crate does not read
//! the process environment, open a socket, or call Exemem.
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
pub use mutation_log_mem::{MutationLog, MutationLogConfig};

/// Which shared-sync implementation a caller runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SharedSyncPath<L> {
    /// The current `lastdb_node` shared-sync worker. No log is constructed.
    Existing,
    /// One mutation log, stepped by [`drive`].
    Log(L),
}

/// Install the log path only when `config` is enabled.
///
/// The builder runs only for [`SharedSyncPath::Log`]. A disabled config does
/// not call it, so the existing worker stays the path and no log is built.
#[must_use]
pub fn select_path<L>(
    config: MutationLogConfig,
    build_log: impl FnOnce() -> L,
) -> SharedSyncPath<L> {
    if config.is_enabled() {
        SharedSyncPath::Log(build_log())
    } else {
        SharedSyncPath::Existing
    }
}

#[cfg(test)]
mod tests;
