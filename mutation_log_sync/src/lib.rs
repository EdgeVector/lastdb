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
mod model;
mod step;

use std::fmt;

pub use drive::drive;
pub use model::{Outcome, SyncEvent, SyncRole, SyncState};
use mutation_log::LogError;
pub use mutation_log::MutationLog;

/// A refused sync step. The caller's previous participant state is unchanged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SyncError {
    /// The log refused the effect.
    Log(LogError),
    /// A receiver has nothing to append.
    ReadOnly,
    /// Only the publisher re-appends a submission.
    NotPublisher,
    /// Re-append only copies a submission, not an entry the publisher key already authored.
    NotSubmission,
    /// Append ran before [`crate::SyncState::resume`] loaded the writer tip.
    NeedsResume,
}

impl From<LogError> for SyncError {
    fn from(error: LogError) -> Self {
        Self::Log(error)
    }
}

impl fmt::Display for SyncError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Log(error) => write!(formatter, "{error}"),
            Self::ReadOnly => write!(formatter, "a receiver cannot append"),
            Self::NotPublisher => write!(formatter, "only the publisher can re-append"),
            Self::NotSubmission => {
                write!(
                    formatter,
                    "re-append copies a submission, not an entry authored by the publisher key"
                )
            }
            Self::NeedsResume => write!(formatter, "resume the writer sequence before appending"),
        }
    }
}

impl std::error::Error for SyncError {}
