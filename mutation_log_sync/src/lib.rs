//! Shared-sync step for the unified mutation log.
//!
//! [`drive`] pulls one page through an injected [`MutationLog`] and advances
//! the participant cursor. Personal writers and share participants use this
//! same step. There is no flag and no second path. This crate does not read
//! the process environment, open a socket, or call Exemem. It depends on the
//! [`MutationLog`] port, not on an adapter.

#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

use std::fmt;

pub use mutation_log::MutationLog;
use mutation_log::{
    DatabaseId, DatabaseKind, DeviceId, LogError, PrincipalId, StoredEntry, Writer, LIST_PAGE_LIMIT,
};

/// A refused sync step. The caller's previous participant state is unchanged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SyncError {
    /// The log refused the read or recovery.
    Log(LogError),
}

impl From<LogError> for SyncError {
    fn from(error: LogError) -> Self {
        Self::Log(error)
    }
}

impl fmt::Display for SyncError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self::Log(error) = self;
        write!(formatter, "{error}")
    }
}

impl std::error::Error for SyncError {}

/// Entries this participant may apply locally.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Outcome {
    /// Visible entries, oldest first.
    pub entries: Vec<StoredEntry>,
    /// Delivery sequence for the next pull.
    pub cursor: u64,
}

/// How this participant uses the database.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SyncRole {
    /// One device of a personal database's owner key.
    Owner {
        /// Owner node key.
        principal: PrincipalId,
        /// Device of that key.
        device: DeviceId,
    },
    /// One device of a shareable database's publisher key.
    Publisher {
        /// Publisher node key.
        principal: PrincipalId,
        /// Device of that key.
        device: DeviceId,
    },
    /// Reader of entries whose writer key is the publisher key.
    ///
    /// The device is this reader's ack key. A receiver cannot append.
    Receiver {
        /// Receiver node key.
        principal: PrincipalId,
        /// Device of that key.
        device: DeviceId,
    },
}

/// One participant's delivery cursor and role for one database.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncState {
    database_id: DatabaseId,
    kind: DatabaseKind,
    role: SyncRole,
    cursor: u64,
}

impl SyncState {
    /// Start a participant at delivery cursor 0.
    ///
    /// A personal database accepts an owner only. A shareable database accepts
    /// its publisher or a receiver, and rejects an owner grant.
    pub fn open(
        database_id: DatabaseId,
        kind: DatabaseKind,
        role: SyncRole,
    ) -> Result<Self, SyncError> {
        check_role(&kind, &role)?;
        Ok(Self {
            database_id,
            kind,
            role,
            cursor: 0,
        })
    }

    /// Database identity bound at open.
    #[must_use]
    pub fn database_id(&self) -> &DatabaseId {
        &self.database_id
    }

    /// Database kind bound at open.
    #[must_use]
    pub fn kind(&self) -> &DatabaseKind {
        &self.kind
    }

    /// Delivery cursor the next pull starts after. The shell persists this.
    #[must_use]
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Replace the delivery cursor the shell loaded from its durable session.
    #[must_use]
    pub fn with_cursor(mut self, cursor: u64) -> Self {
        self.cursor = cursor;
        self
    }

    fn read_writer(&self) -> Writer {
        let (principal, device) = match &self.role {
            SyncRole::Owner { principal, device }
            | SyncRole::Publisher { principal, device }
            | SyncRole::Receiver { principal, device } => (principal, device),
        };
        Writer {
            key: principal.clone(),
            device: device.clone(),
        }
    }
}

/// Pull the next visible page through `log` and advance `state`.
///
/// On `Err`, `state` is unchanged. A cursor expiry runs recovery without
/// moving the failed cursor beforehand.
pub fn drive<L: MutationLog>(
    state: &mut SyncState,
    log: &L,
    now_ms: u64,
) -> Result<Outcome, SyncError> {
    let outcome = pull(log, &state.read_writer(), state.cursor, now_ms)?;
    state.cursor = outcome.cursor;
    Ok(outcome)
}

fn pull<L: MutationLog>(
    log: &L,
    reader: &Writer,
    after: u64,
    now_ms: u64,
) -> Result<Outcome, SyncError> {
    match log.list(&reader.key, after, LIST_PAGE_LIMIT, now_ms) {
        Ok(page) => Ok(Outcome {
            entries: page.entries,
            cursor: page.next,
        }),
        Err(LogError::CursorExpired { .. }) => {
            let recovered = log.recover(reader, after, now_ms)?;
            let _ = recovered.reset;
            Ok(Outcome {
                entries: recovered.entries,
                cursor: recovered.next,
            })
        }
        Err(error) => Err(error.into()),
    }
}

fn check_role(kind: &DatabaseKind, role: &SyncRole) -> Result<(), SyncError> {
    match kind {
        DatabaseKind::Personal => {
            if matches!(role, SyncRole::Owner { .. }) {
                Ok(())
            } else {
                Err(LogError::PersonalNotShareable.into())
            }
        }
        DatabaseKind::Shareable { publisher } => match role {
            SyncRole::Owner { .. } => Err(LogError::DeviceGrantNotShareable.into()),
            SyncRole::Publisher { principal, .. } if principal != publisher => {
                Err(LogError::Unauthorized.into())
            }
            SyncRole::Publisher { .. } | SyncRole::Receiver { .. } => Ok(()),
        },
    }
}
