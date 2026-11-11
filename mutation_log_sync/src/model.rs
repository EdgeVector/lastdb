use crate::error::SyncError;
use mutation_log::{
    DatabaseId, DatabaseKind, DeviceId, LogError, MutationLog, Namespace, PrincipalId, StoredEntry,
    Writer,
};

/// What one participant does next.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SyncEvent {
    /// Append one local mutation for this participant's writer.
    Append {
        /// Namespace of the schema, without the schema name.
        namespace: Namespace,
        /// Opaque record bytes. Synthetic in tests.
        payload: Vec<u8>,
        /// When the participant composed the mutation.
        authored_at_ms: u64,
        /// When the shell accepted it.
        accepted_at_ms: u64,
    },
    /// Read the next page this participant is allowed to see.
    Pull {
        /// Shell clock, used for retention.
        now_ms: u64,
    },
    /// Publisher key copies one submission into the log.
    Reappend {
        /// The submission entry the publisher read.
        entry: mutation_log::Entry,
        /// When the shell accepted the copy.
        accepted_at_ms: u64,
    },
}

/// What [`crate::drive`] did to the log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The batch was stored.
    Appended {
        /// Delivery sequences assigned to the new entries.
        delivery_seqs: Vec<u64>,
        /// Next writer sequence for this participant.
        next_seq: u64,
    },
    /// The batch was already stored. `next_seq` is the next sequence the writer accepts.
    Duplicate {
        /// Next writer sequence for this participant.
        next_seq: u64,
    },
    /// Entries this participant may apply locally.
    Pulled {
        /// Visible entries, oldest first.
        entries: Vec<StoredEntry>,
        /// Delivery sequence for the next pull.
        cursor: u64,
    },
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
    Receiver {
        /// Receiver node key.
        principal: PrincipalId,
    },
    /// One device of a contributor key.
    Contributor {
        /// Contributor node key.
        principal: PrincipalId,
        /// Device of that key.
        device: DeviceId,
    },
}

/// One participant's delivery cursor and writer sequence for one database.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncState {
    pub(crate) database_id: DatabaseId,
    pub(crate) kind: DatabaseKind,
    pub(crate) role: SyncRole,
    pub(crate) cursor: u64,
    pub(crate) next_seq: u64,
    pub(crate) resumed: bool,
}

impl SyncState {
    /// Start a participant at delivery cursor 0. `next_seq` is 1 until [`Self::resume`].
    ///
    /// Append is refused until `resume` loads the writer tip. A personal
    /// database accepts an owner only. A shareable database accepts a
    /// publisher, a receiver, or a contributor, and rejects an owner grant.
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
            next_seq: 1,
            resumed: false,
        })
    }

    /// Set `next_seq` from the writer tip already stored in `log`.
    ///
    /// [`crate::drive`] refuses an append until this has run. A process that
    /// rejoins calls it before appending, so the next mutation does not reuse
    /// a stored sequence. A receiver has nothing to resume.
    pub fn resume<L: MutationLog>(state: &Self, log: &L) -> Result<Self, SyncError> {
        let Some(writer) = crate::step::role_writer(state) else {
            return Ok(state.clone());
        };
        let tip = log.writer_tip(state.reader(), &writer)?;
        let mut next = state.clone();
        next.next_seq = tip.checked_add(1).ok_or(LogError::SequenceExhausted)?;
        next.resumed = true;
        Ok(next)
    }

    pub(crate) fn reader(&self) -> &PrincipalId {
        match &self.role {
            SyncRole::Owner { principal, .. }
            | SyncRole::Publisher { principal, .. }
            | SyncRole::Receiver { principal }
            | SyncRole::Contributor { principal, .. } => principal,
        }
    }
}

fn check_role(kind: &DatabaseKind, role: &SyncRole) -> Result<(), SyncError> {
    match kind {
        DatabaseKind::Personal => personal_role(role),
        DatabaseKind::Shareable { publisher } => shareable_role(publisher, role),
    }
}

fn personal_role(role: &SyncRole) -> Result<(), SyncError> {
    match role {
        SyncRole::Owner { .. } => Ok(()),
        SyncRole::Publisher { .. } | SyncRole::Receiver { .. } | SyncRole::Contributor { .. } => {
            Err(LogError::PersonalNotShareable.into())
        }
    }
}

fn shareable_role(publisher: &PrincipalId, role: &SyncRole) -> Result<(), SyncError> {
    match role {
        SyncRole::Owner { .. } => Err(LogError::DeviceGrantNotShareable.into()),
        SyncRole::Publisher { principal, .. } if principal != publisher => {
            Err(LogError::Unauthorized.into())
        }
        SyncRole::Publisher { .. } | SyncRole::Receiver { .. } | SyncRole::Contributor { .. } => {
            Ok(())
        }
    }
}

pub(crate) fn writer_of(key: PrincipalId, device: DeviceId) -> Writer {
    Writer { key, device }
}
