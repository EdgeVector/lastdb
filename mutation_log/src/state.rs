use std::collections::{BTreeMap, BTreeSet};

use crate::{DatabaseId, DatabaseKind, Grant, Head, LogError, PrincipalId, Snapshot, Writer};

/// The whole log as a value. The shell stores it. This crate does not.
///
/// One log per database. Writer tips and device read positions are keyed by
/// writer (node key, device). There is no separate log or storage per device.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogState {
    /// Database this log belongs to.
    pub database_id: DatabaseId,
    /// Personal or shareable.
    pub kind: DatabaseKind,
    /// Delivery head. Absent while the log has no entries.
    pub head: Option<Head>,
    /// Last accepted writer sequence for each writer. Missing means zero.
    pub tips: BTreeMap<Writer, u64>,
    /// Highest writer sequence already dropped for each writer.
    pub trimmed_tips: BTreeMap<Writer, u64>,
    /// Who may read or append. This list is not a storage path.
    pub grants: Vec<Grant>,
    /// Highest delivery sequence each reading device has acknowledged.
    pub acks: BTreeMap<Writer, u64>,
    /// Retained entries in delivery order.
    pub entries: Vec<crate::StoredEntry>,
    /// Open appends, one per writer (node key, device).
    pub inflight: BTreeSet<Writer>,
    /// Latest snapshot cut, if the shell has taken one.
    pub snapshot: Option<Snapshot>,
    /// Highest delivery sequence already dropped.
    pub trimmed_through: u64,
    /// Submissions already copied by the publisher key.
    ///
    /// The key is `(submission writer, writer_seq)`, once in total. A persistent
    /// adapter stores this set with the log. Trim drops a key when the
    /// submission it names is dropped.
    pub incorporated: BTreeSet<(Writer, u64)>,
}

impl LogState {
    /// Open an empty log for one database.
    #[must_use]
    pub fn open(database_id: DatabaseId, kind: DatabaseKind) -> Self {
        Self {
            database_id,
            kind,
            head: None,
            tips: BTreeMap::new(),
            trimmed_tips: BTreeMap::new(),
            grants: Vec::new(),
            acks: BTreeMap::new(),
            entries: Vec::new(),
            inflight: BTreeSet::new(),
            snapshot: None,
            trimmed_through: 0,
            incorporated: BTreeSet::new(),
        }
    }

    /// Open one append for `writer`. A second open for the same writer is refused.
    pub fn begin_append(&self, writer: &Writer) -> Result<Self, LogError> {
        self.authorize_append(writer)?;
        if self.inflight.contains(writer) {
            return Err(LogError::AppendInFlight);
        }
        let mut next = self.clone();
        next.inflight.insert(writer.clone());
        Ok(next)
    }

    /// Close an open append for `writer`.
    pub fn finish_append(&self, writer: &Writer) -> Result<Self, LogError> {
        if !self.inflight.contains(writer) {
            return Err(LogError::AppendNotOpen);
        }
        self.authorize_append(writer)?;
        let mut next = self.clone();
        next.inflight.remove(writer);
        Ok(next)
    }

    /// Last accepted writer sequence for `writer`, or zero when it has no entries.
    ///
    /// `reader` must be that writer's node key, and that key must be allowed to append.
    pub fn writer_tip(&self, reader: &PrincipalId, writer: &Writer) -> Result<u64, LogError> {
        if reader != &writer.key {
            return Err(LogError::Unauthorized);
        }
        self.authorize_append(writer)?;
        Ok(self.tips.get(writer).copied().unwrap_or(0))
    }

    pub(crate) fn authorize_append(&self, writer: &Writer) -> Result<(), LogError> {
        match &self.kind {
            DatabaseKind::Personal => authorize_owner(self, &writer.key),
            DatabaseKind::Shareable { publisher } if &writer.key == publisher => Ok(()),
            DatabaseKind::Shareable { .. } => authorize_submit(self, &writer.key),
        }
    }
}

fn authorize_owner(state: &LogState, key: &PrincipalId) -> Result<(), LogError> {
    if crate::auth::holds_owner(state, key) {
        Ok(())
    } else {
        Err(LogError::Unauthorized)
    }
}

fn authorize_submit(state: &LogState, key: &PrincipalId) -> Result<(), LogError> {
    if crate::auth::holds_submit(state, key) {
        Ok(())
    } else {
        Err(LogError::Unauthorized)
    }
}
