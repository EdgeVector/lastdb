use std::collections::{BTreeMap, BTreeSet};

use crate::{
    Capability, DatabaseId, DatabaseKind, Grant, Head, LogError, PrincipalId, Snapshot, StreamId,
    StreamRole,
};

/// Progress of one writer stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamProgress {
    /// Last accepted writer sequence. Zero means the stream has no entries.
    pub last_writer_seq: u64,
    /// Highest writer sequence already dropped on this stream.
    pub trimmed_writer_seq: u64,
    /// What kind of stream this is.
    pub role: StreamRole,
}

/// The whole log as a value. The shell stores it. This crate does not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogState {
    /// Database this log belongs to.
    pub database_id: DatabaseId,
    /// Personal or shareable.
    pub kind: DatabaseKind,
    /// Delivery head. Absent while the log has no entries.
    pub head: Option<Head>,
    /// Writer streams known to this database.
    pub streams: BTreeMap<StreamId, StreamProgress>,
    /// Who may read or append. This list is not a storage path.
    pub grants: Vec<Grant>,
    /// Highest delivery sequence each consumer has acknowledged.
    pub acks: BTreeMap<PrincipalId, u64>,
    /// Retained entries in delivery order.
    pub entries: Vec<crate::StoredEntry>,
    /// Open appends, keyed by stream. The value is the principal who opened it.
    pub inflight: BTreeMap<StreamId, PrincipalId>,
    /// Latest snapshot cut, if the shell has taken one.
    pub snapshot: Option<Snapshot>,
    /// Highest delivery sequence already dropped.
    pub trimmed_through: u64,
    /// Submissions already copied onto the publisher stream.
    ///
    /// The key is `(submission_stream, writer_seq)`. Trim does not remove it,
    /// so a repeated incorporation cannot create a second delivery.
    pub incorporated: BTreeSet<(StreamId, u64)>,
}

impl LogState {
    /// Open an empty log. A shareable log starts with its publisher stream.
    #[must_use]
    pub fn open(database_id: DatabaseId, kind: DatabaseKind) -> Self {
        let mut streams = BTreeMap::new();
        if let DatabaseKind::Shareable {
            publisher_stream, ..
        } = &kind
        {
            streams.insert(publisher_stream.clone(), publisher_progress());
        }
        Self {
            database_id,
            kind,
            head: None,
            streams,
            grants: Vec::new(),
            acks: BTreeMap::new(),
            entries: Vec::new(),
            inflight: BTreeMap::new(),
            snapshot: None,
            trimmed_through: 0,
            incorporated: BTreeSet::new(),
        }
    }

    /// Open one append on `stream`. A second open on the same stream is refused.
    pub fn begin_append(&self, author: &PrincipalId, stream: &StreamId) -> Result<Self, LogError> {
        self.authorize_append(author, stream)?;
        if self.inflight.contains_key(stream) {
            return Err(LogError::AppendInFlight);
        }
        let mut next = self.clone();
        next.inflight.insert(stream.clone(), author.clone());
        Ok(next)
    }

    /// Close an open append. Only the principal who opened it may close it.
    pub fn finish_append(&self, caller: &PrincipalId, stream: &StreamId) -> Result<Self, LogError> {
        match self.inflight.get(stream) {
            Some(owner) if owner == caller => {
                let mut next = self.clone();
                next.inflight.remove(stream);
                Ok(next)
            }
            Some(_) => Err(LogError::Unauthorized),
            None => Err(LogError::AppendNotOpen),
        }
    }

    /// Last accepted writer sequence on `stream`, or zero when it has no entries.
    ///
    /// `reader` must be allowed to append that stream.
    pub fn writer_tip(&self, reader: &PrincipalId, stream: &StreamId) -> Result<u64, LogError> {
        self.authorize_append(reader, stream)?;
        Ok(self
            .streams
            .get(stream)
            .map_or(0, |progress| progress.last_writer_seq))
    }

    pub(crate) fn authorize_append(
        &self,
        author: &PrincipalId,
        stream: &StreamId,
    ) -> Result<(), LogError> {
        match &self.kind {
            DatabaseKind::Personal => self.authorize_device(author, stream),
            DatabaseKind::Shareable {
                publisher,
                publisher_stream,
            } => {
                if stream == publisher_stream {
                    return author_is(author, publisher);
                }
                self.authorize_submission(author, stream)
            }
        }
    }

    fn authorize_device(&self, author: &PrincipalId, stream: &StreamId) -> Result<(), LogError> {
        let granted = self.grants.iter().any(|grant| {
            grant.principal == *author
                && matches!(&grant.capability, Capability::Device { stream: granted } if granted == stream)
        });
        if !granted {
            return Err(LogError::Unauthorized);
        }
        match self.streams.get(stream).map(|progress| &progress.role) {
            Some(StreamRole::Device { owner }) if owner == author => Ok(()),
            _ => Err(LogError::Unauthorized),
        }
    }

    fn authorize_submission(
        &self,
        author: &PrincipalId,
        stream: &StreamId,
    ) -> Result<(), LogError> {
        let granted = self.grants.iter().any(|grant| {
            grant.principal == *author
                && matches!(&grant.capability, Capability::Submit { stream: granted } if granted == stream)
        });
        if !granted {
            return Err(LogError::Unauthorized);
        }
        match self.streams.get(stream).map(|progress| &progress.role) {
            Some(StreamRole::Submission { author: owner }) if owner == author => Ok(()),
            _ => Err(LogError::Unauthorized),
        }
    }
}

fn author_is(author: &PrincipalId, expected: &PrincipalId) -> Result<(), LogError> {
    if author == expected {
        Ok(())
    } else {
        Err(LogError::Unauthorized)
    }
}

fn publisher_progress() -> StreamProgress {
    StreamProgress {
        last_writer_seq: 0,
        trimmed_writer_seq: 0,
        role: StreamRole::Publisher,
    }
}
