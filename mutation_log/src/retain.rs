use std::collections::BTreeSet;

use crate::{
    DatabaseKind, LogError, LogState, NamespaceKind, NamespacePolicy, PrincipalId, Snapshot,
    SnapshotRef, StoredEntry, SubmissionRecord, SubmissionStatus, Writer, MAX_RETENTION_MS,
};

impl LogState {
    /// Acknowledge delivery sequence `through` for one reading device.
    ///
    /// The key is the writer (node key, device). Two devices of one key keep
    /// two positions in this log. `through` past the log head is
    /// [`LogError::AckAhead`]. An ack at or below that device's stored
    /// position is a no-op. The first ack of a device is stored even when
    /// `through` is zero. Hidden entries are part of the head, so a reader
    /// who scanned them can ack past entries they cannot see.
    pub fn ack(&self, reader: &Writer, through: u64) -> Result<Self, LogError> {
        if !self.reader_allowed(&reader.key) {
            return Err(LogError::Unauthorized);
        }
        let head = self.head.as_ref().map_or(0, |head| head.delivery_seq);
        if through > head {
            return Err(LogError::AckAhead);
        }
        if self
            .acks
            .get(reader)
            .is_some_and(|stored| through <= *stored)
        {
            return Ok(self.clone());
        }
        let mut next = self.clone();
        next.acks.insert(reader.clone(), through);
        Ok(next)
    }

    /// Record a zero ack for a device the first time it pulls.
    ///
    /// An existing ack is left alone. A device that has appended already
    /// counts as zero in the floor even before this row exists.
    pub fn register_reader(&self, reader: &Writer) -> Result<Self, LogError> {
        if !self.reader_allowed(&reader.key) {
            return Err(LogError::Unauthorized);
        }
        if self.acks.contains_key(reader) {
            return Ok(self.clone());
        }
        let mut next = self.clone();
        next.acks.insert(reader.clone(), 0);
        Ok(next)
    }

    /// Record a snapshot cut at the current delivery head.
    ///
    /// `snapshot_ref` is the shell's pointer to the snapshot bytes.
    /// `caller` is the publisher key, or an owner key of a personal database.
    pub fn cut_snapshot(
        &self,
        caller: &PrincipalId,
        at_ms: u64,
        snapshot_ref: SnapshotRef,
    ) -> Result<(Self, Snapshot), LogError> {
        self.authorize_maintainer(caller)?;
        let snapshot = Snapshot {
            through_delivery_seq: self.head.as_ref().map_or(0, |head| head.delivery_seq),
            at_ms,
            snapshot_ref,
        };
        let mut next = self.clone();
        next.snapshot = Some(snapshot.clone());
        Ok((next, snapshot))
    }

    /// Drop the prefix at or below the trim floor.
    ///
    /// The floor is the minimum device ack, and also the expired prefix,
    /// and it never passes the latest snapshot. A floor that would drop
    /// entries is refused unless a snapshot ref covers it. A young pending
    /// submission stays, including when the floor moves past it. A rejected
    /// one, an incorporated one, and one older than 30 days are dropped.
    /// Dropping a pending submission records the writer, sequence, status,
    /// and this `now_ms`. Those records stay for the life of the log.
    /// `caller` is the publisher key or an owner key.
    pub fn trim(&self, caller: &PrincipalId, now_ms: u64) -> Result<Self, LogError> {
        self.authorize_maintainer(caller)?;
        let floor = self.covered_floor(now_ms)?;
        let mut next = self.clone();
        let dropped = drop_covered(&mut next, floor, now_ms);
        if !dropped && floor == self.trimmed_through {
            return Ok(self.clone());
        }
        next.trimmed_through = floor;
        Ok(next)
    }

    /// Mark one pending submission so a later trim can drop it.
    ///
    /// `caller` is the publisher key or an owner key. The submission must
    /// still be stored and not already copied.
    pub fn reject_submission(
        &self,
        caller: &PrincipalId,
        writer: &Writer,
        writer_seq: u64,
    ) -> Result<Self, LogError> {
        self.authorize_maintainer(caller)?;
        let stored = self
            .entries
            .iter()
            .find(|row| row.entry.writer == *writer && row.entry.writer_seq == writer_seq)
            .ok_or(LogError::UnknownSubmission)?;
        if !pending_submission(self, stored) {
            return Err(LogError::UnknownSubmission);
        }
        let mut next = self.clone();
        next.rejected.insert((writer.clone(), writer_seq));
        Ok(next)
    }

    fn covered_floor(&self, now_ms: u64) -> Result<u64, LogError> {
        let desired = self.desired_floor(now_ms);
        if desired <= self.trimmed_through {
            return Ok(self.trimmed_through);
        }
        let snapshot = self.snapshot.as_ref().ok_or(LogError::SnapshotRequired)?;
        Ok(desired
            .min(snapshot.through_delivery_seq)
            .max(self.trimmed_through))
    }

    /// Highest delivery sequence that may be dropped.
    #[must_use]
    pub fn effective_floor(&self, now_ms: u64) -> u64 {
        match &self.snapshot {
            Some(snapshot) => self
                .desired_floor(now_ms)
                .min(snapshot.through_delivery_seq)
                .max(self.trimmed_through),
            None => self.trimmed_through,
        }
    }

    /// The shell should cut a snapshot before more of the log can be dropped.
    #[must_use]
    pub fn snapshot_due(&self, now_ms: u64) -> bool {
        self.desired_floor(now_ms) > self.effective_floor(now_ms)
    }

    /// Entries the latest snapshot covers.
    ///
    /// A shareable snapshot contains official entries whose writer key is the
    /// publisher key. A personal snapshot also contains app-private namespaces.
    #[must_use]
    pub fn snapshot_basis<'a>(&'a self, policy: &impl NamespacePolicy) -> Vec<&'a StoredEntry> {
        let through = self
            .snapshot
            .as_ref()
            .map_or(0, |cut| cut.through_delivery_seq);
        self.entries
            .iter()
            .filter(|stored| stored.delivery_seq <= through && basis_entry(self, stored, policy))
            .collect()
    }

    fn desired_floor(&self, now_ms: u64) -> u64 {
        self.min_ack().max(self.retention_prefix(now_ms))
    }

    fn min_ack(&self) -> u64 {
        let consumers = self.consumers();
        if consumers.is_empty() {
            return 0;
        }
        consumers
            .iter()
            .map(|principal| self.principal_ack(principal))
            .min()
            .unwrap_or(0)
    }

    /// Lowest device position for `principal`.
    ///
    /// A device that has appended, and a device with an ack row, both count.
    /// A tip with no ack row counts as zero. A key that has never appended
    /// and has no ack row also counts as zero.
    fn principal_ack(&self, principal: &PrincipalId) -> u64 {
        let mut floor = None;
        for (writer, ack) in &self.acks {
            if &writer.key == principal {
                floor = Some(floor.map_or(*ack, |kept: u64| kept.min(*ack)));
            }
        }
        for writer in self.tips.keys() {
            if &writer.key == principal && !self.acks.contains_key(writer) {
                floor = Some(0);
            }
        }
        floor.unwrap_or(0)
    }

    fn consumers(&self) -> BTreeSet<PrincipalId> {
        let mut ids = BTreeSet::new();
        if let DatabaseKind::Shareable { publisher } = &self.kind {
            ids.insert(publisher.clone());
        }
        for grant in &self.grants {
            ids.insert(grant.principal.clone());
        }
        ids
    }

    fn retention_prefix(&self, now_ms: u64) -> u64 {
        let mut floor = self.trimmed_through;
        for entry in &self.entries {
            if now_ms.saturating_sub(entry.accepted_at_ms) < MAX_RETENTION_MS {
                break;
            }
            floor = entry.delivery_seq;
        }
        floor
    }
}

fn drop_covered(state: &mut LogState, floor: u64, now_ms: u64) -> bool {
    let mut dropped = false;
    let mut kept = Vec::new();
    for stored in std::mem::take(&mut state.entries) {
        if stored.delivery_seq > floor || live_pending(state, &stored, now_ms) {
            kept.push(stored);
            continue;
        }
        dropped = true;
        note_dropped(state, &stored, now_ms);
    }
    state.entries = kept;
    dropped
}

fn live_pending(state: &LogState, stored: &StoredEntry, now_ms: u64) -> bool {
    pending_submission(state, stored)
        && !state
            .rejected
            .contains(&(stored.entry.writer.clone(), stored.entry.writer_seq))
        && now_ms.saturating_sub(stored.accepted_at_ms) < MAX_RETENTION_MS
}

fn pending_submission(state: &LogState, stored: &StoredEntry) -> bool {
    let DatabaseKind::Shareable { publisher } = &state.kind else {
        return false;
    };
    stored.entry.incorporated_from.is_none()
        && stored.entry.writer.key != *publisher
        && !state
            .incorporated
            .contains(&(stored.entry.writer.clone(), stored.entry.writer_seq))
}

fn note_dropped(state: &mut LogState, stored: &StoredEntry, now_ms: u64) {
    let key = (stored.entry.writer.clone(), stored.entry.writer_seq);
    let rejected = state.rejected.remove(&key);
    let expired = pending_submission(state, stored)
        && !rejected
        && now_ms.saturating_sub(stored.accepted_at_ms) >= MAX_RETENTION_MS;
    let previous = state.trimmed_tips.get(&key.0).copied().unwrap_or(0);
    state
        .trimmed_tips
        .insert(key.0.clone(), previous.max(key.1));
    state.incorporated.remove(&key);
    if rejected || expired {
        state.submission_records.insert(
            (key.0.clone(), key.1),
            SubmissionRecord {
                writer: key.0,
                writer_seq: key.1,
                status: if rejected {
                    SubmissionStatus::Rejected
                } else {
                    SubmissionStatus::Expired
                },
                time_ms: now_ms,
            },
        );
    }
}

fn basis_entry(state: &LogState, stored: &StoredEntry, policy: &impl NamespacePolicy) -> bool {
    if stored.entry.database_id != state.database_id {
        return false;
    }
    match &state.kind {
        DatabaseKind::Personal => true,
        DatabaseKind::Shareable { publisher } => {
            &stored.entry.writer.key == publisher
                && policy.classify(&stored.entry.namespace) == NamespaceKind::Official
        }
    }
}
