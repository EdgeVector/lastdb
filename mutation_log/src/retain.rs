use std::collections::BTreeSet;

use crate::{
    DatabaseKind, LogError, LogState, NamespaceKind, NamespacePolicy, PrincipalId, Snapshot,
    StoredEntry, Writer, MAX_RETENTION_MS,
};

impl LogState {
    /// Acknowledge delivery sequence `through`.
    ///
    /// `through` past the log head is [`LogError::AckAhead`]. An ack at or
    /// below the stored delivery sequence is a no-op. Hidden entries are part
    /// of the head, so a reader who scanned them can ack past entries they
    /// cannot see.
    pub fn ack(&self, consumer: &PrincipalId, through: u64) -> Result<Self, LogError> {
        if !self.reader_allowed(consumer) {
            return Err(LogError::Unauthorized);
        }
        let head = self.head.as_ref().map_or(0, |head| head.delivery_seq);
        if through > head {
            return Err(LogError::AckAhead);
        }
        let previous = self.acks.get(consumer).copied().unwrap_or(0);
        if through <= previous {
            return Ok(self.clone());
        }
        let mut next = self.clone();
        next.acks.insert(consumer.clone(), through);
        Ok(next)
    }

    /// Record a snapshot cut at the current delivery head.
    ///
    /// `caller` is the publisher key, or an owner key of a personal database.
    pub fn cut_snapshot(
        &self,
        caller: &PrincipalId,
        at_ms: u64,
    ) -> Result<(Self, Snapshot), LogError> {
        self.authorize_maintainer(caller)?;
        let snapshot = Snapshot {
            through_delivery_seq: self.head.as_ref().map_or(0, |head| head.delivery_seq),
            at_ms,
        };
        let mut next = self.clone();
        next.snapshot = Some(snapshot.clone());
        Ok((next, snapshot))
    }

    /// Drop the prefix at or below the trim floor.
    ///
    /// The floor is the minimum delivery ack, and also the expired prefix,
    /// and it never passes the latest snapshot. Without a snapshot nothing
    /// is dropped, so a new consumer can still re-bootstrap. `caller` is the
    /// publisher key or an owner key.
    pub fn trim(&self, caller: &PrincipalId, now_ms: u64) -> Result<Self, LogError> {
        self.authorize_maintainer(caller)?;
        let floor = self.effective_floor(now_ms);
        if floor <= self.trimmed_through {
            return Ok(self.clone());
        }
        let mut next = self.clone();
        remember_trimmed(&mut next, floor);
        prune_incorporated(&mut next, floor);
        next.entries.retain(|entry| entry.delivery_seq > floor);
        next.trimmed_through = floor;
        Ok(next)
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
            .map(|principal| self.acks.get(principal).copied().unwrap_or(0))
            .min()
            .unwrap_or(0)
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

fn remember_trimmed(state: &mut LogState, floor: u64) {
    for stored in &state.entries {
        if stored.delivery_seq > floor {
            continue;
        }
        let writer = stored.entry.writer.clone();
        let seq = stored.entry.writer_seq;
        let previous = state.trimmed_tips.get(&writer).copied().unwrap_or(0);
        state.trimmed_tips.insert(writer, previous.max(seq));
    }
}

fn prune_incorporated(state: &mut LogState, floor: u64) {
    let dropped: Vec<(Writer, u64)> = state
        .entries
        .iter()
        .filter(|stored| stored.delivery_seq <= floor && stored.entry.incorporated_from.is_none())
        .map(|stored| (stored.entry.writer.clone(), stored.entry.writer_seq))
        .collect();
    for key in dropped {
        state.incorporated.remove(&key);
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
