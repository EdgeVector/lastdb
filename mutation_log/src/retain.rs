use std::collections::BTreeSet;

use crate::{
    DatabaseKind, LogError, LogState, NamespaceKind, NamespacePolicy, PrincipalId, Snapshot,
    StoredEntry, MAX_RETENTION_MS,
};

impl LogState {
    /// Acknowledge `through` in the cursor space this consumer can see.
    ///
    /// The bound is the last entry that consumer can read, not the raw head.
    /// The stored value is that cursor and the space it counts in. An ack at
    /// or below the stored cursor in the same space is a no-op. A grant or
    /// revoke that changes the space resets the ack to that space's start.
    pub fn ack(
        &self,
        consumer: &PrincipalId,
        through: u64,
        policy: &impl NamespacePolicy,
    ) -> Result<Self, LogError> {
        if !self.reader_allowed(consumer) {
            return Err(LogError::Unauthorized);
        }
        let visible = crate::cursor::max_visible_cursor(self, consumer, policy);
        if through > visible {
            return Err(LogError::AckAhead);
        }
        let space = crate::cursor_space(self, consumer);
        let previous = stored_through(self, consumer);
        if through <= previous {
            return Ok(self.clone());
        }
        let mut next = self.clone();
        next.acks
            .insert(consumer.clone(), crate::AckCursor { space, through });
        Ok(next)
    }

    /// Record a snapshot cut at the current delivery head.
    ///
    /// `caller` is the publisher, or a device grantee of a personal database.
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
    /// The floor is the minimum ack coverage, and also the expired prefix,
    /// and it never passes the latest snapshot. Without a snapshot nothing
    /// is dropped, so a new consumer can still re-bootstrap. `caller` is the
    /// publisher or a personal device grantee.
    pub fn trim(
        &self,
        caller: &PrincipalId,
        now_ms: u64,
        policy: &impl NamespacePolicy,
    ) -> Result<Self, LogError> {
        self.authorize_maintainer(caller)?;
        let floor = self.effective_floor(now_ms, policy);
        if floor <= self.trimmed_through {
            return Ok(self.clone());
        }
        let mut next = self.clone();
        prune_incorporated(&mut next, floor);
        crate::cursor::remember_trimmed(&mut next, floor, policy);
        next.entries.retain(|entry| entry.delivery_seq > floor);
        next.trimmed_through = floor;
        Ok(next)
    }

    /// Highest delivery sequence that may be dropped.
    #[must_use]
    pub fn effective_floor(&self, now_ms: u64, policy: &impl NamespacePolicy) -> u64 {
        match &self.snapshot {
            Some(snapshot) => self
                .desired_floor(now_ms, policy)
                .min(snapshot.through_delivery_seq),
            None => self.trimmed_through,
        }
    }

    /// The shell should cut a snapshot before more of the log can be dropped.
    #[must_use]
    pub fn snapshot_due(&self, now_ms: u64, policy: &impl NamespacePolicy) -> bool {
        self.desired_floor(now_ms, policy) > self.effective_floor(now_ms, policy)
    }

    /// Entries the latest snapshot covers.
    ///
    /// A shareable snapshot contains the publisher stream's official entries
    /// only. A personal snapshot also contains app-private namespaces.
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

    fn desired_floor(&self, now_ms: u64, policy: &impl NamespacePolicy) -> u64 {
        self.min_ack(policy).max(self.retention_prefix(now_ms))
    }

    fn min_ack(&self, policy: &impl NamespacePolicy) -> u64 {
        let consumers = self.consumers();
        if consumers.is_empty() {
            return 0;
        }
        consumers
            .iter()
            .map(|principal| {
                let cursor = stored_through(self, principal);
                crate::cursor::coverage(self, principal, cursor, policy)
            })
            .min()
            .unwrap_or(0)
    }

    fn consumers(&self) -> BTreeSet<PrincipalId> {
        let mut ids = BTreeSet::new();
        if let DatabaseKind::Shareable { publisher, .. } = &self.kind {
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

fn stored_through(state: &LogState, principal: &PrincipalId) -> u64 {
    let space = crate::cursor_space(state, principal);
    match state.acks.get(principal) {
        Some(ack) if ack.space == space => ack.through,
        _ => 0,
    }
}

fn prune_incorporated(state: &mut LogState, floor: u64) {
    let dropped: Vec<(crate::StreamId, u64)> = state
        .entries
        .iter()
        .filter(|stored| stored.delivery_seq <= floor && stored.entry.incorporated_from.is_none())
        .map(|stored| (stored.entry.stream.clone(), stored.entry.writer_seq))
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
        DatabaseKind::Shareable {
            publisher_stream, ..
        } => {
            stored.entry.stream == *publisher_stream
                && policy.classify(&stored.entry.namespace) == NamespaceKind::Official
        }
    }
}
