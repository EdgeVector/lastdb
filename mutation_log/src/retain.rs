use std::collections::BTreeSet;

use crate::{
    DatabaseKind, LogError, LogState, NamespaceKind, NamespacePolicy, PrincipalId, Snapshot,
    StoredEntry, MAX_RETENTION_MS,
};

impl LogState {
    /// Acknowledge delivery through `through`. The cursor does not move backward.
    pub fn ack(&self, consumer: &PrincipalId, through: u64) -> Result<Self, LogError> {
        if !self.reader_allowed(consumer) {
            return Err(LogError::Unauthorized);
        }
        let head = self.head.as_ref().map_or(0, |head| head.delivery_seq);
        if through > head {
            return Err(LogError::AckAhead);
        }
        let previous = self.acks.get(consumer).copied().unwrap_or(0);
        if through < previous {
            return Err(LogError::AckRegress);
        }
        let mut next = self.clone();
        next.acks.insert(consumer.clone(), through);
        Ok(next)
    }

    /// Record a snapshot cut at the current delivery head.
    #[must_use]
    pub fn cut_snapshot(&self, at_ms: u64) -> (Self, Snapshot) {
        let snapshot = Snapshot {
            through_delivery_seq: self.head.as_ref().map_or(0, |head| head.delivery_seq),
            at_ms,
        };
        let mut next = self.clone();
        next.snapshot = Some(snapshot.clone());
        (next, snapshot)
    }

    /// Drop the prefix at or below the trim floor.
    ///
    /// The floor is the minimum ack, and also the expired prefix, and it
    /// never passes the latest snapshot. Without a snapshot nothing is dropped,
    /// so a new consumer can still re-bootstrap.
    #[must_use]
    pub fn trim(&self, now_ms: u64) -> Self {
        let floor = self.effective_floor(now_ms);
        if floor <= self.trimmed_through {
            return self.clone();
        }
        let mut next = self.clone();
        next.entries.retain(|entry| entry.delivery_seq > floor);
        next.trimmed_through = floor;
        next
    }

    /// Highest delivery sequence that may be dropped.
    #[must_use]
    pub fn effective_floor(&self, now_ms: u64) -> u64 {
        match &self.snapshot {
            Some(snapshot) => self
                .desired_floor(now_ms)
                .min(snapshot.through_delivery_seq),
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
