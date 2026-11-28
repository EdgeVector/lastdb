//! Grant and lease indexes for counting a verified member entry. Rebuild this
//! book from the owner-signed grant and lease rows of one database.
use crate::entry::{EntryHeader, LeaseBody};
use crate::ids::{DeviceId, EntryHash, NodeKey, StreamId};
use crate::kinds::Role;
use crate::payload::RecordPosition;
use std::collections::BTreeMap;
use std::ops::Bound;

/// Total order of positions: `(lamport, node_key, device_id)`.
pub(crate) type PosKey = (u64, NodeKey, DeviceId);

/// Grant role index key: `(lamport, grant_tie_rank(role), node_key, device_id)`.
pub(crate) type RoleKey = (u64, u8, NodeKey, DeviceId);

pub(crate) fn pos_key(position: RecordPosition) -> PosKey {
    (position.lamport, position.node_key, position.device_id)
}

fn role_key(position: RecordPosition, role: Role) -> RoleKey {
    (
        position.lamport,
        crate::grants::grant_tie_rank(role),
        position.node_key,
        position.device_id,
    )
}

/// One accepted lease entry.
#[derive(Clone, Debug)]
pub(crate) struct LeaseMeta {
    hash: EntryHash,
    device_id: DeviceId,
    writer_seq: u64,
    lamport: u64,
    authored_at_ms: u64,
    expires_at_ms: u64,
    closes_at_ms: u64,
    /// Closing points this entry records, by stream.
    closed: BTreeMap<StreamId, u64>,
}

/// A lease entry as nodes read it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaseView {
    /// Hash of the lease entry. A member's direct entry cites it in `seen_heads`.
    pub entry_hash: EntryHash,
    /// Publisher device stream that holds the entry.
    pub device_id: DeviceId,
    /// `writer_seq` of the entry on that stream.
    pub writer_seq: u64,
    /// Order value (`lamport`) of the lease entry.
    pub lamport: u64,
    /// The publisher's clock when it signed the lease (`authored_at_ms`).
    pub authored_at_ms: u64,
    /// A covered member signs direct entries only before this time.
    pub expires_at_ms: u64,
    /// A lease entry signed at or after this time closes the lease.
    pub closes_at_ms: u64,
}

/// Index of the verified owner grants and leases of one database.
#[derive(Clone, Debug, Default)]
pub struct LeaseBook {
    member_roles: BTreeMap<NodeKey, BTreeMap<RoleKey, Role>>,
    leases: BTreeMap<PosKey, LeaseMeta>,
    lease_by_hash: BTreeMap<EntryHash, PosKey>,
}

impl LeaseBook {
    /// True when `key` held the `write` role as of the accepted lease entry
    /// `lease`: its last grant strictly before the lease, with equal-lamport
    /// `revoked` winning ties, is `write`.
    #[must_use]
    pub fn lease_covers(&self, lease: &EntryHash, key: &NodeKey) -> bool {
        let Some(at) = self.lease_by_hash.get(lease) else {
            return false;
        };
        self.member_roles
            .get(key)
            .and_then(|roles| {
                roles
                    .range(..=(at.0, u8::MAX, at.1, at.2))
                    .rev()
                    .find(|(&(gl, _, gn, gd), _)| (gl, gn, gd) < *at)
            })
            .is_some_and(|(_, role)| *role == Role::Write)
    }

    /// The newest accepted lease by position, if any.
    #[must_use]
    pub fn latest_lease(&self) -> Option<LeaseView> {
        self.leases.values().next_back().map(view_of)
    }

    /// The accepted lease entry with this hash.
    #[must_use]
    pub fn lease(&self, lease: &EntryHash) -> Option<LeaseView> {
        let at = self.lease_by_hash.get(lease)?;
        self.leases.get(at).map(view_of)
    }

    /// The earliest later lease entry signed at or after the lease's `closes_at_ms`.
    fn closer_of(&self, at: PosKey, lease: &LeaseMeta) -> Option<PosKey> {
        self.leases
            .range((Bound::Excluded(at), Bound::Unbounded))
            .find(|(_, later)| later.authored_at_ms >= lease.closes_at_ms)
            .map(|(position, _)| *position)
    }

    /// Does an entry of `stream` at `writer_seq` that names `lease` count?
    /// It does unless the lease is closed and the entry lies past the closing
    /// point. A closing entry that does not list the stream gives point 0. An
    /// entry of a member that did not hold `write` as of the lease never counts.
    pub fn entry_counts(&self, lease: &EntryHash, stream: StreamId, writer_seq: u64) -> bool {
        if !self.lease_covers(lease, &stream.node_key) {
            return false;
        }
        let Some(at) = self.lease_by_hash.get(lease) else {
            return true;
        };
        let Some(meta) = self.leases.get(at) else {
            return true;
        };
        match self
            .closer_of(*at, meta)
            .and_then(|closer| self.leases.get(&closer))
        {
            None => true,
            Some(closer) => writer_seq <= closer.closed.get(&stream).copied().unwrap_or(0),
        }
    }

    /// The accepted lease that `seen` cites.
    pub fn lease_named(&self, seen: &[EntryHash]) -> Option<EntryHash> {
        seen.iter()
            .find(|hash| self.lease_by_hash.contains_key(*hash))
            .copied()
    }

    /// Index a verified publisher lease and report earlier leases whose closing
    /// point this entry now supplies. Re-reading a persisted row is idempotent.
    pub fn note_lease(
        &mut self,
        hash: EntryHash,
        position: RecordPosition,
        header: &EntryHeader,
        body: &LeaseBody,
    ) -> Vec<EntryHash> {
        let at = pos_key(position);
        let closed = body
            .closed
            .iter()
            .map(|stream| {
                (
                    StreamId {
                        node_key: stream.node_key,
                        device_id: stream.device_id,
                    },
                    stream.writer_seq,
                )
            })
            .collect();
        self.leases.insert(
            at,
            LeaseMeta {
                hash,
                device_id: header.device_id,
                writer_seq: header.writer_seq,
                lamport: header.lamport,
                authored_at_ms: header.authored_at_ms,
                expires_at_ms: body.expires_at_ms,
                closes_at_ms: body.closes_at_ms,
                closed,
            },
        );
        self.lease_by_hash.insert(hash, at);
        self.leases
            .range(..at)
            .filter(|(earlier, lease)| self.closer_of(**earlier, lease) == Some(at))
            .map(|(_, lease)| lease.hash)
            .collect()
    }

    /// Index a verified member grant and return the leases whose coverage may have changed.
    pub fn note_role(
        &mut self,
        key: NodeKey,
        position: RecordPosition,
        role: Role,
    ) -> Vec<EntryHash> {
        self.member_roles
            .entry(key)
            .or_default()
            .insert(role_key(position, role), role);
        self.leases
            .range((Bound::Excluded(pos_key(position)), Bound::Unbounded))
            .map(|(_, lease)| lease.hash)
            .collect()
    }

    /// Open leases eligible to be closed at the publisher's signing time.
    pub fn due_to_close(&self, authored_at_ms: u64) -> impl Iterator<Item = EntryHash> + '_ {
        self.leases
            .iter()
            .filter(move |(at, lease)| {
                lease.closes_at_ms <= authored_at_ms && self.closer_of(**at, lease).is_none()
            })
            .map(|(_, lease)| lease.hash)
    }
}

fn view_of(lease: &LeaseMeta) -> LeaseView {
    LeaseView {
        entry_hash: lease.hash,
        device_id: lease.device_id,
        writer_seq: lease.writer_seq,
        lamport: lease.lamport,
        authored_at_ms: lease.authored_at_ms,
        expires_at_ms: lease.expires_at_ms,
        closes_at_ms: lease.closes_at_ms,
    }
}
