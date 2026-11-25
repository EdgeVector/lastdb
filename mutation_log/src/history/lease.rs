//! Write leases: the counting rule for a member's direct entries.
//!
//! A member's direct entry cites one lease entry in `seen_heads`. It joins its
//! stream when the member held the `write` role as of that lease. It then
//! counts, unless the lease is closed and the entry lies past the closing
//! point. The rule reads signed entries only: no clock takes part. This file is
//! the only place that decides it.

use std::collections::BTreeMap;
use std::ops::Bound;

use crate::accept::position_of;
use crate::entry::{ClosedStream, DataBody, EntryHeader, LeaseBody, SignedEntry};
use crate::ids::{DeviceId, EntryHash, NodeKey, StreamId};
use crate::kinds::{Role, StreamKind};
use crate::payload::{decode_record_payload, RecordPayload, RecordPosition};

use super::VerifiedHistory;

/// Total order of positions: `(lamport, node_key, device_id)`.
pub(super) type PosKey = (u64, NodeKey, DeviceId);

/// Grant role index key: `(lamport, grant_tie_rank(role), node_key, device_id)`.
pub(super) type RoleKey = (u64, u8, NodeKey, DeviceId);

pub(super) fn pos_key(position: RecordPosition) -> PosKey {
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
pub(super) struct LeaseMeta {
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

/// One accepted direct entry of a member, under the lease it names.
#[derive(Clone, Copy, Debug)]
pub(super) struct Under {
    hash: EntryHash,
    stream: StreamId,
    writer_seq: u64,
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

impl VerifiedHistory {
    /// True when `key` held the `write` role as of the accepted lease entry
    /// `lease`: its last grant strictly before the lease, with equal-lamport
    /// `revoked` winning ties, is `write`.
    #[must_use]
    pub fn lease_covers(&self, lease: &EntryHash, key: &NodeKey) -> bool {
        self.writer_under(lease, key)
    }

    pub(crate) fn writer_under(&self, lease: &EntryHash, key: &NodeKey) -> bool {
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

    /// True when the entry names a lease and does not count.
    #[must_use]
    pub fn is_excluded(&self, entry: &EntryHash) -> bool {
        self.excluded.contains(entry)
    }

    /// Accepted entries that name a lease and do not count, in position order.
    #[must_use]
    pub fn excluded_entries(&self) -> Vec<&SignedEntry> {
        let mut found: Vec<&SignedEntry> = self
            .excluded
            .iter()
            .filter_map(|hash| self.accepted.get(hash))
            .map(|meta| &meta.entry)
            .collect();
        found.sort_by_key(|entry| {
            let header = entry.header();
            (
                header.lamport,
                header.node_key,
                header.device_id,
                header.writer_seq,
            )
        });
        found
    }

    /// The `closed` list of a lease entry that the publisher signs at
    /// `authored_at_ms`: for each device stream that wrote under a lease which
    /// has no closing entry yet and whose `closes_at_ms` has passed, the last
    /// entry of the stream that this history holds. Ascending by node key and
    /// device id.
    #[must_use]
    pub fn closing_streams(&self, authored_at_ms: u64) -> Vec<ClosedStream> {
        let mut points: BTreeMap<StreamId, u64> = BTreeMap::new();
        for (at, lease) in &self.leases {
            if lease.closes_at_ms > authored_at_ms || self.closer_of(*at, lease).is_some() {
                continue;
            }
            for under in self.under_lease.get(&lease.hash).into_iter().flatten() {
                let seq = self
                    .heads
                    .get(&under.stream)
                    .map_or(under.writer_seq, |head| head.writer_seq);
                points.insert(under.stream, seq);
            }
        }
        points
            .into_iter()
            .map(|(stream, writer_seq)| ClosedStream {
                node_key: stream.node_key,
                device_id: stream.device_id,
                writer_seq,
            })
            .collect()
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
    fn entry_counts(&self, lease: &EntryHash, stream: StreamId, writer_seq: u64) -> bool {
        if !self.writer_under(lease, &stream.node_key) {
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
    fn lease_named(&self, seen: &[EntryHash]) -> Option<EntryHash> {
        seen.iter()
            .find(|hash| self.lease_by_hash.contains_key(*hash))
            .copied()
    }

    /// Record a member's accepted direct entry under the lease it names.
    pub(super) fn note_member_direct(&mut self, header: &EntryHeader, hash: EntryHash) {
        let Some(lease) = self.lease_named(&header.seen_heads) else {
            return;
        };
        let stream = StreamId {
            node_key: header.node_key,
            device_id: header.device_id,
        };
        self.under_lease.entry(lease).or_default().push(Under {
            hash,
            stream,
            writer_seq: header.writer_seq,
        });
        if !self.entry_counts(&lease, stream, header.writer_seq) {
            self.excluded.insert(hash);
        }
    }

    /// Record an accepted lease entry. When it closes earlier leases, the
    /// entries that now count differently are found and the records they wrote
    /// are recomputed.
    pub(super) fn note_lease(
        &mut self,
        hash: EntryHash,
        position: RecordPosition,
        header: &EntryHeader,
        body: &LeaseBody,
    ) {
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
        let closed_now: Vec<EntryHash> = self
            .leases
            .range(..at)
            .filter(|(earlier, lease)| self.closer_of(**earlier, lease) == Some(at))
            .map(|(_, lease)| lease.hash)
            .collect();
        let mut changed: Vec<EntryHash> = Vec::new();
        for lease in closed_now {
            let held: Vec<Under> = self.under_lease.get(&lease).cloned().unwrap_or_default();
            for under in held {
                let counts = self.entry_counts(&lease, under.stream, under.writer_seq);
                let flipped = if counts {
                    self.excluded.remove(&under.hash)
                } else {
                    self.excluded.insert(under.hash)
                };
                if flipped {
                    changed.push(under.hash);
                }
            }
        }
        if !changed.is_empty() {
            self.recompute_written_by(&changed);
        }
    }

    /// Decode a direct or publisher-incorporated data entry regardless of lease
    /// exclusion, returning `(author, namespace, payload)`.
    pub(super) fn decoded_direct(
        &self,
        header: &EntryHeader,
        body: &DataBody,
        is_publisher: bool,
    ) -> Option<(NodeKey, String, RecordPayload)> {
        let _ = self;
        let merges =
            body.stream == StreamKind::Direct || (is_publisher && body.incorporated_from.is_some());
        if !merges {
            return None;
        }
        let author = body
            .incorporated_from
            .as_ref()
            .filter(|_| is_publisher)
            .map_or(header.node_key, |from| from.node_key);
        let decoded = decode_record_payload(&body.payload).ok()?;
        Some((author, body.namespace.clone(), decoded))
    }

    /// Keep a publisher grant in the per-member role index that
    /// [`Self::writer_under`] reads, and recompute any later leases whose
    /// coverage of that member changed.
    pub(super) fn note_member_role(&mut self, entry: &SignedEntry) {
        let SignedEntry::Grant { body, .. } = entry else {
            return;
        };
        let crate::kinds::Grantee::Member(key) = body.grantee else {
            return;
        };
        let pos = position_of(entry);
        let grant_at = pos_key(pos);
        self.member_roles
            .entry(key)
            .or_default()
            .insert(role_key(pos, body.role), body.role);
        let affected: Vec<EntryHash> = self
            .leases
            .range((Bound::Excluded(grant_at), Bound::Unbounded))
            .map(|(_, lease)| lease.hash)
            .collect();
        let mut changed: Vec<EntryHash> = Vec::new();
        for lease in affected {
            let held: Vec<Under> = self.under_lease.get(&lease).cloned().unwrap_or_default();
            for under in held {
                if under.stream.node_key != key {
                    continue;
                }
                let counts = self.entry_counts(&lease, under.stream, under.writer_seq);
                let flipped = if counts {
                    self.excluded.remove(&under.hash)
                } else {
                    self.excluded.insert(under.hash)
                };
                if flipped {
                    changed.push(under.hash);
                }
            }
        }
        if !changed.is_empty() {
            self.recompute_written_by(&changed);
        }
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
