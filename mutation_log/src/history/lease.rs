//! Keep the materialized history in sync with the verified lease book.
use super::VerifiedHistory;
use crate::accept::position_of;
use crate::entry::{ClosedStream, DataBody, EntryHeader, LeaseBody, SignedEntry};
use crate::ids::{EntryHash, NodeKey, StreamId};
use crate::kinds::StreamKind;
use crate::lease_book::LeaseView;
use crate::payload::{decode_record_payload, RecordPayload, RecordPosition};
use std::collections::BTreeMap;

/// One accepted direct entry of a member, under the lease it names.
#[derive(Clone, Copy, Debug)]
pub(super) struct Under {
    hash: EntryHash,
    stream: StreamId,
    writer_seq: u64,
}

impl VerifiedHistory {
    /// The grant and lease index for this database.
    pub fn lease_book(&self) -> &crate::LeaseBook {
        &self.lease_book
    }

    /// Whether the member held write access at the lease's position.
    pub fn lease_covers(&self, lease: &EntryHash, key: &NodeKey) -> bool {
        self.lease_book.lease_covers(lease, key)
    }

    /// The newest lease by position.
    pub fn latest_lease(&self) -> Option<LeaseView> {
        self.lease_book.latest_lease()
    }

    /// A verified lease by hash.
    pub fn lease(&self, lease: &EntryHash) -> Option<LeaseView> {
        self.lease_book.lease(lease)
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
        for lease in self.lease_book.due_to_close(authored_at_ms) {
            for under in self.under_lease.get(&lease).into_iter().flatten() {
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

    /// Record a member's accepted direct entry under the lease it names.
    pub(super) fn note_member_direct(&mut self, header: &EntryHeader, hash: EntryHash) {
        let Some(lease) = self.lease_book.lease_named(&header.seen_heads) else {
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
        if !self
            .lease_book
            .entry_counts(&lease, stream, header.writer_seq)
        {
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
        let closed = self.lease_book.note_lease(hash, position, header, body);
        self.recount(closed, None);
    }

    /// Decode a direct or publisher-incorporated data entry regardless of lease
    /// exclusion, returning `(author, namespace, payload)`.
    pub(super) fn decoded_direct(
        &self,
        header: &EntryHeader,
        body: &DataBody,
        is_publisher: bool,
    ) -> Option<(NodeKey, String, RecordPayload)> {
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
        let (namespace, payload) = self.open_data(header, body)?;
        let decoded = decode_record_payload(&payload).ok()?;
        Some((author, namespace, decoded))
    }

    /// Recalculate affected writes when an earlier grant arrives after a lease.
    pub(super) fn note_member_role(&mut self, entry: &SignedEntry) {
        let SignedEntry::Grant { body, .. } = entry else {
            return;
        };
        let crate::kinds::Grantee::Member(key) = body.grantee else {
            return;
        };
        let affected = self
            .lease_book
            .note_role(key, position_of(entry), body.role);
        self.recount(affected, Some(key));
    }

    fn recount(&mut self, leases: Vec<EntryHash>, member: Option<NodeKey>) {
        let mut changed = Vec::new();
        for lease in leases {
            for under in self.under_lease.get(&lease).into_iter().flatten() {
                if member.is_some_and(|key| key != under.stream.node_key) {
                    continue;
                }
                let counts = self
                    .lease_book
                    .entry_counts(&lease, under.stream, under.writer_seq);
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
