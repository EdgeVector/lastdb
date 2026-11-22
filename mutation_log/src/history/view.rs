//! Read-only views of [`VerifiedHistory`] for node grant and submission surfaces.

use crate::accept::StreamHead;
use crate::entry::{DeviceHead, SignedEntry};
use crate::ids::{DeviceId, EntryHash, NodeKey, StreamId};

use super::VerifiedHistory;

/// The latest accepted publisher grant entry (the grant chain tip).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GrantTip {
    /// Hash of the grant entry.
    pub entry_hash: EntryHash,
    /// `writer_seq` of the grant entry on its publisher device stream.
    pub writer_seq: u64,
}

impl VerifiedHistory {
    /// Latest publisher `heads` attestation tips (empty before the first attestation).
    #[must_use]
    pub fn publisher_heads(&self) -> &[DeviceHead] {
        &self.publisher_heads
    }

    /// Tips of every accepted device stream written by `node_key`.
    #[must_use]
    pub fn stream_heads_of(&self, node_key: NodeKey) -> Vec<(DeviceId, StreamHead)> {
        self.heads
            .iter()
            .filter(|(stream, _)| stream.node_key == node_key)
            .map(|(stream, head)| (stream.device_id, *head))
            .collect()
    }

    /// Hash accepted at `(stream, writer_seq)`.
    #[must_use]
    pub fn entry_hash_at(&self, stream: &StreamId, writer_seq: u64) -> Option<EntryHash> {
        self.by_seq.get(&(*stream, writer_seq)).copied()
    }

    /// Latest accepted publisher grant in merge order, if any.
    #[must_use]
    pub fn latest_grant(&self) -> Option<GrantTip> {
        self.accepted
            .iter()
            .filter(|(_, meta)| meta.is_publisher && meta.grant_body.is_some())
            .max_by_key(|(_, meta)| {
                (
                    meta.position.lamport,
                    meta.position.node_key,
                    meta.position.device_id,
                )
            })
            .map(|(hash, meta)| GrantTip {
                entry_hash: *hash,
                writer_seq: meta.entry.header().writer_seq,
            })
    }

    /// Hash and lamport of every accepted publisher grant, for a member's `seen_heads`.
    #[must_use]
    pub fn publisher_grants(&self) -> Vec<(EntryHash, u64)> {
        self.accepted
            .iter()
            .filter(|(_, meta)| meta.is_publisher && meta.grant_body.is_some())
            .map(|(hash, meta)| (*hash, meta.position.lamport))
            .collect()
    }

    /// True when an accepted publisher data entry incorporates the named proposal.
    #[must_use]
    pub fn proposal_incorporated(&self, proposal: &StreamId, writer_seq: u64) -> bool {
        self.accepted.values().any(|meta| {
            meta.is_publisher
                && matches!(
                    &meta.entry,
                    SignedEntry::Data { body, .. } if body.incorporated_from.as_ref().is_some_and(|from| {
                        from.node_key == proposal.node_key
                            && from.device_id == proposal.device_id
                            && from.writer_seq == writer_seq
                    })
                )
        })
    }

    /// True when the publisher signed "not accepted" for the proposal with this
    /// hash: an accepted publisher `heads` entry cites it in `seen_heads`.
    ///
    /// An incorporation cites the proposal too, but it is a data entry. The
    /// publisher signs a `heads` entry for a proposal only to decline it.
    #[must_use]
    pub fn proposal_declined(&self, proposal: &EntryHash) -> bool {
        self.accepted.values().any(|meta| {
            meta.is_publisher
                && matches!(&meta.entry, SignedEntry::Heads { .. })
                && meta.entry.header().seen_heads.contains(proposal)
        })
    }

    /// Accepted proposals the publisher has not decided yet, in ascending
    /// Lamport order, so one member's proposals are decided in the order it
    /// signed them.
    #[must_use]
    pub fn unresolved_proposals(&self) -> Vec<&SignedEntry> {
        let mut found: Vec<&SignedEntry> = self
            .accepted
            .iter()
            .filter_map(|(hash, meta)| {
                let SignedEntry::Data { body, .. } = &meta.entry else {
                    return None;
                };
                if body.stream != crate::kinds::StreamKind::Proposal {
                    return None;
                }
                let header = meta.entry.header();
                let stream = StreamId {
                    node_key: header.node_key,
                    device_id: header.device_id,
                };
                if self.proposal_incorporated(&stream, header.writer_seq)
                    || self.proposal_declined(hash)
                {
                    return None;
                }
                Some(&meta.entry)
            })
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
}
