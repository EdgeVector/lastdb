//! Read-only views of [`VerifiedHistory`] for node grant and submission surfaces.

use crate::accept::StreamHead;
use crate::author::AuthorCtx;
use crate::entry::{DeviceHead, SignedEntry};
use crate::ids::{DeviceId, EntryHash, NodeKey, StreamId};
use crate::kinds::Grantee;

use super::VerifiedHistory;

/// The latest accepted publisher grant entry (the grant chain tip).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GrantTip {
    /// Hash of the grant entry.
    pub entry_hash: EntryHash,
    /// `writer_seq` of the grant entry on its publisher device stream.
    pub writer_seq: u64,
    /// Order value (`lamport`) of the grant entry.
    pub lamport: u64,
}

impl VerifiedHistory {
    /// Hash and lamport of the accepted `birth` entry, when present.
    #[must_use]
    pub fn birth_entry(&self) -> Option<(EntryHash, u64)> {
        self.birth
    }

    /// Winning entry hash and lamport of the declared schema for `namespace`, if any.
    #[must_use]
    pub fn schema_entry(&self, namespace: &str) -> Option<(EntryHash, u64)> {
        self.schema_positions
            .get(namespace)
            .map(|(pos, hash)| (*hash, pos.lamport))
    }

    /// Cite the accepted `birth` entry and every accepted publisher device stream
    /// tip (highest lamport first) into `ctx`.
    pub fn cite_publisher_streams(&self, ctx: &mut AuthorCtx) {
        if let Some((hash, lamport)) = self.birth {
            ctx.cite(hash, lamport);
        }
        let Some(owner) = self.owner else {
            return;
        };
        let mut heads = self.stream_heads_of(owner);
        heads.sort_by_key(|(dev, head)| (std::cmp::Reverse(head.lamport), *dev));
        for (_, head) in heads {
            ctx.cite(head.entry_hash, head.lamport);
        }
    }

    /// Latest publisher `heads` attestation tips (empty before the first attestation).
    #[must_use]
    pub fn publisher_heads(&self) -> &[DeviceHead] {
        &self.publisher_heads
    }

    /// The current tip of every device stream of the publisher, sorted by device id.
    ///
    /// This is the content of a `heads` attestation the publisher signs now. It is
    /// never empty once the publisher's birth entry is accepted.
    #[must_use]
    pub fn publisher_stream_tips(&self) -> Vec<DeviceHead> {
        let Some(owner) = self.owner else {
            return Vec::new();
        };
        let mut tips: Vec<DeviceHead> = self
            .stream_heads_of(owner)
            .into_iter()
            .map(|(device_id, head)| DeviceHead {
                device_id,
                writer_seq: head.writer_seq,
                entry_hash: head.entry_hash,
            })
            .collect();
        tips.sort_by_key(|tip| tip.device_id);
        tips
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

    /// Accepted entries that are not on `stream`, in ascending Lamport order.
    ///
    /// An entry's Lamport is above that of its predecessor and of every entry it
    /// cites, so replaying in this order meets each dependency before its user.
    #[must_use]
    pub fn accepted_outside(&self, stream: &StreamId) -> Vec<&SignedEntry> {
        let mut found: Vec<&SignedEntry> = self
            .accepted
            .values()
            .map(|meta| &meta.entry)
            .filter(|entry| crate::accept::stream_id_of(entry) != *stream)
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

    /// True when an entry of `stream` waits in the pending buffer. The tip of
    /// that stream is then not the real one, and signing on it would fork it.
    #[must_use]
    pub fn pending_on(&self, stream: &StreamId) -> bool {
        self.pending
            .iter()
            .any(|entry| crate::accept::stream_id_of(entry) == *stream)
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
            .filter_map(|(hash, meta)| {
                if !meta.is_publisher {
                    return None;
                }
                let body = meta.grant_body.as_ref()?;
                Some((*hash, meta, body.role))
            })
            .max_by_key(|(_, meta, role)| {
                (
                    meta.position.lamport,
                    crate::grants::grant_tie_rank(*role),
                    meta.position.node_key,
                    meta.position.device_id,
                )
            })
            .map(|(hash, meta, _)| GrantTip {
                entry_hash: hash,
                writer_seq: meta.entry.header().writer_seq,
                lamport: meta.position.lamport,
            })
    }

    /// Latest accepted publisher grant targeting `grantee` in merge order, if any.
    #[must_use]
    pub fn latest_grant_for(&self, grantee: Grantee) -> Option<(EntryHash, u64)> {
        self.accepted
            .iter()
            .filter_map(|(hash, meta)| {
                if !meta.is_publisher {
                    return None;
                }
                let body = meta.grant_body.as_ref()?;
                if body.grantee != grantee {
                    return None;
                }
                Some((*hash, meta.position, body.role))
            })
            .max_by_key(|(_, pos, role)| {
                (
                    pos.lamport,
                    crate::grants::grant_tie_rank(*role),
                    pos.node_key,
                    pos.device_id,
                )
            })
            .map(|(hash, pos, _)| (hash, pos.lamport))
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
