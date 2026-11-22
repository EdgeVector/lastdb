//! Verified history `S` and `accept_entry` application.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::accept::{
    evaluate, position_of, stream_id_of, AcceptContext, AcceptOutcome, RejectReason, StreamHead,
};
use crate::entry::SignedEntry;
use crate::grants::{AcceptedMeta, GrantState};
use crate::ids::{DatabaseId, DeviceId, EntryHash, NodeKey, StreamId};
use crate::kinds::EntryKind;
use crate::payload::MaterializedRecord;
use crate::wire::entry_hash;

mod effects;
mod view;

pub use view::GrantTip;

/// Equivocation evidence kept when a stream freezes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EquivocationEvidence {
    /// Frozen stream.
    pub stream: StreamId,
    /// Writer seq that forked.
    pub writer_seq: u64,
    /// First accepted entry hash at that seq.
    pub held_hash: EntryHash,
    /// Conflicting incoming entry hash.
    pub new_hash: EntryHash,
    /// Conflicting signed entry (evidence).
    pub conflicting: SignedEntry,
}

/// Rejected entry retained as evidence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RejectEvidence {
    /// Rejection reason.
    pub reason: RejectReason,
    /// Rejected entry.
    pub entry: SignedEntry,
}

/// Local verified history for one `database_id`.
#[derive(Clone, Debug)]
pub struct VerifiedHistory {
    /// Database this history is bound to.
    database_id: DatabaseId,
    /// Owner from the accepted birth entry.
    owner: Option<NodeKey>,
    /// Accepted entries by hash.
    accepted: BTreeMap<EntryHash, AcceptedMeta>,
    /// Per-stream tip.
    heads: BTreeMap<StreamId, StreamHead>,
    /// Hash at each `(stream, writer_seq)`.
    by_seq: BTreeMap<(StreamId, u64), EntryHash>,
    /// Frozen streams (equivocation).
    frozen: BTreeSet<StreamId>,
    /// Folded grant state.
    grants: GrantState,
    /// Whole-record LWW state: `(namespace, record_key)`.
    records: BTreeMap<(String, String), MaterializedRecord>,
    /// Declared schema definitions by namespace (last accepted wins).
    schemas: BTreeMap<String, Vec<u8>>,
    /// Blob hashes marked deleted.
    deleted_blobs: BTreeSet<EntryHash>,
    /// Digests referenced by accepted bundle entries (blocks invalid `blob_delete`).
    live_bundle_blobs: BTreeSet<crate::ids::Hex32>,
    /// Latest publisher relay URLs.
    relays: Vec<String>,
    /// Latest publisher heads tips.
    publisher_heads: Vec<crate::entry::DeviceHead>,
    /// Pending entries waiting on deps.
    pending: VecDeque<SignedEntry>,
    /// Reject evidence.
    rejects: Vec<RejectEvidence>,
    /// Equivocation evidence.
    equivocations: Vec<EquivocationEvidence>,
}

impl VerifiedHistory {
    /// Empty history bound to `database_id` (no birth yet).
    #[must_use]
    pub fn new(database_id: DatabaseId) -> Self {
        Self {
            database_id,
            owner: None,
            accepted: BTreeMap::new(),
            heads: BTreeMap::new(),
            by_seq: BTreeMap::new(),
            frozen: BTreeSet::new(),
            grants: GrantState::empty(),
            records: BTreeMap::new(),
            schemas: BTreeMap::new(),
            deleted_blobs: BTreeSet::new(),
            live_bundle_blobs: BTreeSet::new(),
            relays: Vec::new(),
            publisher_heads: Vec::new(),
            pending: VecDeque::new(),
            rejects: Vec::new(),
            equivocations: Vec::new(),
        }
    }

    /// Database id.
    #[must_use]
    pub fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Birth owner, when birth has been accepted.
    #[must_use]
    pub fn owner(&self) -> Option<NodeKey> {
        self.owner
    }

    /// Stream tip, if any.
    #[must_use]
    pub fn stream_head(&self, stream: &StreamId) -> Option<StreamHead> {
        self.heads.get(stream).copied()
    }

    /// True when the stream is frozen forever.
    #[must_use]
    pub fn is_frozen(&self, stream: &StreamId) -> bool {
        self.frozen.contains(stream)
    }

    /// Borrow folded grants.
    #[must_use]
    pub fn grants(&self) -> &GrantState {
        &self.grants
    }

    /// Borrow materialized records.
    #[must_use]
    pub fn records(&self) -> &BTreeMap<(String, String), MaterializedRecord> {
        &self.records
    }

    /// Borrow declared schema definitions (`namespace` → JSON bytes).
    #[must_use]
    pub fn schemas(&self) -> &BTreeMap<String, Vec<u8>> {
        &self.schemas
    }

    /// Equivocation evidence.
    #[must_use]
    pub fn equivocations(&self) -> &[EquivocationEvidence] {
        &self.equivocations
    }

    /// Reject evidence.
    #[must_use]
    pub fn rejects(&self) -> &[RejectEvidence] {
        &self.rejects
    }

    /// Pending buffer length.
    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// Latest publisher relay URLs.
    #[must_use]
    pub fn relays(&self) -> &[String] {
        &self.relays
    }

    /// Deleted blob hashes.
    #[must_use]
    pub fn deleted_blobs(&self) -> &BTreeSet<EntryHash> {
        &self.deleted_blobs
    }

    /// Look up an accepted entry by hash.
    #[must_use]
    pub fn get_accepted(&self, hash: &EntryHash) -> Option<&AcceptedMeta> {
        self.accepted.get(hash)
    }

    /// Run `AcceptEntry(E, S)`. Applies on accept; buffers on pending; keeps evidence on reject.
    pub fn accept_entry(&mut self, entry: SignedEntry) -> AcceptOutcome {
        let outcome = self.evaluate_entry(&entry);
        match &outcome {
            AcceptOutcome::Accept {
                entry_hash,
                duplicate,
            } => {
                if !*duplicate {
                    self.apply_accepted(*entry_hash, entry);
                    self.drain_pending();
                }
            }
            AcceptOutcome::Pending { .. } => {
                self.pending.push_back(entry);
            }
            AcceptOutcome::Reject { reason } => {
                self.note_reject(reason.clone(), entry);
            }
        }
        outcome
    }

    pub(crate) fn evaluate_entry(&self, entry: &SignedEntry) -> AcceptOutcome {
        let stream = stream_id_of(entry);
        let seq = entry.header().writer_seq;
        let ctx = AcceptContext {
            database_id: self.database_id,
            owner: self.owner,
            stream_head: self.heads.get(&stream).copied(),
            stream_frozen: self.frozen.contains(&stream),
            hash_at_seq: self.by_seq.get(&(stream, seq)).copied(),
            lookup: &|h| self.accepted.get(h),
            grants: &self.grants,
        };
        evaluate(entry, &ctx)
    }

    fn note_reject(&mut self, reason: RejectReason, entry: SignedEntry) {
        if let RejectReason::Equivocation {
            held_hash,
            new_hash,
        } = &reason
        {
            let stream = stream_id_of(&entry);
            let writer_seq = entry.header().writer_seq;
            self.frozen.insert(stream);
            self.equivocations.push(EquivocationEvidence {
                stream,
                writer_seq,
                held_hash: *held_hash,
                new_hash: *new_hash,
                conflicting: entry.clone(),
            });
        }
        self.rejects.push(RejectEvidence { reason, entry });
    }

    pub(crate) fn apply_accepted(&mut self, hash: EntryHash, entry: SignedEntry) {
        let stream = stream_id_of(&entry);
        let h = entry.header().clone();
        let position = position_of(&entry);
        let is_publisher = self.owner == Some(h.node_key) || (entry.kind() == EntryKind::Birth);
        let grant_body = match &entry {
            SignedEntry::Grant { body, .. } => Some(body.clone()),
            _ => None,
        };
        if entry.kind() == EntryKind::Birth {
            if let SignedEntry::Birth { body, .. } = &entry {
                self.owner = Some(body.owner);
            }
        }
        self.apply_effects(&entry, hash, position, is_publisher);
        let rebuild = is_publisher && grant_body.is_some();
        let meta = AcceptedMeta {
            position,
            is_publisher,
            grant_body,
            entry,
        };
        self.by_seq.insert((stream, h.writer_seq), hash);
        self.heads.insert(
            stream,
            StreamHead {
                entry_hash: hash,
                writer_seq: h.writer_seq,
                lamport: h.lamport,
            },
        );
        self.accepted.insert(hash, meta);
        if rebuild {
            self.rebuild_grants();
        }
    }

    fn rebuild_grants(&mut self) {
        self.grants = GrantState::rebuild_from(self.accepted.values().filter_map(|m| {
            if m.is_publisher {
                m.grant_body.as_ref().map(|b| (m.position, b))
            } else {
                None
            }
        }));
    }

    pub(crate) fn drop_pending_hash(&mut self, hash: &EntryHash) {
        self.pending
            .retain(|e| entry_hash(e).ok().as_ref() != Some(hash));
    }

    pub(crate) fn drain_pending(&mut self) {
        let mut progressed = true;
        while progressed {
            progressed = false;
            let mut i = 0;
            while i < self.pending.len() {
                let outcome = self.evaluate_entry(&self.pending[i]);
                match outcome {
                    AcceptOutcome::Accept {
                        entry_hash,
                        duplicate,
                    } => {
                        let entry = self.pending.remove(i).expect("pending index");
                        if !duplicate {
                            self.apply_accepted(entry_hash, entry);
                        }
                        progressed = true;
                    }
                    AcceptOutcome::Reject { reason } => {
                        let entry = self.pending.remove(i).expect("pending index");
                        self.note_reject(reason, entry);
                        progressed = true;
                    }
                    AcceptOutcome::Pending { .. } => {
                        i += 1;
                    }
                }
            }
        }
    }

    /// Convenience: stream id for `(node_key, device_id)`.
    #[must_use]
    pub fn stream(node_key: NodeKey, device_id: DeviceId) -> StreamId {
        StreamId {
            node_key,
            device_id,
        }
    }
}
