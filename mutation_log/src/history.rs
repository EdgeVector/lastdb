//! Verified history `S` and `accept_entry` application.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::accept::{
    evaluate, position_of, stream_id_of, AcceptContext, AcceptOutcome, RejectReason, StreamHead,
};
use crate::entry::{GrantBody, SignedEntry};
use crate::grants::{AcceptedMeta, GrantState};
use crate::ids::{DatabaseId, DeviceId, EntryHash, NodeKey, StreamId};
use crate::kinds::EntryKind;
use crate::outbox::Outbox;
use crate::payload::{decode_record_payload, MaterializedRecord, RecordPosition};
use crate::wire::entry_hash;

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
    /// Folded grant state + cutoffs.
    grants: GrantState,
    /// Whole-record LWW state: `(namespace, record_key)`.
    records: BTreeMap<(String, String), MaterializedRecord>,
    /// Blob hashes marked deleted.
    deleted_blobs: BTreeSet<EntryHash>,
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
    /// Local outbox of authored entries not yet relay-acked.
    outbox: Outbox,
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
            deleted_blobs: BTreeSet::new(),
            relays: Vec::new(),
            publisher_heads: Vec::new(),
            pending: VecDeque::new(),
            rejects: Vec::new(),
            equivocations: Vec::new(),
            outbox: Outbox::new(),
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

    /// Borrow the outbox.
    #[must_use]
    pub fn outbox(&self) -> &Outbox {
        &self.outbox
    }

    /// Mutable outbox (ack / enqueue).
    pub fn outbox_mut(&mut self) -> &mut Outbox {
        &mut self.outbox
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

    fn evaluate_entry(&self, entry: &SignedEntry) -> AcceptOutcome {
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

    fn apply_accepted(&mut self, hash: EntryHash, entry: SignedEntry) {
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
        if let Some(body) = &grant_body {
            if is_publisher {
                self.grants.apply_grant(position, body);
            }
        }
        self.apply_effects(&entry, position);
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
    }

    fn apply_effects(&mut self, entry: &SignedEntry, position: RecordPosition) {
        match entry {
            SignedEntry::Data { body, .. } => {
                if body.stream == crate::kinds::StreamKind::Direct
                    || body.incorporated_from.is_some()
                {
                    self.merge_data(&body.namespace, &body.payload, position);
                }
            }
            SignedEntry::BlobDelete { body, .. } => {
                self.deleted_blobs.insert(body.sha256);
            }
            SignedEntry::Heads { body, .. } => {
                self.publisher_heads = body.publisher_heads.clone();
            }
            SignedEntry::Relays { body, .. } => {
                self.relays = body.relays.iter().map(|r| r.url.clone()).collect();
            }
            _ => {}
        }
    }

    fn merge_data(&mut self, namespace: &str, payload: &[u8], position: RecordPosition) {
        let Ok(decoded) = decode_record_payload(payload) else {
            return;
        };
        let key = (namespace.to_owned(), decoded.record_key);
        match self.records.get(&key) {
            Some(existing) if !position.beats(existing.position) => {}
            _ => {
                self.records.insert(
                    key,
                    MaterializedRecord {
                        position,
                        value: decoded.value,
                    },
                );
            }
        }
    }

    fn drain_pending(&mut self) {
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

    /// Enqueue an already-accepted locally authored entry onto the outbox.
    pub fn enqueue_outbox(
        &mut self,
        entry: SignedEntry,
    ) -> Result<EntryHash, crate::wire::EncodeError> {
        let hash = entry_hash(&entry)?;
        self.outbox.enqueue(hash, entry);
        Ok(hash)
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

/// Compute expected next lamport for authoring.
#[must_use]
pub fn next_lamport(prev_lamport: u64, seen_lamports: impl Iterator<Item = u64>) -> u64 {
    let mut max = prev_lamport;
    for l in seen_lamports {
        max = max.max(l);
    }
    1 + max
}

/// Borrow grant body from accepted meta helper for external folders.
#[must_use]
pub fn grant_body_of(meta: &AcceptedMeta) -> Option<&GrantBody> {
    meta.grant_body.as_ref()
}
