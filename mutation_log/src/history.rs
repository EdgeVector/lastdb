//! Verified history `S` and `accept_entry` application.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::accept::{
    evaluate_with_leases, position_of, stream_id_of, AcceptContext, AcceptOutcome, RejectReason,
    StreamHead,
};
use crate::entry::SignedEntry;
use crate::grants::{AcceptedMeta, GrantState};
use crate::ids::{DatabaseId, DeviceId, EntryHash, NodeKey, StreamId};
use crate::kinds::{EntryKind, Role};
use crate::payload::MaterializedRecord;
use crate::wire::entry_hash;

mod effects;
mod lease;
mod recompute;
mod view;

pub use lease::LeaseView;
pub use view::GrantTip;

use lease::{LeaseMeta, PosKey, RoleKey, Under};
use recompute::WriteMeta;

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
    /// Hash and lamport of the accepted birth entry.
    birth: Option<(EntryHash, u64)>,
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
    /// Declared schema definitions by namespace (highest position wins).
    schemas: BTreeMap<String, Vec<u8>>,
    /// Winning position and entry hash of each declared schema definition.
    schema_positions: BTreeMap<String, (crate::payload::RecordPosition, EntryHash)>,
    /// Accepted schema declarations per namespace, ordered by position.
    schema_decls:
        BTreeMap<String, BTreeMap<PosKey, (crate::payload::RecordPosition, EntryHash, Vec<u8>)>>,
    /// Latest publisher relay URLs.
    relays: Vec<String>,
    /// Winning position of `relays`.
    relays_position: Option<crate::payload::RecordPosition>,
    /// Latest publisher heads tips.
    publisher_heads: Vec<crate::entry::DeviceHead>,
    /// Winning position of `publisher_heads`.
    publisher_heads_position: Option<crate::payload::RecordPosition>,
    /// Pending entries waiting on deps.
    pending: VecDeque<SignedEntry>,
    /// Reject evidence.
    rejects: Vec<RejectEvidence>,
    /// Each member's role in publisher grant order, by `(lamport, tie_rank, node_key, device_id)`.
    member_roles: BTreeMap<NodeKey, BTreeMap<RoleKey, Role>>,
    /// Accepted lease entries by position.
    leases: BTreeMap<PosKey, LeaseMeta>,
    /// Position of each accepted lease entry.
    lease_by_hash: BTreeMap<EntryHash, PosKey>,
    /// Accepted direct entries of members, by the lease they name.
    under_lease: BTreeMap<EntryHash, Vec<Under>>,
    /// Entries that name a lease and do not count.
    excluded: BTreeSet<EntryHash>,
    /// Per record key, the best bundle write that a non-bundle write shadowed
    /// or displaced. A recompute after a lease closing needs it.
    bundle_floor: BTreeMap<(String, String), MaterializedRecord>,
    /// Ordered metadata of every direct and bundle write per record key.
    write_meta: BTreeMap<(String, String), BTreeMap<PosKey, WriteMeta>>,
}

impl VerifiedHistory {
    /// Empty history bound to `database_id` (no birth yet).
    #[must_use]
    pub fn new(database_id: DatabaseId) -> Self {
        Self {
            database_id,
            owner: None,
            birth: None,
            accepted: BTreeMap::new(),
            heads: BTreeMap::new(),
            by_seq: BTreeMap::new(),
            frozen: BTreeSet::new(),
            grants: GrantState::empty(),
            records: BTreeMap::new(),
            schemas: BTreeMap::new(),
            schema_positions: BTreeMap::new(),
            schema_decls: BTreeMap::new(),
            relays: Vec::new(),
            relays_position: None,
            publisher_heads: Vec::new(),
            publisher_heads_position: None,
            pending: VecDeque::new(),
            rejects: Vec::new(),
            member_roles: BTreeMap::new(),
            leases: BTreeMap::new(),
            lease_by_hash: BTreeMap::new(),
            under_lease: BTreeMap::new(),
            excluded: BTreeSet::new(),
            bundle_floor: BTreeMap::new(),
            write_meta: BTreeMap::new(),
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
        evaluate_with_leases(entry, &ctx, true)
    }

    fn note_reject(&mut self, reason: RejectReason, entry: SignedEntry) {
        if matches!(reason, RejectReason::Equivocation { .. }) {
            self.frozen.insert(stream_id_of(&entry));
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
        if let SignedEntry::Birth { body, .. } = &entry {
            self.owner = Some(body.owner);
            self.birth = Some((hash, h.lamport));
        }
        self.apply_effects(&entry, hash, position, is_publisher);
        let rebuild = is_publisher && grant_body.is_some();
        let grant_ref = rebuild.then(|| entry.clone());
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
        if let Some(grant_entry) = grant_ref {
            self.note_member_role(&grant_entry);
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
