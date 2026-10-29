use super::{AtomSize, DeletedAtom};
use crate::atom::Atom;
use crate::db_operations::atom_store::PerKeyRecord;
use serde_json::Value;
use std::collections::HashMap;

/// One tip record change. `new == None` removes the tip record.
pub(crate) struct TipChange {
    /// The full storage key of the `mk:` record, scope prefix included.
    pub key: String,
    pub new: Option<PerKeyRecord>,
}

/// One conflict record that holds references to atoms.
pub(crate) struct RecordRefs {
    /// The full storage key of the record that names the atoms.
    pub key: String,
    /// The distinct atoms that the record names.
    pub atoms: Vec<String>,
    /// File references stored only in an archived tip's metadata.
    pub files: Vec<String>,
    /// Rows that only a new record writes: its retention queue entry and its
    /// slot directory entry. A repeat of a decision writes none of them.
    pub new_rows: Vec<(String, Value)>,
    /// The molecule the record belongs to. When the record is new, the planner
    /// raises `records` in the header of this molecule, in the same batch. A
    /// release of the record lowers it in the batch of the release.
    pub molecule: String,
}

/// One kept record to release. The planner reads the history row at `history`
/// in the write gate, lowers one reference for each distinct atom the row
/// names, lowers `records` of the molecule that the row names, and deletes the
/// row and `deletes` in the same batch.
pub(crate) struct ReleaseRequest {
    /// The full storage key of the history row that holds the references.
    pub history: String,
    /// The other rows of the record (conflict row, directory entry, queue
    /// entry). They go with the history row, or when it is already gone.
    pub deletes: Vec<Vec<u8>>,
}

/// A delete that runs only while the key still holds the value the caller read.
/// The planner reads the key in the write gate. A key that holds other bytes, or
/// none, is left alone.
pub(crate) struct GuardedDelete {
    pub key: Vec<u8>,
    pub expected: Vec<u8>,
}

/// Everything one planner run needs.
#[derive(Default)]
pub(crate) struct TipChangeSet<'a> {
    pub history: super::super::atom_history::HistoryMode,
    pub scope: Option<&'a str>,
    pub tips: Vec<TipChange>,
    /// Bodies that this request created. The planner writes a body only for an
    /// atom that gains its first reference in this scope.
    pub bodies: &'a [Atom],
    /// A scope to read a missing body from, for a scope that copies tips from
    /// another scope. `Some(None)` names the personal scope.
    pub body_source: Option<Option<&'a str>>,
    /// Conflict records written by this run. The planner adds one reference per
    /// distinct atom for each record that is new.
    pub records: Vec<RecordRefs>,
    /// Conflict records this run releases (a purge of their key, or the end of
    /// their retention window). See `records.rs`.
    pub releases: Vec<ReleaseRequest>,
    /// Other rows of the same batch: molecule headers, order log, record rows.
    pub rows: Vec<(String, Value)>,
    /// Other keys that the same batch deletes.
    pub deletes: Vec<Vec<u8>>,
    /// Keys that the batch deletes only if they still hold the bytes read
    /// before (the retirement of a queue bucket).
    pub guarded_deletes: Vec<GuardedDelete>,
}

/// Where the planner finds a body for an atom that is new in the scope.
pub(crate) struct BodySources<'a> {
    pub bodies: &'a [Atom],
    pub source: Option<Option<&'a str>>,
}

/// The keep-small debit of one removed tip: its storage key, its stored row
/// length, and its atom's size when the commit knew it.
pub(crate) type TipDebit = (
    String,
    u64,
    Option<crate::db_operations::MoleculeTipCounterSource>,
);

/// The tip a changed key held before the run, read inside the write gate.
#[derive(Debug, Clone)]
pub(crate) struct PreviousTip {
    /// Length of the stored tip record, as the keep-small meters count rows.
    pub row_bytes: u64,
    pub atom_uuid: String,
}

/// What a planner run did, for reports and meters.
#[derive(Debug, Default)]
pub(crate) struct TipCommit {
    /// Bodies that this run wrote, for the keep-small meters.
    pub created: Vec<Atom>,
    /// Atoms whose count reached zero. The run deleted their bodies.
    pub deleted: Vec<DeletedAtom>,
    /// History rows of conflict records this run released.
    pub records_released: u64,
    /// Counter faults: an underflow, an absent counter, or a dangling removal.
    pub counter_events: u64,
    /// Old tips of changed keys, for the keep-small replacement debit.
    pub previous: HashMap<String, PreviousTip>,
    /// The size of each atom whose count changed, from its counter or from
    /// the body this run created. An atom whose counter has no size is absent.
    pub atom_sizes: HashMap<String, AtomSize>,
    /// Each molecule header row the run wrote, with its old and new length,
    /// read under the scoped key.
    pub headers: Vec<super::molecule::HeaderChange>,
    /// Guarded deletes that found the expected bytes and deleted the key.
    pub guarded_deleted: u64,
}

impl TipCommit {
    /// Fold the result of a later planner call of the same request into this one.
    pub(crate) fn absorb(&mut self, other: Self) {
        self.created.extend(other.created);
        self.deleted.extend(other.deleted);
        self.records_released += other.records_released;
        self.counter_events += other.counter_events;
        self.previous.extend(other.previous);
        self.atom_sizes.extend(other.atom_sizes);
        self.headers.extend(other.headers);
        self.guarded_deleted += other.guarded_deleted;
    }
}
