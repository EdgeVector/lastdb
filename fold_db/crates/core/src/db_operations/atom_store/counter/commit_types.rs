use crate::atom::Atom;
use crate::db_operations::atom_store::PerKeyRecord;
use serde_json::Value;

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
    pub history_slot: Option<String>,
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

/// What a planner run did, for reports and meters.
#[derive(Debug, Default)]
pub(crate) struct TipCommit {
    /// Atoms whose count reached zero. The run deleted their bodies.
    pub deleted: u64,
    /// History rows of conflict records this run released.
    pub records_released: u64,
    /// Counter faults: an underflow, an absent counter, or a dangling removal.
    pub counter_events: u64,
    /// Changed keys that held a tip before the run.
    pub old_tips: u64,
    /// Guarded deletes that found the expected bytes and deleted the key.
    pub guarded_deleted: u64,
}
