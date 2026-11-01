//! The atom counter: the only authority for whether an atom body may go.
//!
//! Every atom body in a storage scope has one counter record in the same hash
//! group. The counter holds the number of tip records and conflict records in
//! that scope that name the atom. A batch that lowers the count to exactly zero
//! deletes the body and the counter in that same batch. No other structure
//! authorizes a delete and no other structure blocks one.
//!
//! The invariants, for every scope and every atom `A`:
//!
//! - I1: `refs(A)` is the number of tip records plus conflict records that name `A`.
//! - I2: the body exists if and only if the counter exists. A counter never
//!   holds zero, except a `suspect` counter, which keeps its body.
//! - I3: a batch lowers `refs(A)` by `k` only if the same batch removes `k`
//!   references to `A`, each read from the store inside the same write gate hold.
//! - I4: the stored count is never lower than the true number of references.
//!
//! Only this module writes `mk:` tip records, conflict record keys, atom body
//! records, and atom counter records. The
//! planner here reads the old state from the store inside the native write
//! request scope and writes one batch. A crash keeps whole batches only, so each
//! batch moves the store from one equal state to another.
//! `scripts/lints/lint-atom-counter-writers.py` guards this boundary at the
//! source level.
//!
//! Design: `docs/design-lastdb-counter-lifecycle.md`. The contract in short:
//! `docs/lastdb-atom-counter.md`.

mod build;
mod commit;
mod commit_types;
mod file_dependencies;
mod guarded;
mod plan;
mod read;
mod records;
mod recount;
mod refs;
mod report;
pub(crate) mod restore_validation;
mod scan;
mod verify;

pub(crate) use commit::{GuardedDelete, RecordRefs, ReleaseRequest, TipChangeSet, TipCommit};
pub use read::AtomCounterView;
pub use recount::{Change, RecountOptions, RecountReport};
pub(crate) use refs::event_atoms;
pub use report::AtomCounterReport;
pub(crate) use report::CounterEvents;
pub(crate) use verify::VerifyState;
pub use verify::{VerifyReport, VerifyRequest, VerifySummary};

use serde::{Deserialize, Serialize};

/// Format version of a counter record.
pub const ATOM_COUNTER_VERSION: u8 = 1;

/// One counter record, at `atom:{uuid}\0c` in the `atoms` collection.
///
/// The record stays small: the count, the blob references of the body (so a
/// delete needs no body read), the body size, and the `suspect` flag.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AtomCounter {
    #[serde(rename = "v")]
    pub version: u8,
    pub refs: u64,
    /// Blob references of the atom body, set when the body is created.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blobs: Vec<String>,
    /// Size of the body, set when the body is created. The planner reads the
    /// counter of every atom a tip leaves, so the keep-small meters learn the
    /// old size there and keep no per-key state. `None` when a recount wrote
    /// the counter or the body names a blob without its size.
    #[serde(rename = "sz", default, skip_serializing_if = "Option::is_none")]
    pub size: Option<AtomSize>,
    #[serde(rename = "src", default, skip_serializing_if = "Option::is_none")]
    pub source_schema: Option<String>,
    /// A planner found a count that disagrees with a removal. The body stays,
    /// whatever the count says, until an operator repairs the counter.
    #[serde(default, skip_serializing_if = "is_false")]
    pub suspect: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl AtomCounter {
    /// A new counter for a body that this batch creates.
    #[must_use]
    pub fn new(refs: u64, blobs: Vec<String>) -> Self {
        Self {
            version: ATOM_COUNTER_VERSION,
            refs,
            blobs,
            size: None,
            source_schema: None,
            suspect: false,
        }
    }
}

/// Logical bytes of one atom body and the logical bytes of the blobs it names,
/// as the keep-small meters count them. Stored as a two-number array.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AtomSize(pub u64, pub u64);

impl AtomSize {
    /// The meter size of `atom`, or `None` when it names a blob without the
    /// blob's size (the meters then fail closed).
    #[must_use]
    pub fn of(atom: &crate::atom::Atom) -> Option<Self> {
        let blob =
            crate::atom::file_pointer::blob_logical_bytes_of_atom(atom.content(), atom.metadata())?;
        let logical = serde_json::to_vec(atom).map_or(0, |bytes| bytes.len() as u64);
        Some(Self(logical, blob))
    }
}

impl super::AtomStore {
    /// Check the scope that a verb names. The only scopes a verb may name are the
    /// personal scope (no value) and a database scope: exactly 64 lowercase hex
    /// characters. A mistyped scope reads an empty range and looks like a clean
    /// scope, so the verbs that take a scope check it before they read. The check is
    /// narrower than the key validation of the storage layer on purpose: a verb
    /// never needs a `cloud:` or `from:` scope or a reserved name.
    pub fn validate_scope(scope: Option<&str>) -> Result<(), crate::schema::SchemaError> {
        let Some(scope) = scope else {
            return Ok(());
        };
        let lower_hex = scope.len() == 64
            && scope
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'));
        if lower_hex {
            return Ok(());
        }
        let shown: String = scope.chars().take(80).collect();
        Err(crate::schema::SchemaError::InvalidData(format!(
            "invalid scope {shown:?}: a scope is the personal scope (leave the value out) \
             or a database scope of exactly 64 lowercase hex characters"
        )))
    }
}
