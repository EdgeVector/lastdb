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
//! records, and counter records. The planner here reads the old state from the
//! store inside the native write request scope and writes one batch. A crash
//! keeps whole batches only, so each batch moves the store from one equal state
//! to another. `scripts/lints/lint-atom-counter-writers.py` guards this
//! boundary at the source level.
//!
//! Design: `docs/design-lastdb-counter-lifecycle.md`. The contract in short:
//! `docs/lastdb-atom-counter.md`.

mod build;
mod commit;
mod plan;
mod read;
mod records;
mod refs;
mod report;

pub(crate) use commit::{RecordRefs, ReleaseRequest, TipChange, TipChangeSet, TipCommit};
pub use read::AtomCounterView;
pub(crate) use refs::event_atoms;
pub use report::AtomCounterReport;
pub(crate) use report::CounterEvents;

use serde::{Deserialize, Serialize};

/// Format version of a counter record.
pub const ATOM_COUNTER_VERSION: u8 = 1;

/// One counter record, at `atom:{uuid}\0c` in the `atoms` collection.
///
/// The record stays small: the count, the blob references of the body (so a
/// delete needs no body read), and the `suspect` flag.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AtomCounter {
    #[serde(rename = "v")]
    pub version: u8,
    pub refs: u64,
    /// Blob references of the atom body, set when the body is created.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blobs: Vec<String>,
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
            suspect: false,
        }
    }
}
