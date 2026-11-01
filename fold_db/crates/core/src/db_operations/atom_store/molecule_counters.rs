//! The read verb of the molecule counters.
//!
//! A molecule keeps three counters in one scope: `slots` and `records` in its
//! header, and `bindings` beside its binding records. This file joins them in
//! one view. A molecule may go only when all three are zero and none is
//! `suspect`. The drain that removes a dead molecule is a later step. This view
//! is its input and its completion test.

use super::counter::{counts_of_header, MoleculeCounts};
use super::molecule_bindings::MoleculeBindings;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The three counters of one molecule in one scope, for operators.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoleculeCountersView {
    pub molecule: String,
    /// A header record exists. The molecule lives at this prefix.
    pub header_present: bool,
    /// Tip records of the molecule in this scope.
    pub slots: u64,
    /// Conflict records that name the molecule in this scope.
    pub records: u64,
    pub bindings: u64,
    /// A counter disagreed with a change, or the header carries no counters.
    pub suspect: bool,
    /// `live`, `droppable`, `suspect`, `mismatch` or `absent`. `mismatch` is a
    /// molecule whose counters say it is dead and that still has a binding
    /// record with no counter: a fault, never a drain.
    pub state: String,
    /// A binding record exists. `None` when the read did not look for one
    /// (only a molecule whose counters are all zero is checked).
    #[serde(default)]
    pub binding_records: Option<bool>,
}

impl MoleculeCountersView {
    /// All three counters are zero, none is `suspect`, a header exists, and no
    /// binding record exists where the read looked for one. The molecule holds
    /// nothing and nothing names it.
    #[must_use]
    pub fn droppable(&self) -> bool {
        self.state == "droppable"
    }
}

/// Join a header and a bindings record into one view. Pure.
#[must_use]
pub fn molecule_view(
    molecule: &str,
    header: Option<&Value>,
    bindings: Option<&MoleculeBindings>,
) -> MoleculeCountersView {
    let counts = header.and_then(counts_of_header);
    let unset = header.is_some() && counts.is_none();
    let MoleculeCounts {
        slots,
        records,
        suspect,
    } = counts.unwrap_or_default();
    let (bound, bound_suspect) =
        bindings.map_or((0, false), |record| (record.bindings, record.suspect));
    let suspect = suspect || bound_suspect || unset;
    let state = if suspect {
        "suspect"
    } else if slots + records + bound > 0 {
        "live"
    } else if header.is_some() {
        "droppable"
    } else {
        "absent"
    };
    MoleculeCountersView {
        molecule: molecule.to_string(),
        header_present: header.is_some(),
        slots,
        records,
        bindings: bound,
        suspect,
        state: state.to_string(),
        binding_records: None,
    }
}
