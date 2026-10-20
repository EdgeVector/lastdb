//! The pure part of the molecule checks: what a read says about the counters.
//!
//! Nothing here reads or writes the store. A reviewer can check each rule
//! against the contract of the molecule counters without the store code.
//!
//! `slots` and `records` live in the header and change in the batch of the tip
//! change, so at one root a header equals the tips and conflict records of its
//! molecule. The pages of a run are read at different roots, so a molecule that a
//! writer changed during the pass is a candidate, not a fault, as for an atom.
//! The `bindings` counter and the binding records change in one batch, and one
//! page reads both at one root, so a difference there is always a fault.

use super::check::Finding;
use super::report::Kind;
use crate::db_operations::atom_store::{MoleculeBindings, MoleculeCounts};

/// What the whole pass saw for one molecule.
pub(super) struct HeaderSeen {
    /// Tip records of the molecule over all pages.
    pub tips: u64,
    /// Counted conflict records of the molecule over all pages.
    pub records: u64,
    /// A writer changed a tip or a record of this molecule while the pass read.
    pub touched: bool,
    /// Every molecule fit in the totals. Without it no total is exact.
    pub complete: bool,
}

fn finding(kind: Kind, detail: String) -> Finding {
    Finding { kind, detail }
}

/// Compare one counter with the total of the pass.
fn compare(
    what: &str,
    stored: u64,
    seen: u64,
    touched: bool,
    (above, below): (Kind, Kind),
) -> Option<Finding> {
    if stored == seen {
        return None;
    }
    let kind = if touched {
        Kind::UnconfirmedCountMismatch
    } else if stored > seen {
        above
    } else {
        below
    };
    Some(finding(
        kind,
        format!("header {what} holds {stored} and the pass counted {seen}"),
    ))
}

/// Judge one header. `readable` is false when the row does not decode.
pub(super) fn judge_header(
    counts: Option<&MoleculeCounts>,
    readable: bool,
    seen: &HeaderSeen,
) -> Vec<Finding> {
    if !readable {
        return vec![finding(
            Kind::UnreadableHeader,
            "header does not decode".into(),
        )];
    }
    let Some(counts) = counts else {
        return vec![finding(
            Kind::MoleculeCountersUnset,
            "header carries no counters".into(),
        )];
    };
    if counts.suspect {
        return vec![finding(
            Kind::SuspectMolecule,
            format!(
                "header holds slots={} records={} and is marked suspect",
                counts.slots, counts.records
            ),
        )];
    }
    if !seen.complete {
        return Vec::new();
    }
    [
        compare(
            "slots",
            counts.slots,
            seen.tips,
            seen.touched,
            (Kind::SlotsAboveTips, Kind::SlotsBelowTips),
        ),
        compare(
            "records",
            counts.records,
            seen.records,
            seen.touched,
            (Kind::RecordsAboveRecords, Kind::RecordsBelowRecords),
        ),
    ]
    .into_iter()
    .flatten()
    .collect()
}

/// Judge one `bindings` counter against the number of binding records read at
/// the same root. `record` is `None` when the row does not decode.
pub(super) fn judge_bindings(
    record: Option<&MoleculeBindings>,
    binding_records: u64,
) -> Vec<Finding> {
    let Some(record) = record else {
        return vec![finding(
            Kind::UnreadableBindings,
            "bindings record does not decode".into(),
        )];
    };
    if record.suspect {
        return vec![finding(
            Kind::SuspectBindings,
            format!(
                "bindings holds {} and is marked suspect; {binding_records} binding records",
                record.bindings
            ),
        )];
    }
    let mut out = Vec::new();
    if record.bindings == 0 {
        out.push(finding(
            Kind::ZeroBindingsCounter,
            "bindings record holds zero".into(),
        ));
    }
    if record.bindings != binding_records {
        out.push(finding(
            Kind::BindingsMismatch,
            format!(
                "bindings holds {} and {binding_records} binding records exist",
                record.bindings
            ),
        ));
    }
    out
}
