//! The pure part of the verifier: what a read says about the counters.
//!
//! Nothing here reads or writes the store. A reviewer can check each rule
//! against the invariants of the parent module without the store code.

use super::report::Kind;
use crate::db_operations::atom_store::counter::AtomCounter;

/// What a read found for the counter record of an atom.
pub(super) enum CounterRead {
    Absent,
    /// The record exists and does not decode.
    Unreadable,
    Present(AtomCounter),
}

impl CounterRead {
    pub(super) fn parse(bytes: Option<&[u8]>) -> Self {
        match bytes {
            None => Self::Absent,
            Some(bytes) => serde_json::from_slice(bytes).map_or(Self::Unreadable, Self::Present),
        }
    }
}

/// One fault, with a line for the sample list.
pub(super) struct Finding {
    pub kind: Kind,
    pub detail: String,
}

fn finding(kind: Kind, detail: String) -> Finding {
    Finding { kind, detail }
}

/// What holds the references the page shows.
#[derive(Clone, Copy)]
pub(super) enum Holder {
    Tip,
    Record,
}

/// Check an atom that `holders` tips (or records) of one page name. The page,
/// the counter and the body were read at one root, so no writer can explain a
/// fault found here: I3 and I4 hold at every root.
///
/// - The body must exist.
/// - The counter must not be below the holders of the page. Holders in other
///   pages only raise the true count, so this check has no false positive.
///   A missing counter with an existing body is reported by the pairing check
///   of the atoms phase, once.
pub(super) fn judge_referenced(
    holder: Holder,
    holders: u32,
    counter: &CounterRead,
    body: bool,
) -> Vec<Finding> {
    let mut out = Vec::new();
    if !body {
        let (kind, what) = match holder {
            Holder::Tip => (Kind::TipWithoutBody, "tips"),
            Holder::Record => (Kind::RecordWithoutBody, "records"),
        };
        out.push(finding(
            kind,
            format!("{holders} {what} name an atom with no body"),
        ));
    }
    if let CounterRead::Present(counter) = counter {
        if !counter.suspect && counter.refs < u64::from(holders) {
            out.push(finding(
                Kind::CounterBelowTips,
                format!(
                    "counter holds {} and the page shows {holders} holders",
                    counter.refs
                ),
            ));
        }
    }
    out
}

/// Check the body and the counter of one atom, read at one root.
pub(super) fn judge_pair(body: bool, counter: &CounterRead) -> Vec<Finding> {
    let mut out = Vec::new();
    match (body, counter) {
        (true, CounterRead::Absent) => {
            out.push(finding(
                Kind::BodyWithoutCounter,
                "no counter record".into(),
            ));
        }
        (false, CounterRead::Present(_) | CounterRead::Unreadable) => {
            out.push(finding(Kind::CounterWithoutBody, "no body record".into()));
        }
        _ => {}
    }
    match counter {
        CounterRead::Unreadable => {
            out.push(finding(
                Kind::UnreadableCounter,
                "counter does not decode".into(),
            ));
        }
        CounterRead::Present(counter) if counter.suspect => out.push(finding(
            Kind::SuspectCounter,
            format!("counter holds {} and is marked suspect", counter.refs),
        )),
        CounterRead::Present(counter) if counter.refs == 0 => {
            out.push(finding(Kind::ZeroCounter, "counter holds zero".into()));
        }
        _ => {}
    }
    out
}

/// What the whole pass saw for one atom.
pub(super) struct Seen {
    /// Tips and records that name the atom, over all pages.
    pub holders: u64,
    /// A writer changed a tip or a record of this atom while the pass read.
    pub touched: bool,
    /// A page already reported the counter as below the holders.
    pub reported_below: bool,
}

/// Compare a counter with the totals of the pass.
///
/// The pages were read at different roots. For an atom that no writer touched
/// during the pass, the holders did not change, so the totals are exact and a
/// difference is a fault. For a touched atom a difference can be a write between
/// two pages, so it is a candidate only.
pub(super) fn judge_total(counter: &AtomCounter, seen: &Seen) -> Option<Finding> {
    if counter.suspect || counter.refs == seen.holders {
        return None;
    }
    let (kind, word) = if counter.refs > seen.holders {
        (Kind::CounterAboveTips, "above")
    } else if seen.reported_below {
        return None;
    } else {
        (Kind::CounterBelowTips, "below")
    };
    let kind = if seen.touched {
        Kind::UnconfirmedCountMismatch
    } else {
        kind
    };
    Some(finding(
        kind,
        format!(
            "counter holds {}, {word} the {} holders of the pass",
            counter.refs, seen.holders
        ),
    ))
}
