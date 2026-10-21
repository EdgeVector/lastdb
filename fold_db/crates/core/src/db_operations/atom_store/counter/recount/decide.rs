//! The pure part of the recount: what one atom needs, given its holders.

use crate::db_operations::atom_store::counter::scan::{CounterRead, PairRead};
use crate::db_operations::atom_store::counter::{AtomCounter, AtomSize};

/// Why a counter is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Why {
    Create,
    Raise,
    Lower,
    ClearSuspect,
    Rewrite,
}

impl Why {
    pub(super) fn action(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Raise => "raise",
            Self::Lower => "lower",
            Self::ClearSuspect => "clear_suspect",
            Self::Rewrite => "rewrite",
        }
    }
}

/// What the recount does for one atom.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Decision {
    /// The counter holds the exact count.
    Keep,
    /// Write the counter. `blobs` is `None` when the counter did not give the
    /// blob references of the body: the recount reads them from the body.
    Write {
        refs: u64,
        blobs: Option<Vec<String>>,
        /// The size the counter held. A counter rebuilt from the body measures
        /// the body instead.
        size: Option<AtomSize>,
        why: Why,
        before: Option<u64>,
    },
    /// A counter without a body, and no tip or record names the atom.
    DeleteCounter { before: Option<u64> },
    /// A body that no tip and no record names. Delete the body and the counter.
    /// The blob counters follow in the blob pass of the recount.
    DeleteAtom { before: Option<u64> },
    /// Tips or records name the atom and it has no body.
    LostBody,
    /// A body that nothing names, kept because a row did not decode.
    KeepOrphan,
    /// No body, no counter, no holder: nothing to do.
    Nothing,
}

/// Decide for one atom. `holders` is the number of tips and counted records
/// that name it. `may_delete` is false when a row did not decode.
pub(super) fn decide(pair: &PairRead, holders: u64, may_delete: bool) -> Decision {
    let existing = match &pair.counter {
        CounterRead::Present(counter) => Some(counter),
        _ => None,
    };
    let before = existing.map(|counter| counter.refs);
    match (pair.body, holders) {
        (false, 0) => match pair.counter {
            CounterRead::Absent => Decision::Nothing,
            _ => Decision::DeleteCounter { before },
        },
        (false, _) => Decision::LostBody,
        (true, 0) if !may_delete => Decision::KeepOrphan,
        (true, 0) => Decision::DeleteAtom { before },
        (true, holders) => decide_exact(pair, holders, existing, before),
    }
}

fn decide_exact(
    pair: &PairRead,
    holders: u64,
    existing: Option<&AtomCounter>,
    before: Option<u64>,
) -> Decision {
    let why = match (&pair.counter, existing) {
        (CounterRead::Absent, _) => Why::Create,
        (CounterRead::Unreadable, _) => Why::Rewrite,
        (_, Some(counter)) if counter.refs < holders => Why::Raise,
        (_, Some(counter)) if counter.refs > holders => Why::Lower,
        (_, Some(counter)) if counter.suspect => Why::ClearSuspect,
        _ => return Decision::Keep,
    };
    Decision::Write {
        refs: holders,
        blobs: existing.map(|counter| counter.blobs.clone()),
        size: existing.and_then(|counter| counter.size),
        why,
        before,
    }
}
