//! The pure part of the molecule counters: one decision per molecule.
//!
//! The input is the counters in the stored header and the net change that the
//! batch makes. The output is the counters the batch writes. This file reads
//! nothing and writes nothing, so a reviewer can check the arithmetic without
//! the store code.
//!
//! `slots` counts the tip records (`mk:{M}:...`) of one molecule in one storage
//! scope. `records` counts the conflict records that name the molecule. Both sit
//! in the molecule header `mh:{M}`, which every tip write already rewrites. The
//! third counter, `bindings`, lives beside the binding records (see
//! `molecule_bindings.rs`), because a bind can come before the first tip and
//! the header's presence means "the molecule lives at this prefix".

/// The counters of one molecule in one scope, as the header stores them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MoleculeCounts {
    /// Tip records of the molecule in this scope.
    pub slots: u64,
    /// Conflict records that name the molecule in this scope.
    pub records: u64,
    /// A removal asked for more than the counter held, or the header had no
    /// counters. The molecule never counts as droppable while this is set.
    pub suspect: bool,
}

/// What the header says before the batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Before {
    /// True when a header record exists.
    pub header_present: bool,
    /// The stored counters, or `None` when the header carries none.
    pub counts: Option<MoleculeCounts>,
}

/// The net change the batch makes to one molecule.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Change {
    pub slots: i64,
    pub records: i64,
}

impl Change {
    pub(super) fn is_zero(&self) -> bool {
        self.slots == 0 && self.records == 0
    }
}

/// Why the planner set `suspect`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoleculeFault {
    /// A removal asked for more slots or records than the counter held.
    Underflow,
    /// The header existed and carried no counters.
    CountersUnset,
    /// A count would pass `u64::MAX`.
    Overflow,
}

/// The counters to write, and the fault the decision found, if any.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct After {
    pub counts: MoleculeCounts,
    pub fault: Option<MoleculeFault>,
}

/// Decide the counters for one molecule. The planner never clamps a count and
/// calls the result exact: a count that disagrees with a removal is clamped to
/// zero, `suspect` is set, and the fault is returned for the report.
pub(super) fn plan_molecule(before: &Before, change: &Change) -> After {
    let mut fault = None;
    let mut counts = match before.counts {
        Some(counts) => counts,
        None if before.header_present => {
            // A header that predates the counters: the numbers are unknown.
            fault = Some(MoleculeFault::CountersUnset);
            MoleculeCounts {
                suspect: true,
                ..MoleculeCounts::default()
            }
        }
        None => MoleculeCounts::default(),
    };
    for (stored, delta) in [
        (&mut counts.slots, change.slots),
        (&mut counts.records, change.records),
    ] {
        if let Err(found) = apply(stored, delta) {
            fault = Some(found);
            counts.suspect = true;
        }
    }
    After { counts, fault }
}

fn apply(stored: &mut u64, delta: i64) -> Result<(), MoleculeFault> {
    let magnitude = delta.unsigned_abs();
    if delta >= 0 {
        match stored.checked_add(magnitude) {
            Some(sum) => *stored = sum,
            None => {
                *stored = u64::MAX;
                return Err(MoleculeFault::Overflow);
            }
        }
    } else if *stored >= magnitude {
        *stored -= magnitude;
    } else {
        *stored = 0;
        return Err(MoleculeFault::Underflow);
    }
    Ok(())
}
