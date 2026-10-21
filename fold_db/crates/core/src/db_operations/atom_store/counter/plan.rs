//! The pure part of the planner: one decision per atom.
//!
//! The input is the stored counter, the net change of references, and whether
//! this batch brings a body. The output says what the batch writes. This file
//! reads nothing and writes nothing, so a reviewer can check the arithmetic
//! against the invariants of the parent module without the store code.

use super::{AtomCounter, AtomSize};

/// What the stored state shows before the batch.
pub(super) struct AtomBefore {
    pub counter: Option<AtomCounter>,
    /// True when no counter exists but a body record does. The caller reads
    /// this only for an atom without a counter.
    pub body_without_counter: bool,
}

/// What the batch does to one atom.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum AtomPlan {
    /// The net change is zero. Nothing to write.
    Unchanged,
    /// The counter changes and the body stays as it is.
    SetCounter(AtomCounter),
    /// The atom is new in this scope. The batch writes the body and the counter.
    Create(AtomCounter),
    /// The count reached zero. The batch deletes the body, the counter, and the
    /// blob references that the counter lists.
    Delete {
        blobs: Vec<String>,
        size: Option<AtomSize>,
    },
    /// The counter disagrees with the removal. The batch writes the counter
    /// with `suspect` and keeps the body.
    Suspect(AtomCounter, SuspectKind),
    /// A removal names an atom that has neither counter nor body. Nothing to
    /// write. The planner reports it.
    Dangling,
}

/// Why a counter became `suspect`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SuspectKind {
    /// A removal asked for more references than the counter holds.
    Underflow,
    /// A body exists and its counter does not.
    CounterAbsent,
}

/// A body the batch brings for an atom, with its blob references.
pub(super) struct BodyOffer<'a> {
    pub blobs: &'a [String],
    pub size: Option<AtomSize>,
}

/// Why a plan cannot proceed.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum PlanError {
    /// A tip names an atom that has no counter, no body to write, and no body
    /// in the store. The write must fail: no counter can describe it.
    NoBody,
    /// The count would pass `u64::MAX`.
    Overflow,
}

/// Decide the change for one atom. `delta` is the number of references the
/// batch adds minus the number it removes.
pub(super) fn plan_atom(
    before: &AtomBefore,
    delta: i64,
    body: Option<&BodyOffer<'_>>,
) -> Result<AtomPlan, PlanError> {
    if delta == 0 {
        return Ok(AtomPlan::Unchanged);
    }
    match (&before.counter, delta > 0) {
        (None, true) => plan_new(before, delta.unsigned_abs(), body),
        (None, false) => Ok(plan_removal_without_counter(before)),
        (Some(counter), true) => raise(counter, delta.unsigned_abs()),
        (Some(counter), false) => Ok(lower(counter, delta.unsigned_abs())),
    }
}

fn plan_new(
    before: &AtomBefore,
    add: u64,
    body: Option<&BodyOffer<'_>>,
) -> Result<AtomPlan, PlanError> {
    if before.body_without_counter {
        // A body with no counter breaks the invariant before this batch. This
        // test comes first: an offered body must not turn the batch into a
        // fresh create, because the holders that the lost counter counted are
        // unknown. Keep the stored body, count only the new references, and
        // mark the counter, so no later removal can reach zero and delete.
        let blobs = body.map_or_else(Vec::new, |body| body.blobs.to_vec());
        let mut counter = AtomCounter::new(add, blobs);
        counter.size = body.and_then(|body| body.size);
        counter.suspect = true;
        return Ok(AtomPlan::Suspect(counter, SuspectKind::CounterAbsent));
    }
    if let Some(body) = body {
        let mut counter = AtomCounter::new(add, body.blobs.to_vec());
        counter.size = body.size;
        return Ok(AtomPlan::Create(counter));
    }
    Err(PlanError::NoBody)
}

fn plan_removal_without_counter(before: &AtomBefore) -> AtomPlan {
    if before.body_without_counter {
        let mut counter = AtomCounter::new(0, Vec::new());
        counter.suspect = true;
        return AtomPlan::Suspect(counter, SuspectKind::CounterAbsent);
    }
    AtomPlan::Dangling
}

fn raise(counter: &AtomCounter, add: u64) -> Result<AtomPlan, PlanError> {
    let refs = counter.refs.checked_add(add).ok_or(PlanError::Overflow)?;
    Ok(AtomPlan::SetCounter(AtomCounter {
        refs,
        ..counter.clone()
    }))
}

fn lower(counter: &AtomCounter, remove: u64) -> AtomPlan {
    if counter.refs < remove {
        // The counter says fewer references than the batch removes. Do not
        // clamp to zero and delete. Keep the body and mark the counter.
        return AtomPlan::Suspect(
            AtomCounter {
                refs: 0,
                suspect: true,
                ..counter.clone()
            },
            SuspectKind::Underflow,
        );
    }
    let refs = counter.refs - remove;
    if refs == 0 && !counter.suspect {
        return AtomPlan::Delete {
            blobs: counter.blobs.clone(),
            size: counter.size,
        };
    }
    AtomPlan::SetCounter(AtomCounter {
        refs,
        ..counter.clone()
    })
}
