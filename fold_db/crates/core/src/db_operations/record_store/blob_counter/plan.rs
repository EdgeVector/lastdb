//! The pure part of the blob planner: one decision per blob.
//!
//! The input is the stored counter, the net change of references, and the time.
//! The output says what the batch writes: the new counter, the queue entry to
//! delete and the queue entry to add. This file reads and writes nothing, so a
//! reviewer can check the arithmetic against the contract without the store code.

use super::BlobCounter;

/// What the batch does to one blob.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum BlobPlan {
    /// Nothing to write.
    Unchanged,
    /// A removal names a blob with no counter. The blob record, if any, stays.
    /// The planner reports it and writes nothing.
    Dangling,
    Write(BlobWrite),
}

/// One counter write and the queue entries that go with it.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct BlobWrite {
    pub counter: BlobCounter,
    /// Queue entry to delete: the old `zero_since`, when it changes.
    pub unqueue: Option<u64>,
    /// Queue entry to put: the new `zero_since`, when it changes.
    pub enqueue: Option<u64>,
    pub event: Option<BlobEvent>,
}

/// What the report counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BlobEvent {
    /// A reference made the counter of a blob that had none.
    Created,
    /// An upload made or renewed a counter at zero. The dwell starts.
    Registered,
    /// A reference arrived during the dwell. The deletion is cancelled.
    Cancelled,
    /// The count reached zero. The dwell starts.
    ZeroReached,
    /// A removal asked for more references than the counter held.
    Underflow,
    /// A removal named a blob that has no counter.
    Dangling,
    /// A reference made a counter for a blob whose row is gone: the atom names
    /// a blob that is not in the store.
    LateReference,
    /// The counter record did not decode. The counter is now `suspect`.
    Unreadable,
}

/// Why a plan cannot proceed.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum PlanError {
    /// The count would pass `u64::MAX`.
    Overflow,
}

/// Decide the change for one blob. `delta` is the number of references the
/// batch adds minus the number it removes.
pub(super) fn plan_delta(
    blob: &str,
    before: Option<&BlobCounter>,
    delta: i64,
    now: u64,
) -> Result<BlobPlan, PlanError> {
    if delta == 0 {
        return Ok(BlobPlan::Unchanged);
    }
    let amount = delta.unsigned_abs();
    Ok(match (before, delta > 0) {
        (None, true) => finish(
            None,
            BlobCounter::new(blob, amount, None),
            Some(BlobEvent::Created),
        ),
        (None, false) => BlobPlan::Dangling,
        (Some(counter), true) => raise(counter, amount)?,
        (Some(counter), false) => lower(counter, amount, now),
    })
}

/// An upload of the blob row. A counter at zero starts or renews its dwell. A
/// blob that has references, or a `suspect` counter, needs nothing.
pub(super) fn plan_upload(blob: &str, before: Option<&BlobCounter>, now: u64) -> BlobPlan {
    match before {
        None => finish(
            None,
            BlobCounter::new(blob, 0, Some(now)),
            Some(BlobEvent::Registered),
        ),
        Some(counter) if counter.refs > 0 || counter.suspect => BlobPlan::Unchanged,
        Some(counter) => finish(
            Some(counter),
            BlobCounter {
                zero_since: Some(now),
                ..counter.clone()
            },
            Some(BlobEvent::Registered),
        ),
    }
}

/// The counter record does not decode, so its state is unknown. Replace it with a
/// `suspect` counter that holds the references this batch adds. A `suspect`
/// counter never authorizes a delete, and the verifier and the recount report
/// it. The old queue entry, if any, is left: the dwell pass drops it as stale.
pub(super) fn plan_unreadable(blob: &str, delta: i64) -> BlobPlan {
    let mut counter = BlobCounter::new(blob, delta.max(0).unsigned_abs(), None);
    counter.suspect = true;
    BlobPlan::Write(BlobWrite {
        counter,
        unqueue: None,
        enqueue: None,
        event: Some(BlobEvent::Unreadable),
    })
}

fn raise(counter: &BlobCounter, add: u64) -> Result<BlobPlan, PlanError> {
    let refs = counter.refs.checked_add(add).ok_or(PlanError::Overflow)?;
    let cancelled = counter.zero_since.is_some();
    Ok(finish(
        Some(counter),
        BlobCounter {
            refs,
            zero_since: None,
            ..counter.clone()
        },
        cancelled.then_some(BlobEvent::Cancelled),
    ))
}

fn lower(counter: &BlobCounter, remove: u64, now: u64) -> BlobPlan {
    if counter.refs < remove {
        // The counter says fewer references than the batch removes. Do not
        // clamp to zero and start a dwell. Mark the counter: it never queues.
        return finish(
            Some(counter),
            BlobCounter {
                refs: 0,
                zero_since: None,
                suspect: true,
                ..counter.clone()
            },
            Some(BlobEvent::Underflow),
        );
    }
    let refs = counter.refs - remove;
    if refs == 0 && !counter.suspect {
        return finish(
            Some(counter),
            BlobCounter {
                refs,
                zero_since: Some(now),
                ..counter.clone()
            },
            Some(BlobEvent::ZeroReached),
        );
    }
    finish(
        Some(counter),
        BlobCounter {
            refs,
            zero_since: None,
            ..counter.clone()
        },
        None,
    )
}

/// The queue entries follow `zero_since`: the old one goes, the new one comes.
fn finish(
    before: Option<&BlobCounter>,
    counter: BlobCounter,
    event: Option<BlobEvent>,
) -> BlobPlan {
    let old = before.and_then(|before| before.zero_since);
    let (unqueue, enqueue) = if old == counter.zero_since {
        (None, None)
    } else {
        (old, counter.zero_since)
    };
    BlobPlan::Write(BlobWrite {
        counter,
        unqueue,
        enqueue,
        event,
    })
}
