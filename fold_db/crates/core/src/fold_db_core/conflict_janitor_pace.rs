//! The pacing of the retention janitor: how long it rests after a step.

use crate::db_operations::atom_store::{StepOutcome, WriterWatch};
use std::time::Duration;

/// The shortest rest between two pages.
const MIN_PAUSE: Duration = Duration::from_millis(25);
/// The longest rest between two pages.
const MAX_PAUSE: Duration = Duration::from_secs(30);
/// The longest wait after a wake that failed.
const BACKOFF_MAX: Duration = Duration::from_secs(600);
/// The shortest base of the wait after a wake that failed.
const BACKOFF_BASE_MIN: Duration = Duration::from_secs(1);

/// The rest after a step that kept writers waiting for `blocked`. With a duty
/// share of `d` percent the rest is `blocked * (100 - d) / d`, so writers wait for
/// the janitor `d` percent of the time. The measure is the time the step keeps
/// writers waiting (`storage::disk_pack::transaction::blocked`), not its wall time:
/// the page reads before the gate block nobody. It is also not the hold of the
/// gate alone when a batch requires a sealed durability fence. A normal background
/// batch shares the bounded collection window and measures its gate and shared flush holds.
fn pause_after(blocked: Duration, duty_pct: u64) -> Duration {
    let blocked_us = u64::try_from(blocked.as_micros()).unwrap_or(u64::MAX);
    Duration::from_micros(blocked_us.saturating_mul(100 - duty_pct) / duty_pct)
        .clamp(MIN_PAUSE, MAX_PAUSE)
}

/// The wait before the next wake after `failures` wakes in a row that failed.
pub(super) fn backoff_after(failures: u64, interval: Duration) -> Duration {
    let base = interval.max(BACKOFF_BASE_MIN);
    let shift = u32::try_from(failures.min(10)).unwrap_or(10);
    base.saturating_mul(1 << shift).min(BACKOFF_MAX)
}

/// The rest after a step: the full rest when another writer was there within the
/// quiet period, and the shortest one when the janitor has the node to itself
/// (`WriterWatch`).
pub(super) fn rest_after(
    watch: &mut WriterWatch,
    outcome: &StepOutcome,
    duty_pct: u64,
) -> Duration {
    rest_after_cost(watch, (outcome.blocked, outcome.scopes), duty_pct)
}

/// The same rest for a job that has no `StepOutcome` (the molecule drain): the time
/// the step kept writers waiting and the write scopes it opened. Call it after every
/// step, also after one that wrote nothing, so the scopes of the job are taken off the
/// count of the watch.
pub(super) fn rest_after_cost(
    watch: &mut WriterWatch,
    (blocked, scopes): (Duration, u64),
    duty_pct: u64,
) -> Duration {
    if watch.writer_present(scopes) {
        pause_after(blocked, duty_pct)
    } else {
        MIN_PAUSE
    }
}

/// Sleep `pause`, and end the sleep early when no other writer has opened a write
/// scope within the quiet period (`WriterWatch::rest_may_end`).
pub(super) async fn rest(pause: Duration, watch: &mut WriterWatch) {
    const SLICE: Duration = Duration::from_millis(250);
    let end = std::time::Instant::now() + pause;
    loop {
        let left = end.saturating_duration_since(std::time::Instant::now());
        if left.is_zero() {
            return;
        }
        tokio::time::sleep(left.min(SLICE)).await;
        if left > SLICE && watch.rest_may_end() {
            return;
        }
    }
}
