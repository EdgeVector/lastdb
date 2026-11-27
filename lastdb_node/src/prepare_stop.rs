//! The write hold of a drain before a stop.
//!
//! `POST /api/system/prepare-stop` makes the daemon refuse new writes while an
//! updater uploads what the spool holds. This module keeps the one hold of the
//! process. A write enters through [`admit`] and holds the returned guard until it
//! has committed, so the caller that began the hold can wait for the writes that
//! were already past the gate. The hold ends on [`cancel`], or when its time
//! passes. A refused write leaves nothing behind: the caller retries it.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

/// The `retry_after_ms` in the refusal body.
pub const RETRY_AFTER_MS: u64 = 5000;
/// The longest upload the daemon waits for in one call.
pub const MAX_DEADLINE_MS: u64 = 600_000;
/// The longest hold of one call.
pub const MAX_HOLD_MS: u64 = 3_600_000;

/// The end of the hold in milliseconds since [`anchor`], or 0 when no hold is set.
static HOLD_UNTIL_MS: AtomicU64 = AtomicU64::new(0);
/// Writes that passed the gate and have not finished.
static IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
/// What the last drain said, for a second call that finds the hold set.
static DETAIL: Mutex<String> = Mutex::new(String::new());

fn anchor() -> Instant {
    static ANCHOR: OnceLock<Instant> = OnceLock::new();
    *ANCHOR.get_or_init(Instant::now)
}

/// Milliseconds since the anchor, never 0, so 0 can mean "no hold".
fn now_ms() -> u64 {
    u64::try_from(anchor().elapsed().as_millis()).unwrap_or(u64::MAX - 1) + 1
}

/// True while the hold is set and its time has not passed.
#[must_use]
pub fn holding() -> bool {
    let until = HOLD_UNTIL_MS.load(Ordering::SeqCst);
    until != 0 && now_ms() < until
}

/// A write that passed the gate. Dropping it ends the write.
pub struct WriteGuard(());

impl Drop for WriteGuard {
    fn drop(&mut self) {
        IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The gate of every write. `None` while the hold is set.
///
/// The write counts itself before it looks at the hold, and [`begin`] sets the
/// hold before it looks at the count, so one of the two always sees the other.
pub fn admit() -> Option<WriteGuard> {
    IN_FLIGHT.fetch_add(1, Ordering::SeqCst);
    let guard = WriteGuard(());
    if holding() {
        return None;
    }
    Some(guard)
}

/// Set the hold for `hold` and return whether one was already set. A hold that
/// is already set keeps its end time.
pub fn begin(hold: Duration) -> bool {
    let mut detail = DETAIL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if holding() {
        return true;
    }
    detail.clear();
    let ms = u64::try_from(hold.as_millis()).unwrap_or(u64::MAX / 2);
    HOLD_UNTIL_MS.store(now_ms().saturating_add(ms), Ordering::SeqCst);
    false
}

/// End the hold. Returns whether one was set.
pub fn cancel() -> bool {
    let was = holding();
    HOLD_UNTIL_MS.store(0, Ordering::SeqCst);
    was
}

/// Wait until no write is in flight, or until `limit` passes. Returns the count
/// of writes still in flight.
pub async fn wait_idle(limit: Duration) -> usize {
    let end = Instant::now() + limit;
    loop {
        let n = IN_FLIGHT.load(Ordering::SeqCst);
        if n == 0 || Instant::now() >= end {
            return n;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Remember what the drain said.
pub fn set_detail(text: &str) {
    *DETAIL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = text.to_owned();
}

/// What the last drain said.
#[must_use]
pub fn detail() -> String {
    DETAIL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}
