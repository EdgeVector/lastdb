//! The clock of a time queue.

/// A forward excursion of the wall clock beyond this is ignored.
const CLOCK_TOLERANCE_NANOS: u64 = 24 * 60 * 60 * 1_000_000_000;

fn wall_clock_nanos() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX)).unwrap_or(0)
}

/// The wall clock when the store was made, and the monotonic clock then.
/// They give an estimate of the time that a clock excursion cannot move.
#[derive(Debug)]
pub(crate) struct QueueClock {
    anchor_wall_nanos: u64,
    anchor: std::time::Instant,
}

impl Default for QueueClock {
    fn default() -> Self {
        Self {
            anchor_wall_nanos: wall_clock_nanos(),
            anchor: std::time::Instant::now(),
        }
    }
}

impl QueueClock {
    /// The wall clock, but never more than a day ahead of the time that the
    /// monotonic clock gives since the store was made. The written expiry and
    /// the due test of a reader both read it. A forward excursion of the wall
    /// clock longer than a day cannot give an entry a far-future expiry, and
    /// cannot make a reader release an entry at once. A smaller forward jump
    /// moves the clock, and a reader then releases entries early by at most
    /// the size of the jump.
    pub(crate) fn now_nanos(&self) -> u64 {
        let elapsed = u64::try_from(self.anchor.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let estimate = self.anchor_wall_nanos.saturating_add(elapsed);
        wall_clock_nanos().min(estimate.saturating_add(CLOCK_TOLERANCE_NANOS))
    }
}
