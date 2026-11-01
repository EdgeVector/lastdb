//! The clock of the blob counters.
//!
//! A dwell starts at `zero_since`, which a writer stamps, and ends when the dwell
//! pass reads that time plus the dwell as past. Both sides read this clock. A
//! wall clock that jumps forward would end every dwell at once and delete blobs
//! that a late write still names. So the clock is the wall clock, but never more
//! than a tolerance ahead of the time that the monotonic clock gives since the
//! store was made. The tolerance is a quarter of the dwell (15 minutes for the
//! default dwell), at least 30 s and at most one day. A forward excursion of the
//! wall clock beyond it cannot end a dwell early: the clock then advances with
//! the monotonic clock. The one day cap is the rule of the conflict janitor; for
//! a dwell of one hour a cap of one day would protect nothing, so the tolerance
//! follows the dwell.
//!
//! What stays: a forward jump below the tolerance moves the clock, and the dwells
//! end early by at most the size of the jump. A backward jump delays every dwell
//! by its size. A wall clock that was wrong when the node started, and was
//! corrected by a large step later, reads as the monotonic time plus the
//! tolerance until the next start. The monotonic clock of the host may stop while
//! the host sleeps. The clock then lags the wall clock, a restart takes the wall
//! clock again, and the dwells stamped during the lag end early by the lag.

use std::time::Instant;

/// The least tolerance, in milliseconds.
const MIN_TOLERANCE_MS: u64 = 30_000;
/// The greatest tolerance: one day.
const MAX_TOLERANCE_MS: u64 = 24 * 60 * 60 * 1000;

/// The wall clock when the store was made, and the monotonic clock then.
#[derive(Debug)]
pub(crate) struct BlobClock {
    anchor_wall_ms: u64,
    anchor: Instant,
}

impl Default for BlobClock {
    fn default() -> Self {
        Self {
            anchor_wall_ms: crate::clock::unix_millis(),
            anchor: Instant::now(),
        }
    }
}

/// How far ahead of the monotonic estimate the wall clock may run: a quarter of
/// the dwell, between 30 s and one day.
fn tolerance_ms() -> u64 {
    (super::blob_dwell_millis() / 4).clamp(MIN_TOLERANCE_MS, MAX_TOLERANCE_MS)
}

impl BlobClock {
    /// Unix milliseconds: the wall clock, at most the tolerance ahead of the
    /// estimate that the monotonic clock gives.
    pub(crate) fn now_millis(&self) -> u64 {
        self.now_with(crate::clock::unix_millis(), tolerance_ms())
    }

    fn now_with(&self, wall_ms: u64, tolerance_ms: u64) -> u64 {
        let elapsed = u64::try_from(self.anchor.elapsed().as_millis()).unwrap_or(u64::MAX);
        let estimate = self.anchor_wall_ms.saturating_add(elapsed);
        wall_ms.min(estimate.saturating_add(tolerance_ms))
    }
}
