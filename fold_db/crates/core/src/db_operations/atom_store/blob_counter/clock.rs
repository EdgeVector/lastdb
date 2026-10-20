//! The clock of the blob counters.
//!
//! A dwell starts at `zero_since`, which a writer stamps, and ends when the dwell
//! pass reads that time plus the dwell as past. Both sides read this clock. A
//! wall clock that jumps forward would end every dwell at once and delete blobs
//! that a late write still names. So the clock is the wall clock, but never more
//! than one day ahead of the time that the monotonic clock gives since the store
//! was made. A forward excursion longer than a day cannot end a dwell early. A
//! smaller forward jump moves the clock, and the dwells then end early by at
//! most the size of the jump (the dwell floor and the default dwell keep that
//! small against the delay of a write). A backward jump delays every dwell by
//! its size. A wall clock that was wrong when the node started, and was
//! corrected by more than a day later, reads as the monotonic time plus a day
//! until the next start.

use std::time::Instant;

/// A forward excursion of the wall clock beyond this is ignored: one day.
const TOLERANCE_MS: u64 = 24 * 60 * 60 * 1000;

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

impl BlobClock {
    /// Unix milliseconds: the wall clock, at most a day ahead of the estimate
    /// that the monotonic clock gives.
    pub(crate) fn now_millis(&self) -> u64 {
        let elapsed = u64::try_from(self.anchor.elapsed().as_millis()).unwrap_or(u64::MAX);
        let estimate = self.anchor_wall_ms.saturating_add(elapsed);
        crate::clock::unix_millis().min(estimate.saturating_add(TOLERANCE_MS))
    }
}
