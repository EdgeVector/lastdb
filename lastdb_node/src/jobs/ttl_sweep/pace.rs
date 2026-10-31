//! Batch size and pacing of the TTL sweep.
//!
//! The duty share uses the time a batch keeps writers waiting. Reads before
//! the write gate do not count. A writer watch preserves the duty share for
//! sparse writers; an idle node uses only the minimum pause.

use std::time::Duration;

/// Environment name: keys erased in one batch.
pub const TTL_SWEEP_BATCH_ENV: &str = "LASTDB_TTL_SWEEP_BATCH";
/// Environment name: the share of time, in percent, that the sweep may keep writers waiting.
pub const TTL_SWEEP_DUTY_PCT_ENV: &str = "LASTDB_TTL_SWEEP_DUTY_PCT";
/// Environment name: the shortest rest between two batches, in milliseconds.
pub const TTL_SWEEP_MIN_PAUSE_MS_ENV: &str = "LASTDB_TTL_SWEEP_MIN_PAUSE_MS";

const DEFAULT_BATCH: usize = 16;
const MAX_BATCH: usize = 64;
const DEFAULT_DUTY_PCT: u64 = 25;
const DEFAULT_MIN_PAUSE_MS: u64 = 25;
/// The longest rest after one batch. It keeps the duty share exact for a batch
/// of up to 200 s at the default duty, and it bounds how long the pass waits
/// between two batches when a batch is slower than that.
const MAX_PAUSE: Duration = Duration::from_secs(600);

/// The tuned values of one pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Pace {
    pub(super) batch: usize,
    duty_pct: u64,
    min_pause: Duration,
}

impl Pace {
    pub(super) fn from_env() -> Self {
        Self::new(
            env_flag::var_or(TTL_SWEEP_BATCH_ENV, DEFAULT_BATCH),
            env_flag::var_or(TTL_SWEEP_DUTY_PCT_ENV, DEFAULT_DUTY_PCT),
            env_flag::var_or(TTL_SWEEP_MIN_PAUSE_MS_ENV, DEFAULT_MIN_PAUSE_MS),
        )
    }

    fn new(batch: usize, duty_pct: u64, min_pause_ms: u64) -> Self {
        Self {
            batch: batch.clamp(1, MAX_BATCH),
            duty_pct: duty_pct.clamp(1, 100),
            min_pause: Duration::from_millis(min_pause_ms).min(MAX_PAUSE),
        }
    }

    /// The rest after a batch that took `took`. With a duty share of `d`
    /// percent the rest is `took * (100 - d) / d`.
    pub(super) fn pause_after(&self, blocked: Duration, others_wrote: bool) -> Duration {
        if !others_wrote {
            return self.min_pause;
        }
        let work_ms = u64::try_from(blocked.as_millis()).unwrap_or(u64::MAX);
        let rest_ms = work_ms.saturating_mul(100 - self.duty_pct) / self.duty_pct;
        Duration::from_millis(rest_ms)
            .max(self.min_pause)
            .min(MAX_PAUSE)
    }
}
