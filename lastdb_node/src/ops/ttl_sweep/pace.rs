//! Batch size and pacing of the TTL sweep.
//!
//! One purge batch is one erasure job on the persist lane of its schema, with
//! several durable barriers, and the sweep holds the local mutation admission
//! while the batch runs. A snapshot cut waits for that admission, and every
//! later writer queues behind the cut. So the stall that a sweep causes is as
//! long as one batch. The sweep keeps a batch small, drops the admission
//! between batches, and rests after each batch for a share of the batch time.
//!
//! All three knobs are read at the start of every pass, so a change needs no
//! restart of the node. A value that does not parse, or lies outside its range,
//! is clamped into the range.

use std::time::Duration;

/// Environment name: keys purged in one batch.
pub const TTL_SWEEP_BATCH_ENV: &str = "LASTDB_TTL_SWEEP_BATCH";
/// Environment name: the share of wall time, in percent, that the sweep may
/// spend inside purge batches.
pub const TTL_SWEEP_DUTY_PCT_ENV: &str = "LASTDB_TTL_SWEEP_DUTY_PCT";
/// Environment name: the shortest rest between two batches, in milliseconds.
pub const TTL_SWEEP_MIN_PAUSE_MS_ENV: &str = "LASTDB_TTL_SWEEP_MIN_PAUSE_MS";

const MAX_BATCH: usize = 64;
const DEFAULT_DUTY_PCT: u64 = 25;
const DEFAULT_MIN_PAUSE_MS: u64 = 25;
/// The longest rest after one batch. A very slow batch earns a long rest, but
/// the pass must not park for minutes between two batches.
const MAX_PAUSE: Duration = Duration::from_secs(30);

/// The tuned values of one pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Pace {
    pub(super) batch: usize,
    duty_pct: u64,
    min_pause: Duration,
}

impl Pace {
    /// `default_batch` is the batch size when the environment sets none. The
    /// verb chooses it, because the cost of a batch depends on the verb.
    pub(super) fn from_env(default_batch: usize) -> Self {
        Self::new(
            env_flag::var_or(TTL_SWEEP_BATCH_ENV, default_batch),
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
    pub(super) fn pause_after(&self, took: Duration) -> Duration {
        let work_ms = u64::try_from(took.as_millis()).unwrap_or(u64::MAX);
        let rest_ms = work_ms.saturating_mul(100 - self.duty_pct) / self.duty_pct;
        Duration::from_millis(rest_ms)
            .max(self.min_pause)
            .min(MAX_PAUSE)
    }
}
