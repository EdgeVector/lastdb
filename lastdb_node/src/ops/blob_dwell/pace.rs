//! Chunk size and pacing of the dwell pass.
//!
//! One chunk is one hold of the native write gate and one batch with its own
//! flush, and the pass holds the local mutation admission while the chunk runs.
//! A writer that arrives meanwhile waits for the chunk. A pass that runs its
//! chunks back to back keeps the gate busy for as long as the queue lasts. So the
//! pass keeps a chunk small and rests after each chunk for a share of the time
//! the chunk held the gate.
//!
//! The rest after a chunk is `held * (100 - duty) / duty`, where `held` is the
//! time the chunk held the native write gate (its reads in the gate, its staging
//! and its accept). The wait for the flush comes after the gate is free and is not
//! part of `held`: a rest that followed the wall time of the chunk was about nine
//! times the flush wait and kept the pass to about five blobs a second. The rest
//! is at least the minimum rest and at most [`MAX_PAUSE`]. The knobs come from
//! the node environment and are read at the start of each pass. A value that does
//! not parse, or lies outside its range, is clamped into the range.

use std::time::Duration;

/// Environment name: blobs deleted in one chunk (one hold of the gate).
pub const BLOB_DWELL_CHUNK_ENV: &str = "LASTDB_BLOB_DWELL_CHUNK";
/// Environment name: the share of write-gate time, in percent, that the pass may
/// use.
pub const BLOB_DWELL_DUTY_PCT_ENV: &str = "LASTDB_BLOB_DWELL_DUTY_PCT";
/// Environment name: the shortest rest between two chunks, in milliseconds.
pub const BLOB_DWELL_MIN_PAUSE_MS_ENV: &str = "LASTDB_BLOB_DWELL_MIN_PAUSE_MS";
/// Environment name: seconds between two sweeps of the safety net of the dwell
/// queue (`0` turns the net off).
pub const BLOB_DWELL_NET_SECS_ENV: &str = "LASTDB_BLOB_DWELL_NET_SECS";
/// Default time between two sweeps of the safety net: one hour.
const DEFAULT_NET_SECS: u64 = 3_600;

const DEFAULT_CHUNK: usize = 32;
const DEFAULT_DUTY_PCT: u64 = 10;
const DEFAULT_MIN_PAUSE_MS: u64 = 25;
/// The longest rest after one chunk.
const MAX_PAUSE: Duration = Duration::from_secs(30);

/// The tuned values of one pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Pace {
    pub(super) chunk: usize,
    duty_pct: u64,
    min_pause: Duration,
}

impl Pace {
    pub(super) fn from_env() -> Self {
        Self::new(
            env_flag::var_or(BLOB_DWELL_CHUNK_ENV, DEFAULT_CHUNK),
            env_flag::var_or(BLOB_DWELL_DUTY_PCT_ENV, DEFAULT_DUTY_PCT),
            env_flag::var_or(BLOB_DWELL_MIN_PAUSE_MS_ENV, DEFAULT_MIN_PAUSE_MS),
        )
    }

    fn new(chunk: usize, duty_pct: u64, min_pause_ms: u64) -> Self {
        Self {
            chunk: chunk.clamp(1, fold_db::db_operations::DWELL_CHUNK),
            duty_pct: duty_pct.clamp(1, 100),
            min_pause: Duration::from_millis(min_pause_ms).min(MAX_PAUSE),
        }
    }

    /// The rest after a chunk that held the write gate for `held`. With a duty
    /// share of `d` percent the rest is `held * (100 - d) / d`.
    pub(super) fn pause_after(&self, held: Duration) -> Duration {
        let held_us = u64::try_from(held.as_micros()).unwrap_or(u64::MAX);
        let rest_us = held_us.saturating_mul(100 - self.duty_pct) / self.duty_pct;
        Duration::from_micros(rest_us)
            .max(self.min_pause)
            .min(MAX_PAUSE)
    }
}

/// The time between two sweeps of the safety net, from
/// `LASTDB_BLOB_DWELL_NET_SECS`. Unset or invalid gives one hour, and zero turns
/// the net off (`None`).
pub(super) fn net_every_from_env() -> Option<Duration> {
    parse_net_every(std::env::var(BLOB_DWELL_NET_SECS_ENV).ok().as_deref())
}

fn parse_net_every(raw: Option<&str>) -> Option<Duration> {
    let default = Some(Duration::from_secs(DEFAULT_NET_SECS));
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return default;
    };
    match raw.parse::<u64>() {
        Ok(0) => None,
        Ok(secs) => Some(Duration::from_secs(secs)),
        Err(_) => {
            tracing::warn!(
                raw,
                env = BLOB_DWELL_NET_SECS_ENV,
                default_secs = DEFAULT_NET_SECS,
                "invalid blob dwell net interval; using the default"
            );
            default
        }
    }
}
