//! Chunk size and pacing of the dwell pass.
//!
//! One chunk is one hold of the native write gate and one batch with its own
//! flush, and the pass holds the local mutation admission while the chunk runs.
//! A chunk seals the write queue, so a writer that arrives meanwhile waits for the
//! chunk and for its flush. A pass that runs its chunks back to back keeps writers
//! waiting for as long as the queue lasts. So the pass rests after each chunk for a
//! share of the time the chunk kept writers waiting.
//!
//! The rest after a chunk is `blocked * (100 - duty) / duty`, where `blocked` is
//! the time the chunk kept writers waiting: the hold of the native write gate (its
//! reads in the gate, its staging and its accept) and the wait until the chunk is
//! durable. The reads of the queue page before the gate block nobody and are not
//! part of it. When no other task wrote since the last chunk (`WriterWatch`), the
//! rest protects nobody and it is the minimum rest. The rest is otherwise at least
//! the minimum rest and at most [`MAX_PAUSE`]. The knobs come from the node
//! environment and are read at the start of each pass. A value that does not
//! parse, or lies outside its range, is clamped into the range.

use std::time::Duration;

/// Environment name: blobs deleted in one chunk (one hold of the gate).
pub const BLOB_DWELL_CHUNK_ENV: &str = "LASTDB_BLOB_DWELL_CHUNK";
/// Environment name: the share of time, in percent, that the pass may keep
/// writers waiting.
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

    /// The rest after a chunk that kept writers waiting for `blocked`. With a duty
    /// share of `d` percent the rest is `blocked * (100 - d) / d`. When no other
    /// writer was there (`others_wrote` is false) the rest protects nobody, and it
    /// is the minimum rest.
    pub(super) fn pause_after(&self, blocked: Duration, others_wrote: bool) -> Duration {
        if !others_wrote {
            return self.min_pause;
        }
        let blocked_us = u64::try_from(blocked.as_micros()).unwrap_or(u64::MAX);
        let rest_us = blocked_us.saturating_mul(100 - self.duty_pct) / self.duty_pct;
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
