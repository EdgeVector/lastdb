//! Chunk size and pacing of the dwell pass.
//!
//! The duty share measures the time a chunk keeps writers waiting. Background
//! batches share the bounded collection window and still wait for durability.
//! Their private prefix is available to write preparation only. Ordinary reads
//! retain the committed view. A chunk that needs a sealed fence includes that
//! flush wait in its measured cost. Other chunks measure their gate hold and
//! the shared flush hold, excluding the open collection window.
//!
//! The writer watch preserves that share for sparse writers. An idle node uses
//! only the minimum pause. The environment sets the knobs at the start of a pass.

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
