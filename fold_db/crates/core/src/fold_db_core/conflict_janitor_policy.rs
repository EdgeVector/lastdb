//! The policy of the retention janitor: the environment variables, their
//! defaults, and the parse of each value (`conflict_janitor` reads the policy
//! and runs the task).

use std::time::Duration;

/// Env: interval between janitor wakes in milliseconds (`0` = off).
pub const CONFLICT_JANITOR_MS_ENV: &str = "LASTDB_CONFLICT_JANITOR_MS";
/// Default interval between wakes: one minute.
pub const DEFAULT_CONFLICT_JANITOR_MS: u64 = 60_000;
/// Env: the share of write-gate time, in percent, that the janitor may use. A
/// release batch holds the native write gate, and a writer that meets the gate
/// waits for the batch. The janitor rests after each batch for
/// `held * (100 - duty) / duty`, where `held` is the time the batch held the
/// gate. The wait for the flush comes after the gate is free, so it is not part
/// of `held`.
pub const CONFLICT_JANITOR_DUTY_PCT_ENV: &str = "LASTDB_CONFLICT_JANITOR_DUTY_PCT";
/// Default duty share: 10 percent of the gate time.
pub const DEFAULT_CONFLICT_JANITOR_DUTY_PCT: u64 = 10;
/// Env: seconds between two sweeps of the safety net of the queue (`0` = off).
/// A sweep probes every slice that can hold a live entry, by arithmetic, and
/// registers again a bucket that holds entries and has no directory row.
pub const CONFLICT_JANITOR_NET_SECS_ENV: &str = "LASTDB_CONFLICT_JANITOR_NET_SECS";
/// Default time between sweeps: one hour. The first sweep runs at the start.
pub const DEFAULT_CONFLICT_JANITOR_NET_SECS: u64 = 3_600;

/// Resolved janitor policy for this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConflictJanitorPolicy {
    /// The interval between wakes. `None` means no janitor.
    pub interval: Option<Duration>,
    /// The share of wall time, in percent, that release batches may take.
    pub duty_pct: u64,
    /// The time between two sweeps of the safety net. `None` turns it off.
    pub net_every: Option<Duration>,
}

impl ConflictJanitorPolicy {
    /// Parse from the environment.
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            interval: parse_conflict_janitor_interval(
                std::env::var(CONFLICT_JANITOR_MS_ENV).ok().as_deref(),
            ),
            duty_pct: parse_conflict_janitor_duty(
                std::env::var(CONFLICT_JANITOR_DUTY_PCT_ENV).ok().as_deref(),
            ),
            net_every: parse_conflict_janitor_net(
                std::env::var(CONFLICT_JANITOR_NET_SECS_ENV).ok().as_deref(),
            ),
        }
    }
}

/// `LASTDB_CONFLICT_JANITOR_NET_SECS` to the time between two sweeps of the
/// safety net. Unset or invalid gives the default (one hour), and zero turns the
/// net off.
#[must_use]
pub fn parse_conflict_janitor_net(raw: Option<&str>) -> Option<Duration> {
    let default = Some(Duration::from_secs(DEFAULT_CONFLICT_JANITOR_NET_SECS));
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return default;
    };
    match raw.parse::<u64>() {
        Ok(0) => None,
        Ok(secs) => Some(Duration::from_secs(secs)),
        Err(_) => {
            tracing::warn!(
                raw,
                env = CONFLICT_JANITOR_NET_SECS_ENV,
                default_secs = DEFAULT_CONFLICT_JANITOR_NET_SECS,
                "invalid conflict janitor net interval; using the default"
            );
            default
        }
    }
}

/// `LASTDB_CONFLICT_JANITOR_MS` to an interval. Unset gives the default, zero
/// turns the janitor off, and an invalid value gives the default.
#[must_use]
pub fn parse_conflict_janitor_interval(raw: Option<&str>) -> Option<Duration> {
    let default = Some(Duration::from_millis(DEFAULT_CONFLICT_JANITOR_MS));
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return default;
    };
    match raw.parse::<u64>() {
        Ok(0) => None,
        Ok(ms) => Some(Duration::from_millis(ms)),
        Err(_) => {
            tracing::warn!(
                raw,
                env = CONFLICT_JANITOR_MS_ENV,
                default_ms = DEFAULT_CONFLICT_JANITOR_MS,
                "invalid conflict janitor interval; using the default"
            );
            default
        }
    }
}

/// `LASTDB_CONFLICT_JANITOR_DUTY_PCT` to a percentage from 1 to 100. Unset or
/// invalid gives the default, and a value outside the range is clamped.
#[must_use]
pub fn parse_conflict_janitor_duty(raw: Option<&str>) -> u64 {
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return DEFAULT_CONFLICT_JANITOR_DUTY_PCT;
    };
    raw.parse::<u64>().map_or_else(
        |_| {
            tracing::warn!(
                raw,
                env = CONFLICT_JANITOR_DUTY_PCT_ENV,
                default = DEFAULT_CONFLICT_JANITOR_DUTY_PCT,
                "invalid conflict janitor duty share; using the default"
            );
            DEFAULT_CONFLICT_JANITOR_DUTY_PCT
        },
        |duty| duty.clamp(1, 100),
    )
}
