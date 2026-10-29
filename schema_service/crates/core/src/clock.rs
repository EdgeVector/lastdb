//! Clock port for the registry core.
//!
//! Core does not read the operating-system clock. The shell constructs an
//! adapter and passes it into the state and resolver constructors.

use chrono::{DateTime, Utc};
use std::time::Instant;

/// Wall time returned by [`Clock::wall_time`].
pub type WallTime = DateTime<Utc>;

/// Time source injected by the process shell.
pub trait Clock: Send + Sync {
    /// Wall time, including subseconds.
    fn wall_time(&self) -> WallTime;

    /// Whole seconds since the Unix epoch. A clock before the epoch reads as 0.
    fn unix_secs(&self) -> u64;

    /// Monotonic instant for elapsed measurements.
    fn monotonic(&self) -> Instant;
}

/// RFC 3339 with second precision and a `Z` suffix.
pub(crate) fn rfc3339_secs(clock: &dyn Clock) -> String {
    clock
        .wall_time()
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}
