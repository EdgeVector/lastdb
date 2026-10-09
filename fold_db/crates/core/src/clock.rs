//! Wall-clock reads as whole units since the Unix epoch.
//!
//! One definition replaces the per-module `now_secs` / `unix_ms_now` /
//! `unix_nanos` wrappers. A clock set before the epoch reads as 0, and a value
//! too large for `u64` saturates, so none of these functions can panic.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn since_epoch() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
}

/// Whole seconds since the Unix epoch.
pub fn unix_secs() -> u64 {
    since_epoch().as_secs()
}

/// Whole milliseconds since the Unix epoch, saturating at `u64::MAX`.
pub fn unix_millis() -> u64 {
    u64::try_from(since_epoch().as_millis()).unwrap_or(u64::MAX)
}

/// Whole nanoseconds since the Unix epoch, saturating at `u64::MAX`.
pub fn unix_nanos() -> u64 {
    u64::try_from(since_epoch().as_nanos()).unwrap_or(u64::MAX)
}

/// Whole nanoseconds since the Unix epoch, without saturation.
pub fn unix_nanos_wide() -> u128 {
    since_epoch().as_nanos()
}
