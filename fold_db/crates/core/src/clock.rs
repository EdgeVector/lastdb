//! Wall-clock reads as whole units since the Unix epoch.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn since_epoch() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
}

pub fn unix_secs() -> u64 {
    since_epoch().as_secs()
}

pub fn unix_millis() -> u64 {
    u64::try_from(since_epoch().as_millis()).unwrap_or(u64::MAX)
}

pub fn unix_nanos() -> u64 {
    u64::try_from(since_epoch().as_nanos()).unwrap_or(u64::MAX)
}

pub fn unix_nanos_wide() -> u128 {
    since_epoch().as_nanos()
}
