//! Operating-system clock reads shared by the schema service crates.
//!
//! [`unix_secs`] reads as 0 when the clock is set before the epoch, so that
//! call cannot panic. [`wall_time`] is the chrono wall clock and keeps
//! subsecond precision. The registry core does not call these directly; the
//! shell adapter does.

use chrono::{DateTime, Utc};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Whole seconds since the Unix epoch.
pub fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Wall time, including subseconds. Same read as `Utc::now()`.
pub fn wall_time() -> DateTime<Utc> {
    Utc::now()
}

/// Monotonic instant. Same read as `Instant::now()`.
pub fn monotonic() -> Instant {
    Instant::now()
}
