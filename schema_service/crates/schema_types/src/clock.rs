//! Wall-clock read shared by the schema service crates. A clock set before the
//! epoch reads as 0, so the call cannot panic.

use std::time::{SystemTime, UNIX_EPOCH};

/// Whole seconds since the Unix epoch.
pub fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
