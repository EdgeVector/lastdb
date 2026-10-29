//! Process-clock adapter for schema service shells.
//!
//! Each process entry constructs one [`SystemClock`] and passes it into
//! `SchemaServiceState`. There is no global clock.

use schema_service_core::{Clock, WallTime};
use std::time::Instant;

/// Operating-system clock. Construct this at the shell entrypoint.
#[derive(Clone, Copy, Debug)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn wall_time(&self) -> WallTime {
        schema_types::clock::wall_time()
    }

    fn unix_secs(&self) -> u64 {
        schema_types::clock::unix_secs()
    }

    fn monotonic(&self) -> Instant {
        schema_types::clock::monotonic()
    }
}
