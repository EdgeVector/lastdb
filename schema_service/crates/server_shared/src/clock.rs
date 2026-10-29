//! Process-clock adapter for schema service shells.
//!
//! Each process entry constructs one [`SystemClock`] and passes it into
//! `SchemaServiceState`. There is no global clock.

use schema_service_core::{Clock, FrozenInstant};
use std::time::Instant;

/// Operating-system clock. Construct this at the shell entrypoint.
#[derive(Clone, Copy, Debug)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn rfc3339_secs(&self) -> String {
        schema_types::rfc3339::secs_z(schema_types::clock::wall_time())
    }

    fn rfc3339(&self) -> String {
        schema_types::rfc3339::subseconds(schema_types::clock::wall_time())
    }

    fn unix_secs(&self) -> u64 {
        // Same read as before: whole seconds, and 0 when the clock is before the epoch.
        schema_types::clock::unix_secs()
    }

    fn monotonic(&self) -> Instant {
        schema_types::clock::monotonic()
    }

    fn whole_seconds_since_rfc3339(
        &self,
        stamp: &str,
        frozen: Option<FrozenInstant>,
    ) -> Result<i64, String> {
        let earlier = schema_types::rfc3339::parse_utc(stamp)?;
        let later = match frozen {
            Some(instant) => {
                schema_types::rfc3339::from_unix(instant.secs, instant.subsec_nanos)
                    .ok_or_else(|| "frozen instant is outside the wall-clock range".to_string())?
            }
            None => schema_types::clock::wall_time(),
        };
        Ok(schema_types::rfc3339::whole_seconds_between(later, earlier))
    }
}
