//! Clock port for the registry core.
//!
//! Core does not read the operating-system clock. The shell constructs an
//! adapter and passes it into the state and resolver constructors. RFC 3339
//! text is produced by that adapter, so this crate does not format timestamps.

use std::time::Instant;

/// Unix instant a caller can freeze for pack-age checks.
///
/// `secs` and `subsec_nanos` match `DateTime<Utc>::timestamp` and
/// `timestamp_subsec_nanos` for instants whose fraction is below one second.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrozenInstant {
    pub secs: i64,
    pub subsec_nanos: u32,
}

/// Time source injected by the process shell.
pub trait Clock: Send + Sync {
    /// RFC 3339 with second precision and a `Z` suffix.
    fn rfc3339_secs(&self) -> String;

    /// RFC 3339 including subseconds. UTC offsets use `+00:00`.
    fn rfc3339(&self) -> String;

    /// Whole seconds since the Unix epoch. A clock before the epoch reads as 0.
    fn unix_secs(&self) -> u64;

    /// Monotonic instant for elapsed measurements.
    fn monotonic(&self) -> Instant;

    /// Whole seconds from `stamp` until the wall clock, or until `frozen` when set.
    ///
    /// `stamp` is RFC 3339. A parse failure returns chrono's parse-error text
    /// and does not read the clock. The second count drops the fractional
    /// part toward zero.
    fn whole_seconds_since_rfc3339(
        &self,
        stamp: &str,
        frozen: Option<FrozenInstant>,
    ) -> Result<i64, String>;
}
