//! RFC 3339 conversion for UTC timestamps.
//!
//! These functions do not read the operating-system clock. The shell reads
//! the clock and passes the value in. Output matches `chrono`'s
//! `DateTime<Utc>` formatting and `parse_from_rfc3339` errors.

use chrono::{DateTime, SecondsFormat, Utc};

/// Second precision and a `Z` suffix.
///
/// Same bytes as `to_rfc3339_opts(SecondsFormat::Secs, true)`.
pub fn secs_z(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// Subsecond precision. UTC offsets use `+00:00`.
///
/// Same bytes as `DateTime<Utc>::to_rfc3339`.
pub fn subseconds(time: DateTime<Utc>) -> String {
    time.to_rfc3339()
}

/// Accept any stamp `DateTime::parse_from_rfc3339` accepts.
///
/// The error text is chrono's parse-error display.
pub fn require_rfc3339(stamp: &str) -> Result<(), String> {
    DateTime::parse_from_rfc3339(stamp)
        .map(|_| ())
        .map_err(|error| error.to_string())
}

/// Parse `stamp` and convert it to UTC.
///
/// The error text is chrono's parse-error display.
pub fn parse_utc(stamp: &str) -> Result<DateTime<Utc>, String> {
    DateTime::parse_from_rfc3339(stamp)
        .map(|parsed| parsed.with_timezone(&Utc))
        .map_err(|error| error.to_string())
}

/// Whole seconds of `later - earlier`, fractional part truncated toward zero.
///
/// Same count as `later.signed_duration_since(earlier).num_seconds()`.
pub fn whole_seconds_between(later: DateTime<Utc>, earlier: DateTime<Utc>) -> i64 {
    later.signed_duration_since(earlier).num_seconds()
}

/// Rebuild a UTC instant from Unix seconds and a nanosecond fraction below 1e9.
pub fn from_unix(secs: i64, subsec_nanos: u32) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp(secs, subsec_nanos)
}
