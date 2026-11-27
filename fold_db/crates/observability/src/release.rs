//! Compile-time Sentry release identity and user-device DSN.

/// Git SHA (or deploy release string) baked by `build.rs`.
pub const RELEASE_SHA: &str = env!("OBS_RELEASE_SHA");

/// User-device DSN baked by `build.rs` (`LASTDB_SENTRY_DSN`, or the lastdb-mini default).
///
/// A DSN is not a secret. Release builds of `lastdb_node` refuse to compile when
/// `LASTDB_SENTRY_DSN` is unset, so shipped bottles always embed an explicit value.
pub const USER_DEVICE_DSN: &str = env!("LASTDB_SENTRY_DSN");
