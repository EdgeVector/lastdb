//! Compile-time Sentry release identity.

/// Git SHA (or deploy release string) baked by `build.rs`.
pub const RELEASE_SHA: &str = env!("OBS_RELEASE_SHA");

/// Optional user-device DSN baked into release builds (`LASTDB_SENTRY_DSN`).
pub const EMBEDDED_DSN: Option<&str> = option_env!("LASTDB_SENTRY_DSN");
