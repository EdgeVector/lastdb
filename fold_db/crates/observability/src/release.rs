//! Compile-time Sentry release identity and user-device DSN.

/// Git SHA (or deploy release string) baked by `build.rs`.
pub const RELEASE_SHA: &str = env!("OBS_RELEASE_SHA");

/// Public DSN for the `lastdb-mini` Sentry project (not a secret).
/// Every user-device build (lastdbd, CLI, People) reports here with `environment=user`.
pub const USER_DEVICE_DSN: &str =
    "https://2de2e2512878eedeef5359696ac37e70@o4511522196291584.ingest.us.sentry.io/4511737993101312"; // secret-scan-ok: public lastdb-mini client DSN
