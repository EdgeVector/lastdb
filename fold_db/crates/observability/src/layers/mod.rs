//! Subscriber layer implementations.
//!
//! - [`fmt`] — JSON formatter with redaction (T3, stub).
//! - [`reload`] — runtime [`tracing_subscriber::EnvFilter`] swap (T4).
//! - [`ring`] — bounded in-memory ring buffer for `/api/logs` (T5).
//! - [`error`] — ERROR-only Sentry sink with W3C trace tagging
//!   (Phase 4 / T4).

pub mod error;
pub mod fmt;
pub mod reload;
pub mod ring;

pub use error::{build_error_layer, ErrorLayer, SentryGuard, OBS_SENTRY_DSN_ENV};
// `parse_obs_sentry_dsn` is only available with the `sentry` feature (same as
// `build_error_layer`'s real implementation path).
pub use fmt::OBS_LOG_RETENTION_DAYS;
pub use ring::{build_ring_layer, LogEntry, LogLevel, RingHandle, RingLayer, OBS_RING_CAPACITY};
