//! Shared telemetry substrate for the LastDB monorepo.
//!
//! Provides PII redaction (`redact!` / `redact_id!`), W3C trace propagation,
//! panic marker hooks, UTF-8 boundary truncation, JSON log formatting, and
//! optional Sentry error reporting.
#![warn(missing_docs)]

pub mod attrs;
pub mod crash;
pub mod init;
pub mod layers;
#[cfg(feature = "otel")]
pub mod propagation;
pub mod truncate;

// Re-export `xxhash_rust` so the `redact_id!` macro can reach it via
// `$crate::xxhash_rust::...` without requiring every consumer crate to
// take a direct dependency on `xxhash-rust`.
#[doc(hidden)]
pub use xxhash_rust;

pub use init::{init_lambda, ObsGuard};
pub use layers::error::capture_error;

/// Errors raised by `init_lambda` and other crate-level operations.
#[derive(Debug, thiserror::Error)]
pub enum ObsError {
    /// `init_lambda` was called more than once in the same process.
    #[error("observability already initialized")]
    AlreadyInitialized,
    /// Could not install the global tracing subscriber.
    #[error("failed to install tracing subscriber: {0}")]
    SubscriberInstall(String),
    /// Could not open or write to the configured sink.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
