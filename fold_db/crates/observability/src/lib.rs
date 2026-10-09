//! Shared telemetry substrate for the LastDB / fold monorepo.
//!
//! This crate is the home for all things `tracing` + Sentry trace tagging
//! across `fold_db`, `lastdb_node`, `schema_service`, and Lambda handlers.
//! It deliberately has zero dependencies on `fold_db` core so it can be
//! consumed from sibling crates and external repos without pulling the world.
//! (Desktop/Tauri/`fold_db_node` were removed in the Mini cutover — do not
//! reintroduce them.)
//!
//! What ships:
//!
//! - [`attrs`] — canonical attribute keys + the [`redact!`] / [`redact_id!`]
//!   macros used at log call-sites for PII opacity.
//! - [`propagation`] — W3C `traceparent` inject on `reqwest` egress and
//!   extract from `http::HeaderMap` on ingress (requires `otel` feature).
//! - [`layers`] — FMT (redacting JSON formatter), RELOAD (runtime
//!   `EnvFilter` swap), RING (bounded in-memory log buffer), WEB (SSE
//!   broadcast), and the ERROR-only Sentry sink.
//! - [`init`] — `init_node` / `init_node_with_web` / `init_lambda` /
//!   `init_cli` helpers that compose the layers per binary
//!   type and return an [`ObsGuard`] (or [`NodeObsGuardWithWeb`] for the
//!   web-streaming variant) for the lifetime of the process.
//!
//! Features: `otel` (default) enables the OpenTelemetry no-op TracerProvider
//! and W3C propagation. `default-features = false` keeps JSON FMT + optional
//! Sentry for Lambda-light builds without OTel/reqwest.

pub mod attrs;
pub mod crash;
pub mod init;
pub mod layers;
pub mod progress_reporter;
#[cfg(feature = "otel")]
pub mod propagation;
pub mod truncate;

// Re-export `xxhash_rust` so the `redact_id!` macro can reach it via
// `$crate::xxhash_rust::...` without requiring every consumer crate to
// take a direct dependency on `xxhash-rust`.
#[doc(hidden)]
pub use xxhash_rust;

pub use crash::{
    crash_reports_dir, install_crash_hook, list_crash_reports, resolve_crash_report,
    scan_and_warn_previous_crashes, CrashContext,
};
pub use init::{
    init_cli, init_lambda, init_node, init_node_with_web, installed_service_name,
    NodeObsGuardWithWeb, ObsGuard, ObsHandles,
};
pub use layers::error::capture_error;

/// Errors raised by `init_*` helpers and other crate-level operations.
#[derive(Debug, thiserror::Error)]
pub enum ObsError {
    /// `init_*` was called more than once for the same target.
    #[error("observability already initialized")]
    AlreadyInitialized,
    /// Could not install the global tracing subscriber.
    #[error("failed to install tracing subscriber: {0}")]
    SubscriberInstall(String),
    /// Could not open or write to the configured sink (e.g. log file).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
