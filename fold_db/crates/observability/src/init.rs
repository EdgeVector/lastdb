//! Initialization helpers for each runtime target (node / Lambda).
//!
//! Each helper composes the FMT + RELOAD + RING layers into a `Registry`,
//! installs a no-op [`opentelemetry_sdk::trace::TracerProvider`] so every
//! span carries a real W3C `trace_id` / `span_id` (lifted into RING entries
//! and Sentry tags), optionally adds the ERROR-only Sentry sink when
//! [`OBS_SENTRY_DSN`](crate::layers::error::OBS_SENTRY_DSN_ENV) is set, and
//! installs the result as the global tracing subscriber. The returned
//! [`ObsGuard`] holds the non-blocking writer's worker handle, the
//! [`RingHandle`] / [`ReloadHandle`] used by the rest of the binary, and the
//! Sentry shutdown handle whose `Drop` flushes any pending event queue.
//!
//! ## Per-target shape
//!
//! | helper                  | FMT target               | RELOAD | RING | Sentry |
//! |-------------------------|--------------------------|--------|------|--------|
//! | [`init_node`]           | `OBS_FILE_PATH` ▸ `<node-home>/observability.jsonl` (`LASTDB_HOME` ▸ `~/.lastdb`) | yes | yes | env-gated |
//! | [`init_lambda`]         | stdout                   | yes    | no   | env-gated |
//!
//! ## Single-init invariant
//!
//! A process-global [`once_cell::sync::OnceCell`] enforces exactly one
//! installation. The first successful call wins; every subsequent call
//! returns [`crate::ObsError::AlreadyInitialized`] without panicking and
//! without touching the installed subscriber.
//!
//! ## Contract for callers
//!
//! - `service_name` must be non-empty (whitespace-only is also rejected).
//!   A bad value panics with a message containing `service.name` — this is a
//!   programming error, not a runtime one. The same rule applies to every
//!   `init_*` helper.
//! - After `init_*` returns successfully, the global TracerProvider's
//!   `Resource` is guaranteed to carry a `service.name` attribute equal to
//!   the input. The post-build verification in [`build_noop_traces_layer`]
//!   panics if that invariant is ever violated, and [`installed_service_name`]
//!   returns the verified value for inspection (used by integration tests).
//! - The returned [`ObsGuard`] **must** be held for the lifetime of the
//!   binary. Dropping it stops the FMT worker thread mid-flush and triggers
//!   Sentry shutdown; any log lines or events still in flight after that
//!   point are lost.

use std::path::PathBuf;
#[cfg(feature = "otel")]
use std::sync::{Arc, Mutex};
use std::{fs, io};

use once_cell::sync::OnceCell;
use tracing_log::LogTracer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{EnvFilter, Registry};

#[cfg(feature = "otel")]
use opentelemetry::global;
#[cfg(feature = "otel")]
use opentelemetry::trace::{TraceResult, TracerProvider as _};
#[cfg(feature = "otel")]
use opentelemetry::{Context, Key, KeyValue};
#[cfg(feature = "otel")]
use opentelemetry_sdk::export::trace::SpanData;
#[cfg(feature = "otel")]
use opentelemetry_sdk::propagation::TraceContextPropagator;
#[cfg(feature = "otel")]
use opentelemetry_sdk::trace::{
    Sampler, Span as SdkSpan, SpanProcessor, Tracer, TracerProvider as SdkTracerProvider,
};
#[cfg(feature = "otel")]
use opentelemetry_sdk::Resource;
#[cfg(feature = "otel")]
use tracing_opentelemetry::OpenTelemetryLayer;

use crate::layers::error::{build_error_layer, SentryGuard};
use crate::layers::fmt::{build_fmt_writer, FmtGuard, FmtTarget, RedactingFormat};
use crate::layers::reload::{build_reload_layer, ReloadHandle};
use crate::layers::ring::{build_ring_layer, RingHandle, OBS_RING_CAPACITY};
use crate::ObsError;

/// Override for the node log file path. Read once per `init_node` call.
const OBS_FILE_PATH_ENV: &str = "OBS_FILE_PATH";

/// Process-global guard against double init. Set on the first successful
/// `init_*` call; remains set for the lifetime of the process.
static INIT_ONCE: OnceCell<()> = OnceCell::new();

/// Process-global cache of the `service.name` that the most recent successful
/// `init_*` call committed to. Set after the post-build Resource verification
/// passes. Exposed via [`installed_service_name`] for tests and operators that
/// need to confirm what the observability stack ultimately advertised.
static SERVICE_NAME: OnceCell<&'static str> = OnceCell::new();

/// RAII handle returned by every `init_*` helper.
///
/// Holds:
/// - the FMT layer's [`tracing_appender::non_blocking`] worker guard (when
///   this process did the install) so the background flush thread keeps
///   draining the queue,
/// - the [`RingHandle`] for in-process `/api/logs` queries (when RING is
///   wired — currently only the [`init_node`] full path),
/// - the [`ReloadHandle`] for runtime `EnvFilter` updates (when RELOAD is
///   wired — [`init_node`] and [`init_lambda`]),
/// - the [`CloudShutdown`] bag holding the Sentry guard. `Drop` runs the
///   guard's flush so the binary's exit path drains any in-flight events.
///   Always present in shape; the inner field is `None` when the matching
///   env var was not set at init time.
#[must_use = "ObsGuard must be held for the lifetime of the binary or log lines may be dropped"]
pub struct ObsGuard {
    fmt_guard: Option<FmtGuard>,
    ring: Option<RingHandle>,
    reload: Option<ReloadHandle>,
    cloud_shutdown: Option<CloudShutdown>,
}

/// Bag of cloud-side shutdown handles. Dropped by [`ObsGuard`]'s `Drop`.
///
/// Currently holds only the Sentry guard. Kept as a struct (rather than a
/// bare `Option<SentryGuard>`) so the existing test surface that asserts
/// "shape is engaged when env var is set" stays meaningful and so future
/// distributed-client sinks can attach here without reshaping `ObsGuard`.
struct CloudShutdown {
    sentry_guard: Option<SentryGuard>,
}

impl CloudShutdown {
    fn is_engaged(&self) -> bool {
        self.sentry_guard.is_some()
    }
}

impl ObsGuard {
    /// Handle to the in-memory ring buffer. `None` for targets that don't
    /// install the RING layer (Lambda, CLI) or for the Tauri "attached"
    /// degraded guard.
    pub fn ring(&self) -> Option<&RingHandle> {
        self.ring.as_ref()
    }

    /// Handle to swap the active `EnvFilter` at runtime. `None` for targets
    /// that don't install the RELOAD layer (CLI) or for the Tauri "attached"
    /// degraded guard.
    pub fn reload(&self) -> Option<&ReloadHandle> {
        self.reload.as_ref()
    }
}

impl std::fmt::Debug for ObsGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObsGuard")
            .field("fmt_guard", &self.fmt_guard.is_some())
            .field("ring", &self.ring.is_some())
            .field("reload", &self.reload.is_some())
            .field(
                "cloud_shutdown",
                &self.cloud_shutdown.as_ref().map(CloudShutdown::is_engaged),
            )
            .finish()
    }
}

impl Drop for ObsGuard {
    fn drop(&mut self) {
        // Field drops do the rest: SentryGuard's inner `ClientInitGuard`
        // flushes on Drop, FmtGuard's Drop drains the writer queue.
        let _ = self.cloud_shutdown.take();
    }
}

// ---------------------------------------------------------------------------
// Public init helpers
// ---------------------------------------------------------------------------

/// Initialize observability for a long-running node binary.
///
/// Layers (always wired): redacting JSON FMT writing to the node log file
/// resolved by [`default_node_log_path`] (`OBS_FILE_PATH` ▸
/// `<node-home>/observability.jsonl`, where the node home is `LASTDB_HOME` ▸
/// `~/.lastdb` — the same dir the data lives in) +
/// RELOAD + RING + a `tracing-opentelemetry` layer riding a no-op
/// [`opentelemetry_sdk::trace::TracerProvider`] that stamps W3C `trace_id` /
/// `span_id` onto every span.
///
/// Optional ERROR sink: when `OBS_SENTRY_DSN` is set, a per-layer-filtered
/// Sentry layer captures `tracing::error!` events and tags them with the
/// originating span's W3C ids.
///
/// Also installs the W3C [`TraceContextPropagator`] globally and the
/// `tracing-log` bridge so third-party `log::*` calls flow through the
/// subscriber.
pub fn init_node(service_name: &'static str, _version: &str) -> Result<ObsGuard, ObsError> {
    assert_service_name(service_name);
    try_claim_init(&INIT_ONCE)?;
    install_log_tracer();

    let path = default_node_log_path()?;
    let (writer, fmt_guard) = build_fmt_writer(FmtTarget::File(path))?;
    let (reload_layer, reload) = build_reload_layer::<Registry>(default_env_filter());
    let (ring_layer, ring) = build_ring_layer(OBS_RING_CAPACITY);

    let (error_layer, sentry_guard) = match build_error_layer() {
        Some((layer, guard)) => (Some(layer), Some(guard)),
        None => (None, None),
    };

    let fmt_layer = tracing_subscriber::fmt::layer()
        .event_format(RedactingFormat::from_env_with_service(service_name))
        .with_writer(writer);

    // RELOAD is innermost so its `S = Registry` type binding matches; the
    // remaining layers are generic over `S` and the compiler infers each one
    // from the composition site. By the time RING's `on_event` runs, OTel's
    // `on_new_span` has already attached `OtelData` to the parent span, so
    // RING's extension lookup finds the trace/span ids regardless of layer
    // ordering at this level. The Sentry layer goes last so its `event_span`
    // lookup sees the OtelData attached upstream.
    #[cfg(feature = "otel")]
    {
        let otel_layer = build_noop_traces_layer(service_name);
        let subscriber = Registry::default()
            .with(reload_layer)
            .with(otel_layer)
            .with(fmt_layer)
            .with(ring_layer)
            .with(error_layer);
        install_subscriber(subscriber)?;
    }
    #[cfg(not(feature = "otel"))]
    {
        let subscriber = Registry::default()
            .with(reload_layer)
            .with(fmt_layer)
            .with(ring_layer)
            .with(error_layer);
        install_subscriber(subscriber)?;
    }
    install_globals();
    record_service_name(service_name);

    Ok(ObsGuard {
        fmt_guard: Some(fmt_guard),
        ring: Some(ring),
        reload: Some(reload),
        cloud_shutdown: Some(CloudShutdown { sentry_guard }),
    })
}

/// Initialize observability for an AWS Lambda handler.
///
/// Layers: redacting JSON FMT to stdout + RELOAD + the same no-op
/// TracerProvider as [`init_node`] + the env-gated Sentry layer. Lambda's own
/// log capture pipes stdout to CloudWatch, so a file appender would be wasted
/// IO. RING is omitted — Lambda invocations are too short-lived for an
/// in-process query buffer to be useful.
pub fn init_lambda(service_name: &'static str, _version: &str) -> Result<ObsGuard, ObsError> {
    assert_service_name(service_name);
    try_claim_init(&INIT_ONCE)?;
    install_log_tracer();

    let (writer, fmt_guard) = build_fmt_writer(FmtTarget::Stdout)?;
    let (reload_layer, reload) = build_reload_layer::<Registry>(default_env_filter());

    let (error_layer, sentry_guard) = match build_error_layer() {
        Some((layer, guard)) => (Some(layer), Some(guard)),
        None => (None, None),
    };

    let fmt_layer = tracing_subscriber::fmt::layer()
        .event_format(RedactingFormat::from_env())
        .with_writer(writer);

    // With default features, compose the no-op OTel layer for W3C span ids.
    // With `default-features = false` (Lambda light), JSON FMT + optional
    // Sentry only — no OTel layer / reqwest / opentelemetry stack.
    #[cfg(feature = "otel")]
    {
        let otel_layer = build_noop_traces_layer(service_name);
        let subscriber = Registry::default()
            .with(reload_layer)
            .with(otel_layer)
            .with(fmt_layer)
            .with(error_layer);
        install_subscriber(subscriber)?;
    }
    #[cfg(not(feature = "otel"))]
    {
        let subscriber = Registry::default()
            .with(reload_layer)
            .with(fmt_layer)
            .with(error_layer);
        install_subscriber(subscriber)?;
    }
    install_globals();
    record_service_name(service_name);

    Ok(ObsGuard {
        fmt_guard: Some(fmt_guard),
        ring: None,
        reload: Some(reload),
        cloud_shutdown: Some(CloudShutdown { sentry_guard }),
    })
}

mod support;
use support::*;
