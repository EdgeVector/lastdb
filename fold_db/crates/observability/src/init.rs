//! Initialization helpers for each runtime target (node / Lambda / CLI).
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
//! | helper                  | FMT target               | RELOAD | RING | WEB | Sentry |
//! |-------------------------|--------------------------|--------|------|-----|--------|
//! | [`init_node`]           | `OBS_FILE_PATH` ▸ `<node-home>/observability.jsonl` (honor-both: `LASTDB_HOME` ▸ `FOLDDB_HOME` ▸ existing `~/.lastdb` ▸ existing `~/.folddb` ▸ new `~/.lastdb`) | yes | yes | no  | env-gated |
//! | [`init_node_with_web`]  | same as [`init_node`]    | yes    | yes  | yes | env-gated |
//! | [`init_lambda`]         | stdout                   | yes    | no   | no  | env-gated |
//! | [`init_cli`]            | stderr                   | no     | no   | no  | no     |
//!
//! ## CLI is intentionally bare
//!
//! The Sentry transport amortizes its per-batch cost over many events. CLI
//! processes are short-lived one-shots: setup cost dominates and the
//! transport's batched flushes would not fire before the process exits. We
//! deliberately omit the Sentry layer from [`init_cli`] rather than ship
//! events that never flush.
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
use std::sync::Arc;
#[cfg(feature = "otel")]
use std::sync::Mutex;
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
use crate::layers::web::{build_web_layer, WebHandle, OBS_WEB_CAPACITY};
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

/// Returns the `service.name` that was passed to the most recent successful
/// `init_*` helper, or `None` if no helper has run yet (or the only call
/// panicked before claiming the slot).
///
/// Used by integration tests to assert that the value flowing through
/// `init_*` matches the value the global TracerProvider's Resource ended up
/// carrying.
pub fn installed_service_name() -> Option<&'static str> {
    SERVICE_NAME.get().copied()
}

/// RAII handle returned by every `init_*` helper.
///
/// Holds:
/// - the FMT layer's [`tracing_appender::non_blocking`] worker guard (when
///   this process did the install) so the background flush thread keeps
///   draining the queue,
/// - the [`RingHandle`] for in-process `/api/logs` queries (when RING is
///   wired — currently only the [`init_node`] full path),
/// - the [`ReloadHandle`] for runtime `EnvFilter` updates (when RELOAD is
///   wired — every helper except [`init_cli`]),
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
    fn empty() -> Self {
        Self { sentry_guard: None }
    }

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
/// `<node-home>/observability.jsonl`, where the node home is the honor-both
/// `LASTDB_HOME` ▸ `FOLDDB_HOME` ▸ existing `~/.lastdb` ▸ existing `~/.folddb`
/// ▸ new `~/.lastdb` — the same dir the data lives in) +
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

/// RAII guard returned by [`init_node_with_web`].
///
/// Same lifetime contract as [`ObsGuard`] — must be held for the lifetime of
/// the binary. Differs in two ways:
/// - Every handle is non-optional. Node binaries always wire FMT + RING +
///   RELOAD + WEB, so `Option<…>` accessors only push the unwrap onto every
///   caller.
/// - Adds a [`WebHandle`] for the dashboard's `/api/logs/stream` SSE
///   endpoint. `ReloadHandle` is wrapped in [`Arc`] because it is not
///   `Clone` upstream and `/api/logs/level` needs to share it across Actix
///   workers via `web::Data`.
#[must_use = "NodeObsGuardWithWeb must be held for the lifetime of the binary or log lines may be dropped"]
pub struct NodeObsGuardWithWeb {
    fmt_guard: Option<FmtGuard>,
    ring: RingHandle,
    reload: Arc<ReloadHandle>,
    web: WebHandle,
    cloud_shutdown: Option<CloudShutdown>,
}

impl NodeObsGuardWithWeb {
    /// Handle for in-process `/api/logs` queries.
    pub fn ring(&self) -> &RingHandle {
        &self.ring
    }

    /// Shared handle for runtime `EnvFilter` swaps via `/api/logs/level`.
    pub fn reload(&self) -> Arc<ReloadHandle> {
        Arc::clone(&self.reload)
    }

    /// Handle that the dashboard's `/api/logs/stream` SSE endpoint subscribes
    /// to for live event fan-out.
    pub fn web(&self) -> &WebHandle {
        &self.web
    }

    /// Cheap clones of every handle, ready to wrap in Actix `web::Data`.
    pub fn handles(&self) -> ObsHandles {
        ObsHandles {
            ring: self.ring.clone(),
            web: self.web.clone(),
            reload: Arc::clone(&self.reload),
        }
    }
}

impl std::fmt::Debug for NodeObsGuardWithWeb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeObsGuardWithWeb")
            .field("fmt_guard", &self.fmt_guard.is_some())
            .field("ring_capacity", &self.ring.capacity())
            .field(
                "cloud_shutdown",
                &self.cloud_shutdown.as_ref().map(CloudShutdown::is_engaged),
            )
            .finish()
    }
}

impl Drop for NodeObsGuardWithWeb {
    fn drop(&mut self) {
        let _ = self.cloud_shutdown.take();
    }
}

/// Bundle of handles consumed by the HTTP server's `/api/logs*` endpoints.
///
/// Cheap to clone: [`RingHandle`] and [`WebHandle`] are `Arc`-backed,
/// [`ReloadHandle`] is shared through [`Arc`] because it is not `Clone`
/// upstream.
#[derive(Clone)]
pub struct ObsHandles {
    pub ring: RingHandle,
    pub web: WebHandle,
    pub reload: Arc<ReloadHandle>,
}

/// Initialize observability for a node binary that ALSO needs a WEB
/// broadcast layer (i.e. anything serving `/api/logs/stream` SSE).
///
/// Composition matches [`init_node`] exactly — redacting JSON FMT to the
/// resolved node log file + RELOAD + RING + a no-op-traces OTel layer
/// stamping W3C `trace_id` / `span_id` + the env-gated Sentry sink — and
/// adds a [`crate::layers::web::WebLayer`] whose [`WebHandle`] the returned
/// guard exposes via [`NodeObsGuardWithWeb::web`].
///
/// The `service_name` validation, `INIT_ONCE` claim, log-file path
/// resolution, and `LogTracer` / W3C-propagator install are identical to
/// [`init_node`] — both helpers route through the same private plumbing so
/// they cannot drift.
///
/// OTLP / Sentry defaults are unchanged from [`init_node`]: no OTLP
/// exporter is wired, and the Sentry layer is composed only when
/// `OBS_SENTRY_DSN` is set at startup.
pub fn init_node_with_web(service_name: &'static str) -> Result<NodeObsGuardWithWeb, ObsError> {
    assert_service_name(service_name);
    try_claim_init(&INIT_ONCE)?;
    install_log_tracer();

    let path = default_node_log_path()?;
    let (writer, fmt_guard) = build_fmt_writer(FmtTarget::File(path))?;
    let (reload_layer, reload) = build_reload_layer::<Registry>(default_env_filter());
    let (ring_layer, ring) = build_ring_layer(OBS_RING_CAPACITY);
    let (web_layer, web) = build_web_layer(OBS_WEB_CAPACITY);

    let (error_layer, sentry_guard) = match build_error_layer() {
        Some((layer, guard)) => (Some(layer), Some(guard)),
        None => (None, None),
    };

    let fmt_layer = tracing_subscriber::fmt::layer()
        .event_format(RedactingFormat::from_env_with_service(service_name))
        .with_writer(writer);

    // Layer order mirrors `init_node` — RELOAD innermost so its `S = Registry`
    // binding pins the type; OTel before FMT/RING/WEB so its `on_new_span`
    // attaches `OtelData` before the downstream layers' `on_event` runs and
    // their lookups for trace/span ids succeed; WEB after RING because both
    // independently lift trace/span ids off the same `OtelData` extension and
    // their relative order is irrelevant for correctness; ERROR last so its
    // `event_span` lookup sees `OtelData` already attached.
    #[cfg(feature = "otel")]
    {
        let otel_layer = build_noop_traces_layer(service_name);
        let subscriber = Registry::default()
            .with(reload_layer)
            .with(otel_layer)
            .with(fmt_layer)
            .with(ring_layer)
            .with(web_layer)
            .with(error_layer);
        install_subscriber(subscriber)?;
    }
    #[cfg(not(feature = "otel"))]
    {
        let subscriber = Registry::default()
            .with(reload_layer)
            .with(fmt_layer)
            .with(ring_layer)
            .with(web_layer)
            .with(error_layer);
        install_subscriber(subscriber)?;
    }
    install_globals();
    record_service_name(service_name);

    Ok(NodeObsGuardWithWeb {
        fmt_guard: Some(fmt_guard),
        ring,
        reload: Arc::new(reload),
        web,
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

/// Initialize observability for a short-lived CLI binary.
///
/// Layers: redacting JSON FMT to stderr only. No RELOAD (CLIs run to
/// completion — runtime filter swaps add no value), no RING (no in-process
/// reader on the other end), no file appender (no daemon to flush).
/// stderr is chosen so the CLI can keep stdout reserved for its own
/// program output.
///
/// The Sentry layer is intentionally omitted. CLI processes are short-lived:
/// the Sentry transport's batched flushes amortize over many events, but a
/// CLI exits before the pipeline reaches its first flush — shipping it would
/// add startup cost with no observable benefit. Long-lived CLIs that need
/// remote error capture should use [`init_node`] instead.
pub fn init_cli(service_name: &'static str, _version: &str) -> Result<ObsGuard, ObsError> {
    assert_service_name(service_name);
    try_claim_init(&INIT_ONCE)?;
    install_log_tracer();

    let (writer, fmt_guard) = build_fmt_writer(FmtTarget::Stderr)?;
    let fmt_layer = tracing_subscriber::fmt::layer()
        .event_format(RedactingFormat::from_env())
        .with_writer(writer);

    let subscriber = Registry::default().with(fmt_layer);
    install_subscriber(subscriber)?;
    install_globals();
    record_service_name(service_name);

    Ok(ObsGuard {
        fmt_guard: Some(fmt_guard),
        ring: None,
        reload: None,
        cloud_shutdown: Some(CloudShutdown::empty()),
    })
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Reject empty / whitespace-only `service_name` at the entry of every
/// `init_*` helper. The OTel `service.name` Resource attribute is the primary
/// dimension dashboards group by — landing in production with `service.name=""`
/// is a silent telemetry-loss hazard. We treat the bad input as a programming
/// error and panic immediately so the operator sees the failure at boot rather
/// than discovering it in their backend the next morning.
///
/// The panic message MUST include the literal `service.name` so callers can
/// match on it; integration tests rely on this string.
#[inline]
fn assert_service_name(name: &str) {
    assert!(
        !name.trim().is_empty(),
        "service.name must be non-empty (got {name:?})",
    );
}

/// Atomically claim the one-shot init slot. Returns
/// [`ObsError::AlreadyInitialized`] when another caller already set it.
fn try_claim_init(cell: &OnceCell<()>) -> Result<(), ObsError> {
    cell.set(()).map_err(|_| ObsError::AlreadyInitialized)
}

/// Stamp the verified `service_name` onto the process-global cache. Called
/// at the tail of every successful `init_*` (after subscriber install +
/// Resource verification). Subsequent calls — including legitimate ones from
/// nested helpers like nested `init_*` → [`init_node`] — silently no-op via
/// `OnceCell::set`'s "already set" semantics, which is the right behaviour:
/// `INIT_ONCE` already gates double-init.
fn record_service_name(service_name: &'static str) {
    let _ = SERVICE_NAME.set(service_name);
}

fn default_env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
}

// OpenTelemetry no-op TracerProvider + service.name probe — only compiled
// when the `otel` feature is enabled (default for node/HTTP; off for Lambda light).
#[cfg(feature = "otel")]
/// Build a `tracing-opentelemetry` layer riding a no-op
/// [`SdkTracerProvider`] so every span gets a real W3C `trace_id` /
/// `span_id`. There is no exporter — the spans never leave the process — but
/// the ids are what the RING layer stamps onto entries and what the Sentry
/// layer attaches as tags, so they have to be real.
///
/// The constructed provider is configured with a `Resource` carrying
/// `service.name`, attached to a [`ResourceProbe`] that captures the resource
/// the SDK pushes into its processors at build time, and installed as the
/// global tracer provider. Before returning, the captured resource is read
/// back through the probe and the `service.name` attribute is asserted to
/// equal `service_name`. Any mismatch panics — the alternative is shipping
/// spans whose Sentry tags silently group under the wrong service, which is
/// far worse than a hard failure at boot.
fn build_noop_traces_layer<S>(service_name: &'static str) -> OpenTelemetryLayer<S, Tracer>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    let probe = ResourceProbe::new();
    let provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::AlwaysOn)
        .with_resource(build_service_resource(service_name))
        .with_span_processor(probe.clone())
        .build();
    assert_service_name_resource(&probe, service_name);
    let _ = global::set_tracer_provider(provider.clone());
    let tracer = provider.tracer(service_name);
    tracing_opentelemetry::layer().with_tracer(tracer)
}

#[cfg(feature = "otel")]
/// Build the OTel `Resource` carrying the canonical `service.name`
/// attribute. Centralised so any future expansion to `service.version` /
/// `deployment.environment` lands here.
fn build_service_resource(service_name: &str) -> Resource {
    Resource::new(vec![KeyValue::new(
        "service.name",
        service_name.to_string(),
    )])
}

#[cfg(feature = "otel")]
/// Assert that the [`ResourceProbe`] captured a `Resource` whose
/// `service.name` attribute equals `expected`. Panics with a message
/// mentioning `service.name` on any failure mode (no resource captured,
/// missing key, wrong value).
fn assert_service_name_resource(probe: &ResourceProbe, expected: &str) {
    let resource = probe
        .captured()
        .expect("global TracerProvider Resource was never set during init — service.name missing");
    let value = resource
        .get(Key::from_static_str("service.name"))
        .expect("global TracerProvider Resource is missing the service.name attribute");
    let actual = value.as_str();
    assert_eq!(
        actual.as_ref(),
        expected,
        "service.name resource attribute mismatch: expected {expected:?}, got {actual:?}",
    );
}

#[cfg(feature = "otel")]
/// SpanProcessor whose only job is to record the `Resource` that the SDK
/// pushes into it via [`SpanProcessor::set_resource`] during
/// `TracerProvider::build`. Used by [`build_noop_traces_layer`] as a probe
/// to confirm that the constructed provider's resource carries
/// `service.name`.
///
/// `on_start` / `on_end` are intentionally no-ops — the probe MUST NOT
/// perturb production span flow.
#[derive(Default, Clone)]
struct ResourceProbe {
    captured: Arc<Mutex<Option<Resource>>>,
}

#[cfg(feature = "otel")]
impl ResourceProbe {
    fn new() -> Self {
        Self::default()
    }

    fn captured(&self) -> Option<Resource> {
        self.captured
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

#[cfg(feature = "otel")]
impl std::fmt::Debug for ResourceProbe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResourceProbe")
            .field("captured", &self.captured().is_some())
            .finish()
    }
}

#[cfg(feature = "otel")]
impl SpanProcessor for ResourceProbe {
    fn on_start(&self, _span: &mut SdkSpan, _cx: &Context) {}
    fn on_end(&self, _span: SpanData) {}
    fn force_flush(&self) -> TraceResult<()> {
        Ok(())
    }
    fn shutdown(&self) -> TraceResult<()> {
        Ok(())
    }
    fn set_resource(&mut self, resource: &Resource) {
        *self
            .captured
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(resource.clone());
    }
}

/// Path the node binary appends JSON events to.
///
/// Order of resolution:
/// 1. `$OBS_FILE_PATH` if set — used as-is, with no parent-directory
///    creation. The caller chose the path; the caller is responsible.
/// 2. `<node-home>/observability.jsonl` — the directory is created if absent.
///    The node home is resolved by [`folddb_profile::paths::folddb_home`], the
///    SAME honor-both resolver the data dir uses, so the log writer and the
///    data dir always agree:
///    `LASTDB_HOME` ▸ `FOLDDB_HOME` ▸ existing `~/.lastdb` ▸ existing
///    `~/.folddb` ▸ new `~/.lastdb`. On a brand-new install (no env override,
///    neither home present) this lands logs under `~/.lastdb` — NOT a stray
///    `~/.folddb`. An existing `~/.folddb` install keeps writing there (zero
///    movement). `~`-prefixed env values are expanded against `$HOME` by
///    `folddb_home`, so a literal `~/.folddb` never leaks a `<cwd>/~/`
///    directory.
///
/// Delegating to `folddb_home` is the single-source-of-truth that keeps this
/// writer from re-diverging from the data dir / the `folddb logs` reader
/// ([`fold_db_node::utils::paths::observability_log_path`]).
fn default_node_log_path() -> Result<PathBuf, ObsError> {
    if let Ok(p) = std::env::var(OBS_FILE_PATH_ENV) {
        return Ok(PathBuf::from(p));
    }
    let home = folddb_profile::paths::folddb_home().map_err(|e| {
        ObsError::Io(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "could not resolve node home for the observability log path \
                 ({e}); set OBS_FILE_PATH, LASTDB_HOME, or FOLDDB_HOME to \
                 choose a log path explicitly",
            ),
        ))
    })?;
    fs::create_dir_all(&home)?;
    Ok(home.join("observability.jsonl"))
}

fn install_subscriber<S>(subscriber: S) -> Result<(), ObsError>
where
    S: tracing::Subscriber + Send + Sync + 'static,
{
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|e| ObsError::SubscriberInstall(e.to_string()))
}

/// Install process-global OTel plumbing. Idempotent.
/// Called after `install_subscriber` succeeds.
/// No-op when the `otel` feature is disabled (Lambda light path).
fn install_globals() {
    #[cfg(feature = "otel")]
    global::set_text_map_propagator(TraceContextPropagator::new());
}

/// Wire the `log` → `tracing` bridge. Called BEFORE `set_global_default` so
/// any third-party `log::*` call between subscriber install and process exit
/// flows through tracing. Doing it first also means a `log::*!` emitted by
/// init code itself is captured rather than dropped.
///
/// `LogTracer::init` errors only when called twice in the same process;
/// that's expected for retries / multiple test cases and not actionable, so
/// we swallow the error.
fn install_log_tracer() {
    let _ = LogTracer::init();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
