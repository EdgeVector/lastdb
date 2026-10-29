use super::*;

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
pub(super) fn assert_service_name(name: &str) {
    assert!(
        !name.trim().is_empty(),
        "service.name must be non-empty (got {name:?})",
    );
}

/// Atomically claim the one-shot init slot. Returns
/// [`ObsError::AlreadyInitialized`] when another caller already set it.
pub(super) fn try_claim_init(cell: &OnceCell<()>) -> Result<(), ObsError> {
    cell.set(()).map_err(|_| ObsError::AlreadyInitialized)
}

/// Stamp the verified `service_name` onto the process-global cache. Called
/// at the tail of every successful `init_*` (after subscriber install +
/// Resource verification). Subsequent calls — including legitimate ones from
/// nested helpers like nested `init_*` → [`init_node`] — silently no-op via
/// `OnceCell::set`'s "already set" semantics, which is the right behaviour:
/// `INIT_ONCE` already gates double-init.
pub(super) fn record_service_name(service_name: &'static str) {
    let _ = SERVICE_NAME.set(service_name);
}

pub(super) fn default_env_filter() -> EnvFilter {
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
pub(super) fn build_noop_traces_layer<S>(
    service_name: &'static str,
) -> OpenTelemetryLayer<S, Tracer>
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
pub(super) fn build_service_resource(service_name: &str) -> Resource {
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
pub(super) fn assert_service_name_resource(probe: &ResourceProbe, expected: &str) {
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
pub(super) struct ResourceProbe {
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
pub(super) fn default_node_log_path() -> Result<PathBuf, ObsError> {
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

pub(super) fn install_subscriber<S>(subscriber: S) -> Result<(), ObsError>
where
    S: tracing::Subscriber + Send + Sync + 'static,
{
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|e| ObsError::SubscriberInstall(e.to_string()))
}

/// Install process-global OTel plumbing. Idempotent.
/// Called after `install_subscriber` succeeds.
/// No-op when the `otel` feature is disabled (Lambda light path).
pub(super) fn install_globals() {
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
pub(super) fn install_log_tracer() {
    let _ = LogTracer::init();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
