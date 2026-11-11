use super::*;

#[inline]
pub(super) fn assert_service_name(name: &str) {
    assert!(
        !name.trim().is_empty(),
        "service.name must be non-empty (got {name:?})",
    );
}

pub(super) fn try_claim_init(cell: &OnceCell<()>) -> Result<(), ObsError> {
    cell.set(()).map_err(|_| ObsError::AlreadyInitialized)
}

pub(super) fn default_env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
}

#[cfg(feature = "otel")]
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
pub(super) fn build_service_resource(service_name: &str) -> Resource {
    Resource::new(vec![KeyValue::new(
        "service.name",
        service_name.to_string(),
    )])
}

#[cfg(feature = "otel")]
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

pub(super) fn install_subscriber<S>(subscriber: S) -> Result<(), ObsError>
where
    S: tracing::Subscriber + Send + Sync + 'static,
{
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|e| ObsError::SubscriberInstall(e.to_string()))
}

pub(super) fn install_globals() {
    #[cfg(feature = "otel")]
    global::set_text_map_propagator(TraceContextPropagator::new());
}

pub(super) fn install_log_tracer() {
    let _ = LogTracer::init();
}
