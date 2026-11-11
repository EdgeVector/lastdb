//! Initialization helper for AWS Lambda handlers.

#[cfg(feature = "otel")]
use std::sync::{Arc, Mutex};

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
use crate::layers::fmt::{build_stdout_writer, FmtGuard, RedactingFormat};
use crate::ObsError;

static INIT_ONCE: OnceCell<()> = OnceCell::new();

/// RAII handle returned by [`init_lambda`].
#[must_use = "ObsGuard must be held for the lifetime of the binary or log lines may be dropped"]
pub struct ObsGuard {
    fmt_guard: Option<FmtGuard>,
    sentry_guard: Option<SentryGuard>,
}

impl std::fmt::Debug for ObsGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObsGuard")
            .field("fmt_guard", &self.fmt_guard.is_some())
            .field("sentry_guard", &self.sentry_guard.is_some())
            .finish()
    }
}

impl Drop for ObsGuard {
    fn drop(&mut self) {
        let _ = self.sentry_guard.take();
    }
}

/// Initialize observability for an AWS Lambda handler.
pub fn init_lambda(service_name: &'static str, _version: &str) -> Result<ObsGuard, ObsError> {
    assert_service_name(service_name);
    try_claim_init(&INIT_ONCE)?;
    install_log_tracer();

    let (writer, fmt_guard) = build_stdout_writer();
    let env_filter = default_env_filter();

    let (error_layer, sentry_guard) = match build_error_layer() {
        Some((layer, guard)) => (Some(layer), Some(guard)),
        None => (None, None),
    };

    let fmt_layer = tracing_subscriber::fmt::layer()
        .event_format(RedactingFormat::from_env())
        .with_writer(writer);

    #[cfg(feature = "otel")]
    {
        let otel_layer = build_noop_traces_layer(service_name);
        let subscriber = Registry::default()
            .with(env_filter)
            .with(otel_layer)
            .with(fmt_layer)
            .with(error_layer);
        install_subscriber(subscriber)?;
    }
    #[cfg(not(feature = "otel"))]
    {
        let subscriber = Registry::default()
            .with(env_filter)
            .with(fmt_layer)
            .with(error_layer);
        install_subscriber(subscriber)?;
    }
    install_globals();

    Ok(ObsGuard {
        fmt_guard: Some(fmt_guard),
        sentry_guard,
    })
}

mod support;
use support::*;
