//! W3C trace context propagation across HTTP boundaries.
//!
//! Two helpers cover the boundaries we control:
//!
//! - [`inject_w3c`] — wraps a `reqwest::RequestBuilder` and adds the
//!   `traceparent` (and any tracestate) headers derived from the *current*
//!   tracing span, so downstream services can stitch into the same trace.
//! - [`extract_parent_context`] — reads `traceparent` (and friends) from an
//!   `http::HeaderMap` on ingress and returns an `opentelemetry::Context`
//!   that callers attach to the server-side span via
//!   `tracing_opentelemetry::OpenTelemetrySpanExt::set_parent`.
//!
//! Both helpers depend on a global text-map propagator being installed.
//! The standard installation (done in the `init_*` helpers in T6) is
//! `opentelemetry_sdk::propagation::TraceContextPropagator`. Tests in this
//! module install one ad-hoc.
//!
//! AWS SDK egress is **not** covered — Lambdas talk to AWS services, which
//! use a different propagation format. This is intentionally out of scope:
//! AWS SDK calls are auth/billing/sync metadata, not on the user-facing
//! critical path. A lightweight `#[tracing::instrument]` on the Lambda
//! wrapper functions is the fallback if span coverage is needed.

use opentelemetry::global;
use opentelemetry::propagation::Injector;
use opentelemetry::Context;
use opentelemetry_http::HeaderExtractor;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Inject the current span's W3C trace context into a `reqwest` request.
///
/// Use at every outgoing HTTP call site that you want stitched into the
/// caller's trace. Per the plan's classification:
///
/// - `propagate` — wrap with `inject_w3c(builder)`.
/// - `loopback` — wrap (it's still our own service downstream).
/// - `skip-s3`, `skip-3p` — do **not** wrap; third-party services would
///   reject or ignore the headers.
pub fn inject_w3c(builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    let cx = Span::current().context();

    let mut injector = StringHeaderInjector::default();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&cx, &mut injector);
    });

    let mut builder = builder;
    for (key, value) in injector.headers {
        builder = builder.header(key, value);
    }
    builder
}

/// Extract a parent `Context` from incoming HTTP headers.
///
/// Server-side handlers call this on the request's `HeaderMap`, then attach
/// the result to their root span:
///
/// ```ignore
/// let parent = observability::propagation::extract_parent_context(req.headers());
/// let span = tracing::info_span!("http.request");
/// span.set_parent(parent);
/// ```
pub fn extract_parent_context(headers: &http::HeaderMap) -> Context {
    let extractor = HeaderExtractor(headers);
    global::get_text_map_propagator(|propagator| propagator.extract(&extractor))
}

/// `Injector` impl that buffers `(name, value)` pairs in a `Vec` so we can
/// apply them to a `reqwest::RequestBuilder` afterwards. This avoids the
/// `http` crate version coupling between `reqwest` (0.11 → http 0.2) and
/// `opentelemetry-http` (→ http 1.x).
#[derive(Default)]
struct StringHeaderInjector {
    headers: Vec<(String, String)>,
}

impl Injector for StringHeaderInjector {
    fn set(&mut self, key: &str, value: String) {
        self.headers.push((key.to_string(), value));
    }
}
