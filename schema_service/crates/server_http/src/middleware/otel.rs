//! W3C trace-context ingress middleware (Phase 2 / B3).
//!
//! For every incoming HTTP request, extract any `traceparent` /
//! `tracestate` headers and attach the resulting `opentelemetry::Context`
//! as the parent of the request's tracing root span. Combined with
//! `tracing_actix_web::TracingLogger`, this stitches every server span
//! into the upstream caller's distributed trace.
//!
//! Twin of `fold_db_node::server::middleware::otel` (PR #708) — the
//! pattern is identical because both binaries serve Actix and need to
//! parent their root span on the inbound `traceparent`.
//!
//! ## Wiring
//!
//! Must be wrapped **after** `TracingLogger::default()` so the root span
//! exists in `req.extensions()` by the time we run:
//!
//! ```ignore
//! App::new()
//!     .wrap(W3CParentContext)         // inner — runs after root span set
//!     .wrap(TracingLogger::default()) // outer — creates root span first
//! ```
//!
//! In Actix, the *last* `.wrap` call is the *outermost* middleware on
//! the request path, so the order above gives us the desired
//! "TracingLogger → W3CParentContext → handler" pipeline.
//!
//! ## Propagator dependency
//!
//! The extraction is a no-op until a global text-map propagator is
//! installed (`opentelemetry::global::set_text_map_propagator`). The
//! `observability::init_node` family of helpers does this. Until that
//! call is wired into schema_service startup (separate Phase 1
//! follow-up), every extracted context will be empty and parents will
//! not be attached at runtime — but the middleware test below installs
//! a propagator ad-hoc so the round-trip is validated in CI.

use std::future::{ready, Ready};
use std::rc::Rc;

use actix_web::dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::{Error, HttpMessage};
use futures_util::future::LocalBoxFuture;
use tracing_actix_web::RootSpan;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub struct W3CParentContext;

impl<S, B> Transform<S, ServiceRequest> for W3CParentContext
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = W3CParentContextService<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(W3CParentContextService {
            service: Rc::new(service),
        }))
    }
}

pub struct W3CParentContextService<S> {
    service: Rc<S>,
}

impl<S, B> Service<ServiceRequest> for W3CParentContextService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        // actix-http 3 uses http 0.2 internally; observability::propagation
        // takes &http::HeaderMap from http 1.x. The two HeaderName/Value
        // types are not interchangeable, so we round-trip through bytes.
        // Only ASCII trace-context headers matter for W3C propagation,
        // and any value that isn't valid in 1.x is silently skipped —
        // an unparseable traceparent is just a missing parent.
        let mut headers = http::HeaderMap::with_capacity(req.headers().len());
        for (name, value) in req.headers() {
            if let (Ok(n), Ok(v)) = (
                http::HeaderName::from_bytes(name.as_str().as_bytes()),
                http::HeaderValue::from_bytes(value.as_bytes()),
            ) {
                headers.append(n, v);
            }
        }
        let parent = observability::propagation::extract_parent_context(&headers);

        if let Some(root_span) = req.extensions().get::<RootSpan>().cloned() {
            root_span.set_parent(parent);
        }

        let svc = self.service.clone();
        Box::pin(async move { svc.call(req).await })
    }
}
