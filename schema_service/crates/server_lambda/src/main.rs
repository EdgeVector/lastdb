//! Schema Service Lambda handler.
//!
//! Ported from `schema-infra/lambdas/schema_service/` into the new
//! `schema_service` repo as part of Phase 1 (see
//! gbrain slug `projects/phase-1-absorb-lambda`). The handler wraps
//! `SchemaServiceState` as an HTTP API via API Gateway.
//!
//! Storage: S3 blobs. The schema service state is backed by the
//! `S3BlobPersistence` implementation of `ExternalSchemaPersistence`,
//! which stores schemas, canonical_fields, and views in a single bucket.
//!
//! Routes: all endpoints are mounted under `/v1/*` to match the actix
//! wrapper scaffolded in Phase 0. `/health` is also served unversioned
//! as a belt-and-braces alias during deploy transitions.
//!
//! `POST /v1/system/reset` remains actix-only (dev reset; see comment in
//! the main dispatch).
//!
//! Layout: `dispatch` matches method + path, `routes` holds the stateful
//! handlers, `http` holds request/response helpers, `snapshot` holds the
//! API-key-gated export routes, and `state_init` builds the cold-start state.

// The `lambda_http::Error` type wraps a boxed error that Clippy's
// result_large_err lint flags. The whole Lambda uses this error
// consistently; rewriting every handler to box-on-return would be
// substantial churn without behavior benefit.
#![allow(clippy::result_large_err)]

mod api_key;
mod cors;
mod dispatch;
mod http;
mod routes;
mod snapshot;
mod state_init;

use lambda_http::{run, service_fn};
use lambda_http::{Body, Error, Request, Response};

async fn function_handler(event: Request) -> Result<Response<Body>, Error> {
    let method = event.method().as_str();
    let path = event.uri().path();

    tracing::info!("Request: {} {}", method, path);

    // Stateless routes (health, root) bypass cold-start init so health
    // probes stay fast and don't force full state hydration.
    if let Some(resp) = dispatch::dispatch_stateless(method, path) {
        return resp;
    }

    let state = state_init::get_or_init_state().await?;
    dispatch::dispatch_with_state(state.as_ref(), event).await
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Install the shared observability stack: redacting JSON FMT to stdout
    // (captured into CloudWatch by the Lambda runtime) plus the env-gated
    // Sentry ERROR layer. Sentry is additive and a no-op when OBS_SENTRY_DSN
    // is unset, so CloudWatch JSON logging is unchanged where it isn't
    // configured. The guard is held for the whole `main` body — including
    // the long cold-start init below and the `run(...)` await — so the
    // Sentry flush + FMT worker survive until the process exits.
    let _obs = observability::init_lambda("schema_service", env!("CARGO_PKG_VERSION"))
        .map_err(|e| Error::from(format!("failed to install observability: {e}")))?;

    tracing::info!("Schema service Lambda starting...");

    #[cfg(feature = "fastembed")]
    {
        // Point fastembed at the bundled model cache and refuse to hit
        // HuggingFace. AWS Lambda mounts the fastembed Layer at /opt/.
        if std::env::var_os("FASTEMBED_CACHE_DIR").is_none() {
            std::env::set_var("FASTEMBED_CACHE_DIR", "/opt/fastembed_cache");
        }
        if std::env::var_os("HF_HUB_OFFLINE").is_none() {
            std::env::set_var("HF_HUB_OFFLINE", "1");
        }
    }

    // Do the expensive state initialization (S3 hydration + built-in
    // seeding) during Lambda init, not on the first handler invocation.
    // Lambda's init phase is NOT bounded by API Gateway's 29s timeout.
    if let Err(e) = state_init::get_or_init_state().await {
        tracing::error!("Schema service init failed during Lambda init phase: {e}");
        return Err(e);
    }
    tracing::info!("Schema service init complete during Lambda init phase");

    run(service_fn(function_handler)).await
}
