//! Advisory Schema Service Lambda handler.
//!
//! Serves `/v1/health` (`/health`), `/v1/fields/vocabulary`, and
//! `POST /v1/advice/schema` from the curated vocabulary and public registry catalog.

#![allow(clippy::result_large_err)]

mod catalog;
mod dispatch;
mod http;

// API Gateway sets Access-Control-Allow-Origin; the Lambda adds methods and headers.
pub(crate) const ACCESS_CONTROL_ALLOW_METHODS: &str = "GET, POST, OPTIONS";
pub(crate) const ACCESS_CONTROL_ALLOW_HEADERS: &str = "Content-Type";

use std::sync::Arc;

use lambda_http::{run, service_fn, Body, Error, Request, Response};
use schema_service_core::SchemaServiceState;

#[allow(
    clippy::unused_async,
    clippy::needless_pass_by_value,
    reason = "lambda_http::service_fn requires an async handler taking Request by value"
)]
async fn function_handler(
    state: Arc<SchemaServiceState>,
    catalog: Option<Arc<catalog::Catalog>>,
    event: Request,
) -> Result<Response<Body>, Error> {
    let method = event.method().as_str();
    let path = event.uri().path();

    tracing::info!("Request: {method} {path}");

    if let Some(resp) = dispatch::dispatch_stateless(method, path) {
        return resp;
    }

    if method == "POST" && path == catalog::FEED_PATH {
        return catalog::scheduled(catalog.as_deref(), &event).await;
    }

    dispatch::dispatch_with_state(&state, catalog.as_deref(), &event).await
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let state = Arc::new(
        SchemaServiceState::from_builtin()
            .map_err(|e| Error::from(format!("schema service init failed: {e}")))?,
    );

    if std::env::args().any(|arg| arg == "--print-vocabulary") {
        println!("{}", state.canonical_vocabulary());
        return Ok(());
    }

    let _obs = observability::init_lambda("schema_service", env!("CARGO_PKG_VERSION"))
        .map_err(|e| Error::from(format!("failed to install observability: {e}")))?;

    tracing::info!("Advisory Schema Service Lambda starting...");

    let catalog = match catalog::Catalog::from_environment() {
        Ok(catalog) => catalog.map(Arc::new),
        Err(reason) => {
            tracing::warn!(%reason, "schema_catalog_configuration_unavailable");
            None
        }
    };

    run(service_fn(move |event| {
        function_handler(Arc::clone(&state), catalog.clone(), event)
    }))
    .await
}
