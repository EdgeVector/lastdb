//! Advisory Schema Service Lambda handler.
//!
//! Serves `/v1/health` (`/health`), `/v1/fields/vocabulary`,
//! `POST /v1/advice/schema`, and `POST /v1/schemas`. Advice reads the curated
//! vocabulary and the schemas publishers push.

#![allow(clippy::result_large_err)]

mod catalog;

use std::sync::Arc;

use lambda_http::{run, service_fn, Body, Error, Request, Response};
use schema_service_core::advice::SchemaAdviceRequest;
use schema_service_core::SchemaServiceState;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

const ACCESS_CONTROL_ALLOW_METHODS: &str = "GET, POST, OPTIONS";
const ACCESS_CONTROL_ALLOW_HEADERS: &str = "Content-Type";
const MAX_REQUEST_BODY_BYTES: usize = 128 * 1024;

pub(crate) fn json_response(status: u16, body: &Value) -> Result<Response<Body>, Error> {
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .header("Access-Control-Allow-Methods", ACCESS_CONTROL_ALLOW_METHODS)
        .header("Access-Control-Allow-Headers", ACCESS_CONTROL_ALLOW_HEADERS)
        .body(Body::from(body.to_string()))
        .map_err(|e| Error::from(format!("Failed to build response: {e}")))
}

fn json_error(status: u16, message: &str) -> Result<Response<Body>, Error> {
    json_response(status, &json!({ "error": message }))
}

fn payload_too_large() -> Result<Response<Body>, Error> {
    json_error(413, "Request body exceeds 128 KiB limit")
}

fn typed_json_body<T: DeserializeOwned>(
    event: &Request,
    error: &str,
) -> Result<T, Result<Response<Body>, Error>> {
    let parsed = match event.body() {
        Body::Text(s) => {
            if s.len() > MAX_REQUEST_BODY_BYTES {
                return Err(payload_too_large());
            }
            serde_json::from_str(s)
        }
        Body::Binary(b) => {
            if b.len() > MAX_REQUEST_BODY_BYTES {
                return Err(payload_too_large());
            }
            serde_json::from_slice(b)
        }
        Body::Empty => return Err(json_error(400, "Request body is empty")),
    };
    parsed.map_err(|parse_error| json_error(400, &format!("{error}: {parse_error}")))
}

fn dispatch_stateless(method: &str, path: &str) -> Option<Result<Response<Body>, Error>> {
    match (method, path) {
        ("GET", "/health" | "/v1/health") => Some(json_response(
            200,
            &json!({
                "status": "healthy",
                "service": "schema-service"
            }),
        )),
        ("GET" | "POST", "/") => Some(json_response(
            200,
            &json!({
                "service": "LastDB Advisory Schema Service",
                "version": "3.0.0",
                "endpoints": {
                    "GET /health": "Health check (unversioned alias)",
                    "GET /v1/health": "Health check",
                    "GET /v1/fields/vocabulary": "Read the standard canonical vocabulary",
                    "POST /v1/advice/schema": "Advice for one draft schema declaration (read-only, never blocks)",
                    "POST /v1/schemas": "Register one listed app's schema definitions for one channel"
                }
            }),
        )),
        _ => None,
    }
}

async fn dispatch_with_state(
    state: &SchemaServiceState,
    catalog: Option<&catalog::Catalog>,
    event: &Request,
) -> Result<Response<Body>, Error> {
    let method = event.method().as_str();
    let path = event.uri().path();
    match (method, path) {
        ("GET", "/v1/fields/vocabulary") => json_response(200, state.canonical_vocabulary()),
        ("POST", "/v1/advice/schema") => {
            let request: SchemaAdviceRequest =
                match typed_json_body(event, "Invalid advice request") {
                    Ok(request) => request,
                    Err(response) => return response,
                };
            match state.schema_advice(&request) {
                Ok(mut response) => {
                    let status = match catalog {
                        Some(catalog) => catalog.examples(&request, &mut response).await,
                        None => catalog::unavailable(),
                    };
                    json_response(
                        200,
                        &json!({"findings": response.findings, "catalog": status}),
                    )
                }
                Err(message) => json_error(400, &message),
            }
        }
        _ => json_error(404, &format!("Not found: {method} {path}")),
    }
}

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

    if let Some(resp) = dispatch_stateless(method, path) {
        return resp;
    }

    if method == "POST" && path == catalog::SCHEMAS_PATH {
        return catalog::register_schemas(catalog.as_deref(), &event).await;
    }

    dispatch_with_state(&state, catalog.as_deref(), &event).await
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
