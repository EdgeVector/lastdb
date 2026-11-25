//! Advisory Schema Service Lambda handler.
//!
//! Serves `/v1/health` (`/health`), `/v1/fields/vocabulary`,
//! `POST /v1/advice/schema`, `POST /v1/schemas`, and
//! `GET /v1/schemas/<definition_id>`. Advice reads the curated vocabulary and
//! the schemas publishers push. A definition read returns the stored object.

mod catalog;

use std::sync::Arc;

use http::{header, HeaderValue, StatusCode};
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

pub(crate) fn json_bytes_response(
    status: u16,
    body: Vec<u8>,
    cache_control: &str,
) -> Result<Response<Body>, Error> {
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .header("Cache-Control", cache_control)
        .header("Access-Control-Allow-Methods", ACCESS_CONTROL_ALLOW_METHODS)
        .header("Access-Control-Allow-Headers", ACCESS_CONTROL_ALLOW_HEADERS)
        .body(Body::Binary(body))
        .map_err(|e| Error::from(format!("Failed to build response: {e}")))
}

fn json_error(status: u16, message: &str) -> Result<Response<Body>, Error> {
    json_response(status, &json!({ "error": message }))
}

fn bad_body_error(status: StatusCode, message: &str) -> Box<Response<Body>> {
    let mut response = Response::new(Body::from(json!({ "error": message }).to_string()));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static(ACCESS_CONTROL_ALLOW_METHODS),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static(ACCESS_CONTROL_ALLOW_HEADERS),
    );
    Box::new(response)
}

fn typed_json_body<T: DeserializeOwned>(
    event: &Request,
    error: &str,
) -> Result<T, Box<Response<Body>>> {
    let parsed = match event.body() {
        Body::Text(s) => {
            if s.len() > MAX_REQUEST_BODY_BYTES {
                return Err(bad_body_error(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "Request body exceeds 128 KiB limit",
                ));
            }
            serde_json::from_str(s)
        }
        Body::Binary(b) => {
            if b.len() > MAX_REQUEST_BODY_BYTES {
                return Err(bad_body_error(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "Request body exceeds 128 KiB limit",
                ));
            }
            serde_json::from_slice(b)
        }
        Body::Empty => {
            return Err(bad_body_error(
                StatusCode::BAD_REQUEST,
                "Request body is empty",
            ));
        }
    };
    parsed.map_err(|parse_error| {
        bad_body_error(StatusCode::BAD_REQUEST, &format!("{error}: {parse_error}"))
    })
}

fn is_known_route(path: &str) -> bool {
    matches!(
        path,
        "/" | "/health"
            | "/v1/health"
            | "/v1/fields/vocabulary"
            | "/v1/advice/schema"
            | catalog::SCHEMAS_PATH
    ) || catalog::definition_id(path).is_some()
}

fn preflight_response() -> Result<Response<Body>, Error> {
    Response::builder()
        .status(204)
        .header("Access-Control-Allow-Methods", ACCESS_CONTROL_ALLOW_METHODS)
        .header("Access-Control-Allow-Headers", ACCESS_CONTROL_ALLOW_HEADERS)
        .body(Body::Empty)
        .map_err(|e| Error::from(format!("Failed to build response: {e}")))
}

fn dispatch_stateless(method: &str, path: &str) -> Option<Result<Response<Body>, Error>> {
    match (method, path) {
        ("OPTIONS", _) if is_known_route(path) => Some(preflight_response()),
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
                    "POST /v1/schemas": "Register one listed app's schema definitions for one channel",
                    "GET /v1/schemas/{definition_id}": "Read one schema definition by its definition id"
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
                    Err(response) => return Ok(*response),
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

async fn function_handler(
    state: &SchemaServiceState,
    catalog: Option<&catalog::Catalog>,
    event: &Request,
) -> Result<Response<Body>, Error> {
    let method = event.method().as_str();
    let path = event.uri().path();

    tracing::info!("Request: {method} {path}");

    if let Some(resp) = dispatch_stateless(method, path) {
        return resp;
    }

    if method == "POST" && path == catalog::SCHEMAS_PATH {
        return catalog::register_schemas(catalog, event).await;
    }
    if let ("GET", Some(id)) = (method, catalog::definition_id(path)) {
        return catalog::read_definition(catalog, id).await;
    }

    dispatch_with_state(state, catalog, event).await
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
        let state = Arc::clone(&state);
        let catalog = catalog.clone();
        async move { function_handler(&state, catalog.as_deref(), &event).await }
    }))
    .await
}
