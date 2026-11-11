//! Method + path routing for the advisory Schema Service Lambda.

use crate::http::{json_response, typed_json_body};
use lambda_http::{Body, Error, Request, Response};
use schema_service_core::advice::SchemaAdviceRequest;
use schema_service_core::SchemaServiceState;
use serde_json::json;

type RouteResult = Option<Result<Response<Body>, Error>>;

/// Dispatch stateless routes (health, root) without reading vocabulary state.
pub(crate) fn dispatch_stateless(method: &str, path: &str) -> RouteResult {
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
                    "POST /v1/advice/schema": "Advice for one draft schema declaration (read-only, never blocks)"
                }
            }),
        )),
        _ => None,
    }
}

/// Dispatch advisory routes against the initialized canonical vocabulary state.
pub(crate) fn dispatch_with_state(
    state: &SchemaServiceState,
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
            match state.schema_advice_reply(&request).into_http() {
                Ok(response) => json_response(200, &json!(response)),
                Err((status, body)) => json_response(status, &body),
            }
        }
        _ => json_response(
            404,
            &schema_service_core::error_only_body(&format!("Not found: {method} {path}")),
        ),
    }
}
