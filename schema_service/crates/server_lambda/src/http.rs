//! Request parsing and JSON response helpers for the advisory Lambda.

use crate::cors;
use lambda_http::{Body, Error, Request, Response};
use serde::de::DeserializeOwned;
use serde_json::Value;

pub(crate) type HandlerResult<T> = Result<T, Result<Response<Body>, Error>>;

pub(crate) fn cors_builder(status: u16, content_type: &str) -> http::response::Builder {
    Response::builder()
        .status(status)
        .header("Content-Type", content_type)
        .header(
            "Access-Control-Allow-Methods",
            cors::ACCESS_CONTROL_ALLOW_METHODS,
        )
        .header(
            "Access-Control-Allow-Headers",
            cors::ACCESS_CONTROL_ALLOW_HEADERS,
        )
}

pub(crate) fn json_response(status: u16, body: &Value) -> Result<Response<Body>, Error> {
    cors_builder(status, "application/json")
        .body(Body::from(body.to_string()))
        .map_err(|e| Error::from(format!("Failed to build response: {e}")))
}

pub(crate) fn typed_json_body<T>(event: &Request, error: &str) -> HandlerResult<T>
where
    T: DeserializeOwned,
{
    let raw = match event.body() {
        Body::Text(s) => s.clone(),
        Body::Binary(b) => String::from_utf8_lossy(b).into_owned(),
        Body::Empty => {
            return Err(json_response(
                400,
                &schema_service_core::error_only_body("Request body is empty"),
            ));
        }
    };
    serde_json::from_str(&raw).map_err(|parse_error| {
        json_response(
            400,
            &schema_service_core::error_only_body(&format!("{error}: {parse_error}")),
        )
    })
}
