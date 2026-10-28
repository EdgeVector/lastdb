//! Standard vocabulary lookup and operator publication.
use crate::http::*;
use lambda_http::{Body, Error, Request, Response};
use schema_service_server_shared::state::SchemaServiceState;
use serde_json::{json, Value};

pub(crate) async fn publish(
    state: &SchemaServiceState,
    event: &Request,
) -> Result<Response<Body>, Error> {
    let body = match parse_body(event) {
        Ok(body) => body,
        Err(response) => return Ok(response),
    };
    let value: Value = match serde_json::from_str(&body) {
        Ok(value) => value,
        Err(error) => {
            return json_response(
                400,
                &json!({"reason": "invalid_body", "detail": error.to_string()}),
            )
        }
    };
    match state
        .publish_canonical_vocabulary(header_str(event, "x-canonical-signature"), &value)
        .await
    {
        Ok(count) => json_response(200, &json!({"approved_fields": count})),
        Err(error) => json_response(
            error.status,
            &json!({"reason": error.reason, "detail": error.detail}),
        ),
    }
}

pub(crate) fn get(state: &SchemaServiceState) -> Result<Response<Body>, Error> {
    match state.canonical_vocabulary() {
        Ok(value) => json_response(200, &value),
        Err(error) => json_response(500, &json!({"error": error.to_string()})),
    }
}
