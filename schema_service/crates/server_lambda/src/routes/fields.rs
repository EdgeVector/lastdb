//! Canonical field routes (`/v1/fields/*`).

use crate::http::*;
use lambda_http::{Body, Error, Request, Response};
use schema_service_core::{DeclareFieldReply, DeclaredFieldReadReply};
use schema_service_server_shared::state::SchemaServiceState;
use serde_json::{json, Value};

// Declared fields (brain `design-lastdb-declared-fields`).
//
// These MUST stay in lockstep with the actix table in
// `server_http::configure_routes`. The Lambda has its own router, so a
// route added only to actix is present in every local/dev actix run and
// silently 404s in dev and prod — which is exactly what happened on the
// first deploy of this feature. Use a real round trip for product proof.
//
// The literal `/v1/fields/declare` arm must precede the `/v1/fields/`
// prefix arm below, or a declare would be routed as a lookup.
pub(crate) async fn post_fields_declare(
    state: &SchemaServiceState,
    event: &Request,
) -> Result<Response<Body>, Error> {
    let body = match parse_body(event) {
        Ok(b) => b,
        Err(r) => return Ok(r),
    };
    let body_value: Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => {
            return json_response(
                400,
                &json!({
                    "reason": "invalid_body",
                    "detail": format!("invalid JSON: {e}"),
                }),
            );
        }
    };
    let request: schema_service_core::DeclareFieldRequest =
        match serde_json::from_value(body_value.clone()) {
            Ok(parsed) => parsed,
            Err(e) => {
                return json_response(
                    400,
                    &json!({
                        "reason": "invalid_body",
                        "detail": format!(
                            "expected {{ owner_app_id, handle, fields[] }}: {e}"
                        ),
                    }),
                );
            }
        };

    // The signed payload is the whole request body, so owner, handle,
    // and field list are all covered by the signature.
    let cert = header_str(event, "x-exemem-dev-cert");
    let sig = header_str(event, "x-signature");
    match state
        .declare_field_reply(&request, cert, sig, &body_value)
        .await
    {
        DeclareFieldReply::Ready { status, body } | DeclareFieldReply::Failed { status, body } => {
            json_response(status, &body)
        }
    }
}

// Public lookup by id: the id is not a secret, and a node needs to
// resolve a declaration it already holds. What is gated is declaring
// and referencing, never looking up.
pub(crate) fn get_fields(state: &SchemaServiceState, p: &str) -> Result<Response<Body>, Error> {
    let declaration_id = p.trim_start_matches("/v1/fields/").trim_end_matches('/');
    match state.declared_field_read_reply(declaration_id) {
        DeclaredFieldReadReply::Ready { status, body }
        | DeclaredFieldReadReply::Failed { status, body } => json_response(status, &body),
    }
}
