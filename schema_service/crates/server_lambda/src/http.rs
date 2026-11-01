//! Request parsing and response building helpers shared by every route.

use crate::cors;
use lambda_http::{Body, Error, Request, Response};
use schema_service_core::state::SchemaByNameReply;
use schema_service_core::{
    missing_dev_cert_body, reason_detail_body, SchemaMutationGateHeaders, HEADER_DEV_PUBKEY,
    HEADER_NODE_PUBLIC_KEY, HEADER_NODE_SIGNATURE, HEADER_POW_CHALLENGE, HEADER_POW_CHALLENGE_MAC,
    HEADER_POW_COUNTER, HEADER_POW_DIFFICULTY_BITS, HEADER_POW_EXPIRES_AT, HEADER_POW_NONCE,
};
use schema_service_server_shared::state::SchemaServiceState;
use serde_json::{json, Value};

/// Pull the `X-API-Key` header from a request. Header lookup is
/// case-insensitive in `http::HeaderMap`, so requests that send
/// `x-api-key` resolve identically.
pub(crate) fn extract_api_key(event: &Request) -> Option<&str> {
    header_str(event, "x-api-key")
}

/// Pull an arbitrary header as a trimmed, non-empty string. Header lookup
/// is case-insensitive. Used for the app-identity `X-Exemem-Dev-Cert` and
/// `X-Signature` envelopes.
pub(crate) fn header_str<'a>(event: &'a Request, name: &str) -> Option<&'a str> {
    event
        .headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

pub(crate) fn request_ip(event: &Request) -> Option<String> {
    header_str(event, "x-forwarded-for")
        .map(str::to_string)
        .or_else(|| {
            event
                .extensions()
                .get::<lambda_http::request::RequestContext>()
                .and_then(|ctx| match ctx {
                    lambda_http::request::RequestContext::ApiGatewayV2(ctx) => {
                        ctx.http.source_ip.clone()
                    }
                    _ => None,
                })
        })
}

pub(crate) fn schema_mutation_gate_headers(event: &Request) -> SchemaMutationGateHeaders {
    let owned = |name| header_str(event, name).map(str::to_string);
    SchemaMutationGateHeaders {
        node_public_key: owned(HEADER_NODE_PUBLIC_KEY),
        node_signature: owned(HEADER_NODE_SIGNATURE),
        challenge_id: owned(HEADER_POW_CHALLENGE),
        nonce: owned(HEADER_POW_NONCE),
        challenge_mac: owned(HEADER_POW_CHALLENGE_MAC),
        difficulty_bits: owned(HEADER_POW_DIFFICULTY_BITS),
        expires_at_unix_secs: owned(HEADER_POW_EXPIRES_AT),
        counter: owned(HEADER_POW_COUNTER),
        dev_pubkey: owned(HEADER_DEV_PUBKEY),
    }
}

/// Standard 401 for missing/invalid keys.
pub(crate) fn unauthorized(reason: &str) -> Result<Response<Body>, Error> {
    json_response(
        401,
        &json!({
            "error": "Unauthorized",
            "detail": reason,
        }),
    )
}

/// Returns an `http::response::Builder` pre-loaded with the response status,
/// Content-Type, and CORS preflight headers shared by every endpoint. Callers
/// chain `.body(...)` (and any extra headers like `Cache-Control`) on top.
///
/// The deployed HTTP API Gateway owns Access-Control-Allow-Origin from its
/// explicit allowlist; Lambda responses avoid duplicating that policy.
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

/// Map a `serde_json` serialization failure onto the `Error` the Lambda
/// runtime surfaces (a 500). Collapses the
/// `|e| Error::from(format!("Serialization error: {}", e))` closure
/// repeated at every handler that serializes a response body.
#[allow(
    clippy::needless_pass_by_value,
    reason = "used as `.map_err(serialization_error)` callback — map_err passes the error by value"
)]
pub(crate) fn serialization_error(e: serde_json::Error) -> Error {
    Error::from(format!("Serialization error: {e}"))
}

pub(crate) fn json_response(status: u16, body: &Value) -> Result<Response<Body>, Error> {
    cors_builder(status, "application/json")
        .body(Body::from(body.to_string()))
        .map_err(|e| Error::from(format!("Failed to build response: {e}")))
}

pub(crate) fn json_response_cacheable(
    status: u16,
    body: &Value,
    etag: &str,
) -> Result<Response<Body>, Error> {
    cors_builder(status, "application/json")
        .header("Cache-Control", "public, max-age=60")
        .header("ETag", etag)
        .body(Body::from(body.to_string()))
        .map_err(|e| Error::from(format!("Failed to build response: {e}")))
}

/// Extract a single query-string parameter by key. Pass the key with its
/// trailing `=` (e.g. `"source="`) to strip the prefix in one step.
/// Returns `None` when the parameter is absent.
pub(crate) fn query_param<'a>(event: &'a Request, key_eq: &str) -> Option<&'a str> {
    event
        .uri()
        .query()
        .and_then(|q| q.split('&').find_map(|p| p.strip_prefix(key_eq)))
}

/// Parse the `threshold` query-string parameter, defaulting to 0.5 when
/// absent or unparseable. Shared by `/schemas/similar/{name}` and
/// `/transforms/similar/{name}`.
pub(crate) fn parse_threshold(event: &Request) -> f64 {
    query_param(event, "threshold=")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.5)
}

pub(crate) fn parse_body(event: &Request) -> Result<String, Response<Body>> {
    match event.body() {
        Body::Text(s) => Ok(s.clone()),
        Body::Binary(b) => Ok(String::from_utf8_lossy(b).into_owned()),
        Body::Empty => Err(json_response(
            400,
            &schema_service_core::error_only_body("Request body is empty"),
        )
        .unwrap()),
    }
}

// ─── `/v2` route helpers ──────────────────────────────────────────────────
//
// A signed write reads its JSON body and DevCert pair in one step
// (`signed_json_body`). A path split stays its own step.

/// The parsed JSON body, or the 400 to return.
pub(crate) fn v2_body(event: &Request) -> Result<Value, Result<Response<Body>, Error>> {
    json_body(event, "invalid_manifest")
}

/// The parsed JSON body for an app register or update, or the 400 to return.
/// The reason is `invalid_metadata`. A release body uses `invalid_manifest`.
pub(crate) fn app_metadata_body(event: &Request) -> Result<Value, Result<Response<Body>, Error>> {
    json_body(event, "invalid_metadata")
}

/// The parsed JSON value, or the 400 to return.
///
/// `reason` is the body `reason`. A release uses `invalid_manifest`. An app
/// write uses `invalid_metadata`. A field declare uses `invalid_body`.
pub(crate) fn json_body(
    event: &Request,
    reason: &'static str,
) -> Result<Value, Result<Response<Body>, Error>> {
    let raw = match parse_body(event) {
        Ok(body) => body,
        Err(response) => return Err(Ok(response)),
    };
    serde_json::from_str(&raw).map_err(|error| {
        json_response(
            400,
            &reason_detail_body(reason, &format!("invalid JSON: {error}")),
        )
    })
}

/// The `X-Exemem-Dev-Cert` + `X-Signature` pair, or the 401 to return.
/// An app write needs both. A read needs neither. The 401 body is
/// [`missing_dev_cert_body`], which the actix router also writes.
pub(crate) fn v2_cert_headers(
    event: &Request,
) -> Result<(&str, &str), Result<Response<Body>, Error>> {
    match (
        header_str(event, "x-exemem-dev-cert"),
        header_str(event, "x-signature"),
    ) {
        (Some(cert), Some(sig)) => Ok((cert, sig)),
        _ => Err(json_response(401, &missing_dev_cert_body())),
    }
}

pub(crate) type HandlerResult<T> = Result<T, Result<Response<Body>, Error>>;
pub(crate) type BodyParser = fn(&Request) -> HandlerResult<Value>;

/// The JSON body and the DevCert header pair, or the response to return.
///
/// `parse` selects the 400 reason. The body is read first, then the cert.
/// A missing cert still returns 401.
pub(crate) fn signed_json_body(
    event: &Request,
    parse: BodyParser,
) -> HandlerResult<(Value, &str, &str)> {
    let body = parse(event)?;
    let (cert, sig) = v2_cert_headers(event)?;
    Ok((body, cert, sig))
}

/// A path segment that must be exactly one id — non-empty and with no
/// embedded slash, so a crafted path cannot smuggle a second segment in.
/// A trailing slash is ignored.
pub(crate) fn v2_app_id(segment: &str) -> Option<&str> {
    one_segment(segment.trim_end_matches('/'))
}

/// The one segment between `prefix` and `suffix`.
///
/// A slash left after `suffix` is removed is ignored, same as [`v2_app_id`].
pub(crate) fn app_id_before<'a>(path: &'a str, prefix: &str, suffix: &str) -> Option<&'a str> {
    v2_app_id(path.trim_start_matches(prefix).trim_end_matches(suffix))
}

/// The one segment between `prefix` and `suffix`, with no extra slash trim.
///
/// A promote path that ends in `/promote` must keep a slash that the suffix
/// did not remove. [`app_id_before`] would accept that path.
pub(crate) fn segment_before<'a>(path: &'a str, prefix: &str, suffix: &str) -> Option<&'a str> {
    one_segment(path.trim_start_matches(prefix).trim_end_matches(suffix))
}

fn one_segment(segment: &str) -> Option<&str> {
    (!segment.is_empty() && !segment.contains('/')).then_some(segment)
}

/// Split `/v2/apps/{app_id}/channels/{channel}` into its two ids.
pub(crate) fn v2_channel_path(path: &str) -> Option<(&str, &str)> {
    let rest = path.strip_prefix("/v2/apps/")?;
    let (app_id, channel) = rest.split_once("/channels/")?;
    let channel = channel.trim_end_matches('/');
    if app_id.is_empty() || app_id.contains('/') || channel.is_empty() || channel.contains('/') {
        return None;
    }
    Some((app_id, channel))
}

/// `GET /v1/schema/{name}` body: the schema envelope, 404, or 500.
pub(crate) async fn get_schema_by_name(
    state: &SchemaServiceState,
    schema_name: &str,
) -> Result<Response<Body>, Error> {
    match state.schema_by_name_reply(schema_name).await {
        SchemaByNameReply::Ready(envelope) => {
            let body = serde_json::to_value(envelope).map_err(serialization_error)?;
            json_response(200, &body)
        }
        SchemaByNameReply::Missing { body } => json_response(404, &body),
        SchemaByNameReply::Failed { body, .. } => json_response(500, &body),
    }
}
