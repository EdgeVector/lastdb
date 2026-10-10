//! Request parsing and response building helpers shared by every route.

use crate::cors;
use lambda_http::{Body, Error, Request, Response};
use schema_service_core::{
    SchemaMutationGateHeaders, HEADER_DEV_PUBKEY, HEADER_NODE_PUBLIC_KEY, HEADER_NODE_SIGNATURE,
    HEADER_POW_CHALLENGE, HEADER_POW_CHALLENGE_MAC, HEADER_POW_COUNTER, HEADER_POW_DIFFICULTY_BITS,
    HEADER_POW_EXPIRES_AT, HEADER_POW_NONCE,
};
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

/// Parse the optional `source=` query-string parameter for the
/// `/schemas/available` endpoint. Returns the matched `SchemaSource` variant,
/// or `None` when the param is absent. Unknown values are reported back so
/// callers can distinguish "no filter" from "filter that matched nothing".
pub(crate) fn parse_source_filter(event: &Request) -> (Option<schema_types::SchemaSource>, bool) {
    use schema_types::SchemaSource;
    match query_param(event, "source=") {
        None => (None, false),
        Some("system_seed") => (Some(SchemaSource::SystemSeed), false),
        Some("starter_seed") => (Some(SchemaSource::StarterSeed), false),
        Some("user") => (Some(SchemaSource::User), false),
        Some(_) => (None, true), // present but unknown
    }
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
        Body::Empty => Err(json_response(400, &json!({"error": "Request body is empty"})).unwrap()),
    }
}

// ─── `/v2` route helpers ──────────────────────────────────────────────────
//
// The `/v2` arms repeat the same three preludes — parse the body, require
// the DevCert header pair, split an app id or an (app id, channel) pair out
// of the path. Naming them keeps each arm to its own logic.

/// The parsed JSON body, or the 400 to return.
pub(crate) fn v2_body(event: &Request) -> Result<Value, Result<Response<Body>, Error>> {
    let raw = match parse_body(event) {
        Ok(b) => b,
        Err(r) => return Err(Ok(r)),
    };
    serde_json::from_str(&raw).map_err(|e| {
        json_response(
            400,
            &json!({"reason": "invalid_manifest", "detail": format!("invalid JSON: {e}")}),
        )
    })
}

/// The `X-Exemem-Dev-Cert` + `X-Signature` pair, or the 401 to return.
/// Every `/v2` write needs both; a read needs neither.
pub(crate) fn v2_cert_headers(
    event: &Request,
) -> Result<(&str, &str), Result<Response<Body>, Error>> {
    match (
        header_str(event, "x-exemem-dev-cert"),
        header_str(event, "x-signature"),
    ) {
        (Some(cert), Some(sig)) => Ok((cert, sig)),
        _ => Err(json_response(
            401,
            &json!({"reason": "cert_invalid", "detail": "missing X-Exemem-Dev-Cert or X-Signature header"}),
        )),
    }
}

/// A path segment that must be exactly one app id — non-empty and with no
/// embedded slash, so a crafted path cannot smuggle a second segment in.
pub(crate) fn v2_app_id(segment: &str) -> Option<&str> {
    let app_id = segment.trim_end_matches('/');
    (!app_id.is_empty() && !app_id.contains('/')).then_some(app_id)
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
