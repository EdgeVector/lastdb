//! The owner-socket response envelope and the **I4** error mapping — the single
//! choke point through which every socket response leaves either executor.
//!
//! - [`envelope`] builds the `{ ok, ...data, user_hash }` success body (the
//!   `ApiResponse`-flattened shape both binaries serve).
//! - [`json_ok`] serializes a body into a `200 OK` JSON [`UdsResponse`],
//!   collapsing a serialization failure to a content-free `500` (**I4**).
//! - [`content_free`] / [`status_reason`] are the content-free error shape: the
//!   body is exactly the status phrase, identical for every caller.
//! - [`owner_or_content_free`] / [`owner_json_or_content_free`] render an error
//!   to the OWNER caller (developer diagnostics or a typed rejection) while
//!   keeping every non-owner caller content-free.
//!
//! Keeping this in one place is the point: the I4 side-channel-closure property
//! (a non-owner error never echoes a schema name, namespace, or offending value)
//! is enforced by a single implementation both route executors share.

use fold_db::access::AccessContext;
use lastdb_uds::uds_http::UdsResponse;
use serde::Serialize;
use serde_json::Value;

/// The envelope's own keys. A handler payload may not define these — they are
/// the envelope's assertion about the response, not the handler's.
const RESERVED_KEYS: &[&str] = &["ok", "user_hash"];

/// The marker field added when a handler hands the envelope a payload that is
/// not a JSON object. Compile-time constant, so it is I4-safe.
pub const NON_OBJECT_PAYLOAD_MARKER: &str = "contract_violation";

/// Build the `{ ok: true, ...data, user_hash }` success envelope — the
/// `ApiResponse`-flattened shape. `data`'s object fields are spread at the top
/// level alongside `ok`/`user_hash`.
///
/// # The envelope's keys win
///
/// `ok` and `user_hash` are the envelope's assertion *about* the response, so a
/// payload field of the same name is dropped rather than spread over them.
/// Spreading the payload last — the original order — meant any handler could
/// overwrite the success discriminator, and one already sets `"ok"` in its own
/// payload (`POST /api/sync/cloud-off`). A handler that set `"ok": false` would
/// have shipped a `200` whose envelope said the call failed, and every caller
/// that branches on `ok` would have believed it.
///
/// # A non-object payload is a contract violation, not an empty result
///
/// Serializing to anything but an object (a bare array, a scalar, or a failed
/// serialization) previously contributed no fields, so the response was
/// `{ok:true, user_hash}` — byte-identical to a legitimately empty success. The
/// caller could not tell a handler bug from "nothing found". Such a payload now
/// carries the [`NON_OBJECT_PAYLOAD_MARKER`] field so the violation is visible
/// on the wire, and trips a debug assertion so it fails loudly in test builds.
#[must_use]
pub fn envelope<T: Serialize>(data: &T, user_hash: &str) -> Value {
    let mut map = serde_json::Map::new();

    if let Ok(Value::Object(fields)) = serde_json::to_value(data) {
        for (k, v) in fields {
            if RESERVED_KEYS.contains(&k.as_str()) {
                // The envelope owns this key; the payload's copy is dropped.
                continue;
            }
            map.insert(k, v);
        }
    } else {
        debug_assert!(
            false,
            "handler payload did not serialize to a JSON object; a success \
             envelope would be indistinguishable from an empty result"
        );
        map.insert(
            NON_OBJECT_PAYLOAD_MARKER.to_string(),
            Value::String("non_object_payload".to_string()),
        );
    }

    // Written last: the envelope's assertion is authoritative over the payload.
    map.insert("ok".to_string(), Value::Bool(true));
    map.insert(
        "user_hash".to_string(),
        Value::String(user_hash.to_string()),
    );
    Value::Object(map)
}

/// Serialize a JSON body into a `200 OK` response with a JSON content-type.
/// A serialization failure collapses to a content-free `500` rather than
/// surfacing the serde error (**I4**).
#[must_use]
pub fn json_ok(body: &Value) -> UdsResponse {
    match serde_json::to_vec(body) {
        Ok(bytes) => {
            UdsResponse::new(200, "OK", bytes).with_header("Content-Type", "application/json")
        }
        Err(_) => content_free(500, "Internal Server Error"),
    }
}

/// A content-free response carrying only a fixed status phrase (**I4**). The
/// body is the literal `reason`, identical for every caller — it never contains
/// a parse error, a handler error message, or any byte the caller sent.
#[must_use]
pub fn content_free(status: u16, reason: &'static str) -> UdsResponse {
    UdsResponse::new(status, reason, reason.as_bytes().to_vec())
}

/// Map an HTTP status code to its fixed reason phrase. An unmapped status
/// collapses to `Internal Server Error` so no richer body can leak through.
#[must_use]
pub fn status_reason(status: u16) -> &'static str {
    match status {
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        409 => "Conflict",
        413 => "Payload Too Large",
        422 => "Unprocessable Entity",
        429 => "Too Many Requests",
        503 => "Service Unavailable",
        507 => "Insufficient Storage",
        _ => "Internal Server Error",
    }
}

/// Every status a handler may deliberately return to the OWNER with its
/// diagnostic message intact.
///
/// **Keep this in sync with the mappings in [`crate::handlers`]** (`From<
/// SchemaError>` / `From<FoldDbError>` / `From<ReadBusy>` for `HostError`). A
/// status that a mapper produces but this list omits does NOT fall through as
/// itself — it collapses to a content-free `500`, discarding the typed body the
/// mapper built. That drift is invisible in production: the collapse happens
/// after [`render`](crate::handlers::render)'s 5xx logging, so the owner sees a
/// bare `500` and the node log stays empty. `413` was missing here for exactly
/// that reason and silently broke oversized-atom diagnostics.
///
/// `500` is deliberately absent: an unexpected internal error is content-free
/// even for the owner (**I4**).
/// Statuses whose diagnostic message the owner is allowed to see.
///
/// A status the handlers actually produce but this list omits is worse than
/// useless: [`owner_or_content_free`] collapses it to a bare `500`, so the
/// careful classification upstream never reaches the wire. That is what
/// happened to `422` (file-blob verification) and `429` (permanent cloud
/// quota) — the quota mapping added for the Sentry `7654526497` storm spared
/// the ERROR log, because `render` branches on `HostError::status`, but the
/// caller still read `500 Internal Server Error`. Keep this list in step with
/// the statuses handlers return.
const OWNER_VISIBLE_STATUSES: &[u16] = &[400, 401, 403, 404, 409, 413, 422, 429, 503, 507];

/// Render an error: the OWNER caller gets `message` in the body (developer
/// diagnostics like a schema-resolution failure), every non-owner caller gets
/// the content-free status phrase (**I4**). A `500` is always content-free even
/// for the owner, so an unexpected internal error can't leak its message.
#[must_use]
pub fn owner_or_content_free(status: u16, message: &str, ctx: &AccessContext) -> UdsResponse {
    if !ctx.is_owner {
        return content_free(status, status_reason(status));
    }
    if OWNER_VISIBLE_STATUSES.contains(&status) {
        return UdsResponse::new(status, status_reason(status), message.as_bytes().to_vec());
    }
    content_free(500, "Internal Server Error")
}

/// Render a structured error body to the OWNER while preserving the same I4
/// boundary as [`owner_or_content_free`].
///
/// Semantic policy rejections need fields a client can branch on (for example
/// the no-scan guard's product schema and keyed-access remediation). They must
/// not weaken the content-free response for a remote app, and an unexpected
/// status or serialization failure must still collapse to a bare `500`.
#[must_use]
pub fn owner_json_or_content_free(status: u16, body: &Value, ctx: &AccessContext) -> UdsResponse {
    if !ctx.is_owner {
        return content_free(status, status_reason(status));
    }
    if !OWNER_VISIBLE_STATUSES.contains(&status) {
        return content_free(500, "Internal Server Error");
    }
    match serde_json::to_vec(body) {
        Ok(bytes) => UdsResponse::new(status, status_reason(status), bytes)
            .with_header("Content-Type", "application/json"),
        Err(_) => content_free(500, "Internal Server Error"),
    }
}
