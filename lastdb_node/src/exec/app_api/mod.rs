//! App API: `POST /v1/read` and `POST /v1/write`, list bodies, and one error shape.

use std::future::Future;

use serde_json::{json, Value};

use super::reply::{content_free, json_ok, HostError};
use crate::host::owned::Resolved;
use crate::host::Host;
use crate::lastdb_uds::uds_http::UdsResponse;
use crate::lastdb_uds::uds_router::valid_selector;

mod read;
mod write;

pub(in crate::exec) use read::read_route;
pub(in crate::exec) use write::write_route;

fn bad(message: impl Into<String>) -> HostError {
    HostError::new(400, message)
}

/// The statuses the app API answers with. Every other status maps onto one of them.
const fn normalize(status: u16) -> u16 {
    match status {
        400 | 422 => 400,
        401 | 403 => 403,
        404 => 404,
        409 => 409,
        413 => 413,
        423 | 429 | 503 => 503,
        507 => 507,
        _ => 500,
    }
}

const fn code(status: u16) -> &'static str {
    match status {
        400 => "invalid_request",
        403 => "permission_denied",
        404 => "not_found",
        409 => "conflict",
        413 => "too_large",
        503 => "busy",
        _ => "internal",
    }
}

const fn reason(status: u16) -> &'static str {
    match status {
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        409 => "Conflict",
        413 => "Payload Too Large",
        503 => "Service Unavailable",
        507 => "Insufficient Storage",
        _ => "Internal Server Error",
    }
}

fn error_value(error: &HostError) -> Value {
    let status = normalize(error.status);
    let message = if status == 500 {
        "internal error"
    } else {
        error.message.as_str()
    };
    json!({ "error": { "code": code(status), "message": message } })
}

fn fail(error: &HostError) -> UdsResponse {
    let status = normalize(error.status);
    if status == 500 {
        tracing::error!(target: "lastdb_node::exec", status = error.status, "app api error: {}", error.message);
    }
    serde_json::to_vec(&error_value(error)).map_or_else(
        |_| content_free(500, "Internal Server Error"),
        |body| {
            UdsResponse::new(status, reason(status), body)
                .with_header("Content-Type", "application/json")
        },
    )
}

fn ok(body: &Value) -> UdsResponse {
    json_ok(body)
}

/// Run `future` against the database an item names. Without an identifier the
/// personal database serves it. A received database is not served here yet.
async fn in_database<T>(
    selector: Option<&str>,
    host: &Host,
    future: impl Future<Output = Result<T, HostError>>,
) -> Result<T, HostError> {
    let Some(selector) = selector else {
        return future.await;
    };
    if !valid_selector(selector) {
        return Err(bad("invalid database identifier"));
    }
    match host.owned.resolve(selector) {
        Ok((Resolved::Owner, _)) => future.await,
        Ok((Resolved::Owned, Some(store))) => host.with_records(store, future).await,
        Ok((Resolved::Absent, None)) => Err(HostError::new(
            404,
            "database is not on this node; /v1 does not serve a received database yet",
        )),
        Ok(_) => Err(HostError::new(503, "database registry is unavailable")),
        Err(error) => {
            let (status, message) = error.status();
            Err(HostError::new(status, message))
        }
    }
}
