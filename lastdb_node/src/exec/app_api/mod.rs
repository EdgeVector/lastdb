//! App API: `POST /v1/read` and `POST /v1/write`, list bodies, and one error shape.

use std::future::Future;

use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::KeyValue;
use serde_json::{json, Value};

use super::app_session::active_app;
use super::reply::{content_free, json_ok, status_reason, HostError};
use super::schema_routes::resolve_schema_name;
use crate::host::owned::ResolvedStore;
use crate::host::Host;
use crate::lastdb_uds::uds_http::UdsResponse;
use crate::lastdb_uds::uds_router::valid_selector;

mod changes;
mod file_place;
mod read;
mod write;

pub(in crate::exec) use changes::changes_route;
pub(in crate::exec) use read::read_route;
pub(in crate::exec) use write::write_route;

fn bad(message: impl Into<String>) -> HostError {
    HostError::new(400, message)
}

fn key_filter(key: &KeyValue) -> Result<HashRangeFilter, HostError> {
    let hash = key
        .hash
        .clone()
        .filter(|hash| !hash.is_empty())
        .ok_or_else(|| bad("key.hash is required"))?;
    Ok(match key.range.clone() {
        Some(range) => HashRangeFilter::HashRangeKey { hash, range },
        None => HashRangeFilter::HashKey(hash),
    })
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
            UdsResponse::new(status, status_reason(status), body)
                .with_header("Content-Type", "application/json")
        },
    )
}

fn ok(body: &Value) -> UdsResponse {
    json_ok(body)
}

/// A refusal in the app API's one error shape, for the app socket itself.
pub(in crate::exec) fn refuse(status: u16, message: &str) -> UdsResponse {
    fail(&HostError::new(status, message))
}

/// On the app socket an app reads only the schemas it declared it reads.
/// The owner socket carries no app and is not limited.
fn require_read(schema: &str) -> Result<(), HostError> {
    match active_app() {
        Some(app) if !app.read.contains(schema) => Err(HostError::new(
            403,
            format!("the app {} did not declare that it reads {schema}", app.app),
        )),
        _ => Ok(()),
    }
}

/// On the app socket a schema name is the exact name of a schema on the node.
/// Call it after `require_read` or `require_write`, and inside `in_database`
/// when the item names a database. The owner socket keeps loose matching.
fn require_exact_name(host: &Host, name: &str) -> Result<(), HostError> {
    if active_app().is_none() {
        return Ok(());
    }
    match resolve_schema_name(host, name) {
        Ok(Some(found)) if found == name => Ok(()),
        Err(error) if error.status >= 500 => Err(error),
        _ => Err(HostError::new(404, format!("no schema is named {name}"))),
    }
}

/// One sentence for every write whose condition does not hold. The engine's
/// conflict body names the revision it found, which tells a caller whether
/// the key holds a record.
const WRITE_CONFLICT: &str = "the record is not at the revision this write names";

fn same_conflict(error: HostError) -> HostError {
    let revision_conflict = error.status == 409
        && serde_json::from_str::<Value>(&error.message)
            .ok()
            .is_some_and(|body| body.get("error").and_then(Value::as_str) == Some("cas_conflict"));
    if revision_conflict {
        HostError::new(409, WRITE_CONFLICT)
    } else {
        error
    }
}

/// On the app socket an app writes only the schemas it declared it writes.
fn require_write(schema: &str) -> Result<(), HostError> {
    match active_app() {
        Some(app) if !app.write.contains(schema) => Err(HostError::new(
            403,
            format!(
                "the app {} did not declare that it writes {schema}",
                app.app
            ),
        )),
        _ => Ok(()),
    }
}

/// On the app socket an app uses only the extensions it declared.
fn require_extension(name: &str) -> Result<(), HostError> {
    match active_app() {
        Some(app) if !app.extensions.contains(name) => Err(HostError::new(
            403,
            format!("the app {} did not declare the extension {name}", app.app),
        )),
        _ => Ok(()),
    }
}

/// The schemas the calling app declared it reads. `None` on the owner socket.
fn declared_reads() -> Option<Vec<String>> {
    active_app().map(|app| app.read.iter().cloned().collect())
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
    match host.owned.resolve(selector)? {
        ResolvedStore::Owner => future.await,
        ResolvedStore::Owned(store) => host.with_records(store, future).await,
        ResolvedStore::Absent => Err(HostError::new(
            404,
            "database is not on this node; /v1 does not serve a received database yet",
        )),
    }
}
