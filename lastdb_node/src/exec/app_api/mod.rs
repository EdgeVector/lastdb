//! App API: `POST /v1/read` and `POST /v1/write`, list bodies, and one error shape.

use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::KeyValue;
use serde_json::{json, Value};

use super::calling_app::active_app;
use super::reply::{content_free, json_ok, status_reason, HostError};
use super::schema_routes::resolve_schema_name;
use crate::host::Host;
use crate::lastdb_uds::uds_http::UdsResponse;

mod changes;
mod database;
mod databases;
mod databases_reply;
mod file_place;
mod owner_file;
mod read;
mod refusal;
mod write;

pub(in crate::exec) use changes::changes_route;
pub(in crate::exec) use databases::databases_route;
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
    let mut body = serde_json::Map::new();
    body.insert(
        "code".into(),
        json!(error.code.unwrap_or_else(|| code(status))),
    );
    body.insert("message".into(), json!(message));
    // A 500 hides the cause. index and reason belong to the refusal the caller can act on.
    if status != 500 {
        if let Some(index) = error.index {
            body.insert("index".into(), json!(index));
        }
        if let Some(reason) = error.reason {
            body.insert("reason".into(), json!(reason));
        }
    }
    Value::Object(serde_json::Map::from_iter([(
        String::from("error"),
        Value::Object(body),
    )]))
}

fn fail(error: &HostError) -> UdsResponse {
    let status = normalize(error.status);
    if status == 500 {
        tracing::error!(target: "lastdb_node::exec", status = error.status, "app api error: {}", error.message);
    }
    serde_json::to_vec(&error_value(error)).map_or_else(
        |_| content_free(500, "Internal Server Error"),
        |body| {
            let response = UdsResponse::new(status, status_reason(status), body)
                .with_header("Content-Type", "application/json");
            match error.retry_after {
                Some(seconds) => response.with_header("Retry-After", seconds.to_string()),
                None => response,
            }
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

/// On the app socket an app reads a schema only when its grant allows that schema.
/// `"*"` allows every schema. The owner socket carries no app and is not limited.
fn require_read(schema: &str) -> Result<(), HostError> {
    match active_app() {
        Some(app) if !app.read.allows(schema) => Err(HostError::new(
            403,
            format!("the app {} did not declare that it reads {schema}", app.app),
        )),
        _ => Ok(()),
    }
}

/// On the app socket a schema name is the exact name of a schema on the node.
/// Call it after `require_read` or `require_write`, and inside the database
/// the item names. The owner socket keeps loose matching.
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

/// On the app socket an app writes a schema only when its grant allows that schema.
/// `"*"` allows every schema.
fn require_write(schema: &str) -> Result<(), HostError> {
    match active_app() {
        Some(app) if !app.write.allows(schema) => Err(HostError::new(
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

/// Schemas a named read grant limits `changes` to.
/// `None` means every schema: the owner socket, or a grant of `"*"`.
fn declared_reads() -> Option<Vec<String>> {
    active_app().and_then(|app| {
        app.read
            .names()
            .map(|names| names.iter().cloned().collect())
    })
}
