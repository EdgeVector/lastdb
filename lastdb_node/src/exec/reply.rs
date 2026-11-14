//! Owner-socket JSON response envelope and I4 `HostError` mapping.

use fold_db::access::AccessContext;
use fold_db::error::FoldDbError;
use fold_db::schema::SchemaError;
use fold_db::storage::StorageError;
use serde::Serialize;
use serde_json::{json, Value};

use crate::lastdb_uds::uds_http::UdsResponse;

const OWNER_VISIBLE_STATUSES: &[u16] = &[400, 401, 403, 404, 409, 413, 422, 423, 503, 507];

pub(in crate::exec) fn envelope<T: Serialize>(data: &T, user_hash: &str) -> Value {
    let mut map = serde_json::Map::new();
    if let Ok(Value::Object(fields)) = serde_json::to_value(data) {
        map.extend(
            fields
                .into_iter()
                .filter(|(k, _)| k != "ok" && k != "user_hash"),
        );
    } else {
        debug_assert!(false, "handler payload did not serialize to a JSON object");
        map.insert("contract_violation".into(), "non_object_payload".into());
    }
    map.insert("ok".into(), Value::Bool(true));
    map.insert("user_hash".into(), Value::String(user_hash.to_string()));
    Value::Object(map)
}

pub(in crate::exec) fn json_ok(body: &Value) -> UdsResponse {
    serde_json::to_vec(body).map_or_else(
        |_| content_free(500, "Internal Server Error"),
        |bytes| UdsResponse::new(200, "OK", bytes).with_header("Content-Type", "application/json"),
    )
}

pub(in crate::exec) fn content_free(status: u16, reason: &'static str) -> UdsResponse {
    UdsResponse::new(status, reason, reason.as_bytes().to_vec())
}

const fn status_reason(status: u16) -> &'static str {
    match status {
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        409 => "Conflict",
        413 => "Payload Too Large",
        422 => "Unprocessable Entity",
        423 => "Locked",
        429 => "Too Many Requests",
        503 => "Service Unavailable",
        507 => "Insufficient Storage",
        _ => "Internal Server Error",
    }
}

#[derive(Debug, Clone)]
pub(in crate::exec) struct HostError {
    pub(in crate::exec) status: u16,
    pub(in crate::exec) message: String,
    structured_body: Option<Value>,
}

impl HostError {
    pub(in crate::exec) fn new(status: u16, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            structured_body: None,
        }
    }

    pub(in crate::exec) fn structured(
        status: u16,
        message: impl Into<String>,
        body: Value,
    ) -> Self {
        Self {
            status,
            message: message.into(),
            structured_body: Some(body),
        }
    }

    pub(in crate::exec) fn internal(message: impl Into<String>) -> Self {
        Self::new(500, message)
    }
}

impl From<FoldDbError> for HostError {
    fn from(err: FoldDbError) -> Self {
        match err {
            FoldDbError::Schema(s) => Self::from(s),
            FoldDbError::Config(m) | FoldDbError::Serialization(m) => Self::new(400, m),
            FoldDbError::InProgress(m) => Self::new(503, m),
            other => Self::internal(other.to_string()),
        }
    }
}

impl From<SchemaError> for HostError {
    fn from(err: SchemaError) -> Self {
        let or_empty = |v: Option<Value>| v.unwrap_or_else(|| json!({})).to_string();
        match &err {
            SchemaError::NotFound(m) => Self::new(404, m.clone()),
            SchemaError::StorageRead { .. } => Self::internal(err.to_string()),
            SchemaError::Blocked(m) | SchemaError::InvalidCursor(m) => Self::new(400, m.clone()),
            s @ SchemaError::CasConflict { .. } => Self::new(409, or_empty(s.cas_conflict_body())),
            s @ SchemaError::RecordTooLarge { .. } => {
                Self::new(413, or_empty(s.field_value_too_large_body()))
            }
            SchemaError::ReadResourceLimit { detail } => Self::new(
                503,
                json!({
                    "error": "read_resource_limit", "retryable": false,
                    "message": detail, "hint": "request fewer records or fields",
                })
                .to_string(),
            ),
            SchemaError::WriteResourceLimit { detail } => {
                let kind = if detail.starts_with("compaction_behind:") {
                    "compaction_behind"
                } else {
                    "write_resource_limit"
                };
                Self::new(
                    503,
                    json!({ "error": kind, "retryable": true, "retry_after_secs": 2, "message": detail }).to_string(),
                )
            }
            s @ SchemaError::StorageFull { .. } => Self::new(507, or_empty(s.storage_full_body())),
            other => Self::new(400, other.to_string()),
        }
    }
}

impl From<StorageError> for HostError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::ReadResourceLimit { detail } => {
                Self::from(SchemaError::ReadResourceLimit { detail })
            }
            StorageError::WriteResourceLimit { detail } => {
                Self::from(SchemaError::WriteResourceLimit { detail })
            }
            e => Self::internal(e.to_string()),
        }
    }
}

pub(in crate::exec) fn render(
    result: Result<Value, HostError>,
    ctx: &AccessContext,
) -> UdsResponse {
    let e = match result {
        Ok(payload) => return json_ok(&envelope(&payload, ctx.user_id.as_str())),
        Err(e) => e,
    };
    if e.status == 507 {
        tracing::warn!(target: "lastdb_node::exec", status = e.status, "handler storage full: {}", e.message);
    } else if e.status >= 500 && e.status != 503 {
        tracing::error!(target: "lastdb_node::exec", status = e.status, "handler error: {}", e.message);
    }
    if !ctx.is_owner {
        return content_free(e.status, status_reason(e.status));
    }
    if !OWNER_VISIBLE_STATUSES.contains(&e.status) {
        return content_free(500, "Internal Server Error");
    }
    let reason = status_reason(e.status);
    e.structured_body.as_ref().map_or_else(
        || UdsResponse::new(e.status, reason, e.message.into_bytes()),
        |body| {
            serde_json::to_vec(body).map_or_else(
                |_| content_free(500, "Internal Server Error"),
                |b| {
                    UdsResponse::new(e.status, reason, b)
                        .with_header("Content-Type", "application/json")
                },
            )
        },
    )
}

pub(in crate::exec) fn error_response(status: u16, msg: &str, ctx: &AccessContext) -> UdsResponse {
    render(Err(HostError::new(status, msg)), ctx)
}

pub(in crate::exec) fn map_host_err(op: &str, err: FoldDbError) -> HostError {
    let mut m = HostError::from(err);
    if m.status == 500 {
        m.message = format!("{op}: {}", m.message);
    }
    m
}
