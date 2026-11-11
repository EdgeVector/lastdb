//! Owner-socket response envelope and I4 error mapping.

use crate::lastdb_uds::uds_http::UdsResponse;
use fold_db::access::AccessContext;
use fold_db::error::FoldDbError;
use fold_db::schema::SchemaError;
use fold_db::storage::StorageError;
use serde::Serialize;
use serde_json::Value;

const OWNER_VISIBLE_STATUSES: &[u16] = &[400, 401, 403, 404, 409, 413, 422, 423, 503, 507];

pub(in crate::exec) fn envelope<T: Serialize>(data: &T, user_hash: &str) -> Value {
    let mut map = serde_json::Map::new();
    if let Ok(Value::Object(fields)) = serde_json::to_value(data) {
        for (k, v) in fields {
            if k != "ok" && k != "user_hash" {
                map.insert(k, v);
            }
        }
    } else {
        debug_assert!(false, "handler payload did not serialize to a JSON object");
        map.insert(
            "contract_violation".into(),
            Value::String("non_object_payload".into()),
        );
    }
    map.insert("ok".into(), Value::Bool(true));
    map.insert("user_hash".into(), Value::String(user_hash.to_string()));
    Value::Object(map)
}

pub(in crate::exec) fn json_ok(body: &Value) -> UdsResponse {
    match serde_json::to_vec(body) {
        Ok(bytes) => {
            UdsResponse::new(200, "OK", bytes).with_header("Content-Type", "application/json")
        }
        Err(_) => content_free(500, "Internal Server Error"),
    }
}

pub(in crate::exec) fn content_free(status: u16, reason: &'static str) -> UdsResponse {
    UdsResponse::new(status, reason, reason.as_bytes().to_vec())
}

fn status_reason(status: u16) -> &'static str {
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
        match &err {
            FoldDbError::Schema(schema_err) => Self::from(schema_err.clone()),
            FoldDbError::Config(msg) | FoldDbError::Serialization(msg) => {
                Self::new(400, msg.clone())
            }
            FoldDbError::InProgress(msg) => Self::new(503, msg.clone()),
            other => Self::internal(other.to_string()),
        }
    }
}

impl From<SchemaError> for HostError {
    fn from(err: SchemaError) -> Self {
        match &err {
            SchemaError::NotFound(msg) => Self::new(404, msg.clone()),
            SchemaError::StorageRead { .. } => Self::internal(err.to_string()),
            SchemaError::Blocked(msg) | SchemaError::InvalidCursor(msg) => {
                Self::new(400, msg.clone())
            }
            s @ SchemaError::CasConflict { .. } => Self::new(
                409,
                s.cas_conflict_body()
                    .unwrap_or_else(|| serde_json::json!({}))
                    .to_string(),
            ),
            s @ SchemaError::RecordTooLarge { .. } => Self::new(
                413,
                s.field_value_too_large_body()
                    .unwrap_or_else(|| serde_json::json!({}))
                    .to_string(),
            ),
            SchemaError::ReadResourceLimit { detail } => Self::new(
                503,
                serde_json::json!({
                    "error": "read_resource_limit",
                    "retryable": false,
                    "message": detail,
                    "hint": "request fewer records or fields",
                })
                .to_string(),
            ),
            SchemaError::WriteResourceLimit { detail } => Self::new(
                503,
                serde_json::json!({
                    "error": if detail.starts_with("compaction_behind:") {
                        "compaction_behind"
                    } else {
                        "write_resource_limit"
                    },
                    "retryable": true,
                    "retry_after_secs": 2,
                    "message": detail,
                })
                .to_string(),
            ),
            s @ SchemaError::StorageFull { .. } => Self::new(
                507,
                s.storage_full_body()
                    .unwrap_or_else(|| serde_json::json!({}))
                    .to_string(),
            ),
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
            error => Self::internal(error.to_string()),
        }
    }
}

pub(in crate::exec) fn render(
    result: Result<Value, HostError>,
    ctx: &AccessContext,
) -> UdsResponse {
    match result {
        Ok(payload) => json_ok(&envelope(&payload, ctx.user_id.as_str())),
        Err(e) => {
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
            if let Some(body) = e.structured_body.as_ref() {
                match serde_json::to_vec(body) {
                    Ok(bytes) => UdsResponse::new(e.status, status_reason(e.status), bytes)
                        .with_header("Content-Type", "application/json"),
                    Err(_) => content_free(500, "Internal Server Error"),
                }
            } else {
                UdsResponse::new(e.status, status_reason(e.status), e.message.into_bytes())
            }
        }
    }
}

pub(in crate::exec) fn error_response(
    status: u16,
    message: &str,
    ctx: &AccessContext,
) -> UdsResponse {
    render(Err(HostError::new(status, message)), ctx)
}

pub(in crate::exec) fn mapped_error_response(
    operation: &str,
    err: FoldDbError,
    ctx: &AccessContext,
) -> UdsResponse {
    let mapped = HostError::from(err);
    if mapped.status == 500 {
        return render(
            Err(HostError::new(
                500,
                format!("{operation}: {}", mapped.message),
            )),
            ctx,
        );
    }
    render(Err(mapped), ctx)
}
