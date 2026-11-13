//! Owner-socket response envelope, I4 error mapping, and `GET /api/list` route and helpers.

use base64::Engine as _;
use fold_db::access::AccessContext;
use fold_db::db_operations::SchemaRecordKey;
use fold_db::error::FoldDbError;
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::{KeyValue, Query, QueryCursor, Schema};
use fold_db::schema::SchemaError;
use fold_db::storage::StorageError;
use serde::Serialize;
use serde_json::{json, Value};

use super::query;
use super::schema_resolve::resolve_schema_name;
use super::wire::{query_value, QueryRequest};
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

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

pub(in crate::exec) struct ListRecordKeysRequest {
    pub(in crate::exec) requested: String,
    pub(in crate::exec) limit: usize,
    pub(in crate::exec) cursor: Option<String>,
    pub(in crate::exec) hash_filter: Option<String>,
    pub(in crate::exec) key_name: Option<String>,
}

impl ListRecordKeysRequest {
    pub(in crate::exec) fn parse(target: &str) -> Result<Self, &'static str> {
        let requested = query_value(target, "schema")
            .filter(|s| !s.is_empty())
            .ok_or("schema query parameter is required")?;
        let limit =
            query_value(target, "limit").map_or(Ok(100), |raw| match raw.parse::<usize>() {
                Ok(0) => Err("limit must be at least 1"),
                Ok(n) => Ok(n.min(1000)),
                Err(_) => Err("limit must be a positive integer"),
            })?;
        let key_name = query_value(target, "key_name");
        if key_name.as_deref() == Some("") {
            return Err("key_name must not be empty");
        }
        Ok(Self {
            requested,
            limit,
            cursor: query_value(target, "cursor").filter(|c| !c.is_empty()),
            hash_filter: query_value(target, "hash"),
            key_name,
        })
    }
}

pub(in crate::exec) fn decode_alternate_cursor(
    raw: Option<&str>,
) -> Result<Option<QueryCursor>, HostError> {
    raw.map(|enc| {
        let bytes = enc
            .strip_prefix("ak2_")
            .and_then(|s| {
                base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(s)
                    .ok()
            })
            .ok_or_else(|| HostError::new(400, "invalid alternate key-list cursor"))?;
        serde_json::from_slice(&bytes)
            .map_err(|_| HostError::new(400, "invalid alternate key-list cursor"))
    })
    .transpose()
}

pub(in crate::exec) fn alternate_list_report(
    req: &ListRecordKeysRequest,
    schema: &Schema,
    page: &Value,
) -> Result<Value, HostError> {
    let name = req.key_name.as_deref().expect("named branch");
    let def = schema
        .require_record_keys()?
        .alternates
        .get(name)
        .ok_or_else(|| HostError::new(400, "alternate key is not declared"))?;
    let rows = page["results"]
        .as_array()
        .ok_or_else(|| HostError::new(500, "alternate key-list page is incomplete"))?;
    let map_keys = |f| {
        rows.iter()
            .map(|r| {
                let k: KeyValue = serde_json::from_value(r[f].clone())
                    .map_err(|_| HostError::new(500, "key-list returned invalid coordinates"))?;
                Ok(SchemaRecordKey {
                    hash: k
                        .hash
                        .ok_or_else(|| HostError::new(500, "key-list hash is absent"))?,
                    range: k.range.unwrap_or_default(),
                })
            })
            .collect::<Result<Vec<_>, HostError>>()
    };
    let (keys, primary_keys) = (map_keys("matched_key")?, map_keys("key")?);
    let next = (!page["next_cursor"].is_null())
        .then(|| {
            serde_json::to_vec(&page["next_cursor"]).map(|b| {
                format!(
                    "ak2_{}",
                    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b)
                )
            })
        })
        .transpose()
        .map_err(|_| HostError::new(500, "invalid alternate key-list cursor"))?;
    Ok(json!({
        "schema": req.requested, "schema_id": schema.name, "key_name": name,
        "key_definition": def, "hash_filter": req.hash_filter,
        "field_key": fold_db::alternate_keys::field_key(schema, name),
        "keys": keys, "primary_keys": primary_keys,
        "has_more": page["has_more"], "truncated": page["has_more"], "next_cursor": next,
    }))
}

pub(in crate::exec) async fn list_keys(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let res = async {
        let r = ListRecordKeysRequest::parse(&req.target).map_err(|m| HostError::new(400, m))?;
        let canonical = resolve_schema_name(host, &r.requested)?
            .ok_or_else(|| HostError::new(404, format!("schema not found: {}", r.requested)))?;
        if let Some(name) = r.key_name.as_deref() {
            let scope = host.records().db_ops().read_scope().await?;
            let report = scope
                .run_result(Box::pin(async {
                    let hash = r
                        .hash_filter
                        .as_deref()
                        .ok_or_else(|| HostError::new(400, "alternate key lists require hash="))?;
                    let schema = host
                        .records()
                        .schema_manager()
                        .get_schema_metadata(&canonical)?
                        .ok_or_else(|| HostError::new(404, "schema is absent"))?;
                    let mut q = Query::new_with_filter(
                        canonical.clone(),
                        Some(HashRangeFilter::HashKey(hash.to_owned())),
                    );
                    q.key_name = Some(name.to_owned());
                    let qreq = QueryRequest {
                        query: q,
                        limit: Some(r.limit),
                        offset: None,
                        cursor: decode_alternate_cursor(r.cursor.as_deref())?,
                        response_format: query::QueryResponseFormat::Full,
                    };
                    alternate_list_report(&r, &schema, &query::execute_query(host, qreq).await?)
                }))
                .await?;
            return Ok(json!({ "list": report }));
        }
        let report = host
            .records()
            .list_schema_record_keys(
                &r.requested,
                &canonical,
                r.limit,
                r.cursor.as_deref(),
                r.hash_filter.as_deref(),
            )
            .await
            .map_err(|e| map_host_err("list failed", e))?;
        Ok(json!({ "list": report }))
    }
    .await;
    render(res, ctx)
}
