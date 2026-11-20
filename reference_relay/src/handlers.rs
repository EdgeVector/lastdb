//! Relay HTTP handlers. Each authenticated call is gated by the grant index in `gate.rs`.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use mutation_log::{DatabaseId, NodeKey};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::Mutex;

use crate::auth;
use crate::error::{ApiError, ApiResult};
use crate::gate;
use crate::grant_index::GrantIndex;
use crate::presign;
use crate::store::{self, Store};

/// Shared process state.
pub(crate) struct AppState {
    pub(crate) store: Store,
    pub(crate) public_base: String,
    pub(crate) secret: Vec<u8>,
    pub(crate) grants: Mutex<GrantIndex>,
}

impl AppState {
    async fn check_read(&self, db: &DatabaseId, caller: &NodeKey) -> ApiResult<()> {
        let grants = self.grants.lock().await;
        gate::authorize_read(&grants, db, caller)
    }

    async fn check_blob_put(&self, db: &DatabaseId, caller: &NodeKey) -> ApiResult<()> {
        let grants = self.grants.lock().await;
        gate::authorize_blob_put(&grants, db, caller)
    }
}

pub(crate) async fn log_append(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let verified = auth::verify_request(&headers, "/relay/v1/log/append", &body)?;
    let req: LogAppendBody =
        serde_json::from_slice(&body).map_err(|_| ApiError::invalid("body"))?;
    verified.check_database(&req.database_id)?;
    let caller = verified.node_key;
    let db = gate::parse_id(&req.database_id, "database_id")?;
    let signed = gate::parse_entries(&req.entries)?;
    let mut grants = state.grants.lock().await;
    let plan = gate::authorize_append(&grants, &db, &caller, &signed)?;
    let (stored, duplicate) = state
        .store
        .append_entries(&req.database_id, &req.entries)
        .await?;
    plan.commit(&mut grants, db);
    Ok(Json(json!({ "stored": stored, "duplicate": duplicate })))
}

pub(crate) async fn log_read(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let verified = auth::verify_request(&headers, "/relay/v1/log/read", &body)?;
    let req: LogReadBody = serde_json::from_slice(&body).map_err(|_| ApiError::invalid("body"))?;
    verified.check_database(&req.database_id)?;
    let caller = verified.node_key;
    let db = gate::parse_id(&req.database_id, "database_id")?;
    state.check_read(&db, &caller).await?;
    let (node_key, device_id, from_seq) = read_position(&req)?;
    let limit = req.limit.unwrap_or(90) as usize;
    let (entries, cursor) = state
        .store
        .read_entries(&req.database_id, &node_key, &device_id, from_seq, limit)
        .await?;
    let mut out = json!({ "entries": entries });
    if let Some(c) = cursor {
        out["cursor"] = Value::String(c);
    }
    Ok(Json(out))
}

pub(crate) async fn blob_put(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let verified = auth::verify_request(&headers, "/relay/v1/blob/put", &body)?;
    let req: BlobPutBody = serde_json::from_slice(&body).map_err(|_| ApiError::invalid("body"))?;
    verified.check_database(&req.database_id)?;
    let caller = verified.node_key;
    let db = gate::parse_id(&req.database_id, "database_id")?;
    state.check_blob_put(&db, &caller).await?;
    require_hex64(&req.sha256, "sha256")?;
    if req.bytes == 0 {
        return Err(ApiError::invalid("bytes"));
    }
    match req.stage.as_str() {
        "begin" => blob_begin(&state, &req).await,
        "commit" => {
            state.store.stage_commit(&req.sha256, req.bytes).await?;
            Ok(Json(json!({ "stage": "commit", "sha256": req.sha256 })))
        }
        _ => Err(ApiError::invalid("stage")),
    }
}

async fn blob_begin(state: &AppState, req: &BlobPutBody) -> ApiResult<Json<Value>> {
    let token = state.store.stage_begin(&req.sha256, req.bytes).await?;
    let (url, expires_at_ms) = presign::make_put_url(&state.public_base, &token, now_ms());
    Ok(Json(json!({
        "stage": "begin",
        "sha256": req.sha256,
        "upload": {
            "url": url,
            "headers": { "content-type": "application/octet-stream" },
            "expires_at_ms": expires_at_ms
        }
    })))
}

pub(crate) async fn blob_get(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let verified = auth::verify_request(&headers, "/relay/v1/blob/get", &body)?;
    let req: BlobGetBody = serde_json::from_slice(&body).map_err(|_| ApiError::invalid("body"))?;
    verified.check_database(&req.database_id)?;
    let caller = verified.node_key;
    let db = gate::parse_id(&req.database_id, "database_id")?;
    state.check_read(&db, &caller).await?;
    require_hex64(&req.sha256, "sha256")?;
    let (bytes, _) = state.store.blob_meta(&req.sha256).await?;
    if bytes == 0 {
        return Err(ApiError::not_found("sha256"));
    }
    let (url, expires_at_ms) =
        presign::make_get_url(&state.public_base, &state.secret, &req.sha256, now_ms());
    Ok(Json(json!({
        "url": url,
        "headers": {},
        "expires_at_ms": expires_at_ms,
        "bytes": bytes,
        "sha256": req.sha256
    })))
}

pub(crate) async fn blob_upload(
    State(state): State<Arc<AppState>>,
    Path(token): Path<String>,
    body: Bytes,
) -> ApiResult<StatusCode> {
    state.store.stage_put(&token, &body).await?;
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
pub(crate) struct BytesQuery {
    e: u64,
    s: String,
}

pub(crate) async fn blob_bytes(
    State(state): State<Arc<AppState>>,
    Path(sha256): Path<String>,
    Query(q): Query<BytesQuery>,
) -> ApiResult<Response> {
    presign::check_get(&state.secret, &sha256, q.e, &q.s, now_ms())?;
    let data = state.store.read_blob(&sha256).await?;
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/octet-stream")],
        data,
    )
        .into_response())
}

fn read_position(req: &LogReadBody) -> ApiResult<(String, String, u64)> {
    if let Some(cursor) = req.cursor.as_deref() {
        return store::parse_cursor(cursor);
    }
    let node_key = req
        .writer_key
        .clone()
        .ok_or_else(|| ApiError::invalid("writer_key"))?;
    let device_id = req
        .device_id
        .clone()
        .ok_or_else(|| ApiError::invalid("device_id"))?;
    require_hex64(&node_key, "writer_key")?;
    require_hex64(&device_id, "device_id")?;
    Ok((node_key, device_id, req.from_seq.unwrap_or(1).max(1)))
}

fn require_hex64(s: &str, field: &str) -> ApiResult<()> {
    if s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(ApiError::invalid(field))
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

#[derive(Deserialize)]
struct LogAppendBody {
    database_id: String,
    entries: Vec<Value>,
}

#[derive(Deserialize)]
struct LogReadBody {
    database_id: String,
    writer_key: Option<String>,
    device_id: Option<String>,
    from_seq: Option<u64>,
    cursor: Option<String>,
    limit: Option<u64>,
}

#[derive(Deserialize)]
struct BlobPutBody {
    database_id: String,
    stage: String,
    bytes: u64,
    sha256: String,
}

#[derive(Deserialize)]
struct BlobGetBody {
    database_id: String,
    sha256: String,
}
