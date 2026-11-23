//! Relay HTTP handlers. Each authenticated call is gated by the grant index in `gate.rs`.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::Json;
use mutation_log::{DatabaseId, NodeKey};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::Mutex;

use crate::auth;
use crate::blob_store::checksum_header;
use crate::error::{ApiError, ApiResult};
use crate::gate;
use crate::grant_index::GrantIndex;
use crate::presign;
use crate::request_log::EntriesReturned;
use crate::store::{self, Store};

/// Shared process state.
pub(crate) struct AppState {
    pub(crate) store: Store,
    pub(crate) public_base: String,
    pub(crate) secret: Vec<u8>,
    pub(crate) grants: Mutex<GrantIndex>,
    /// The largest blob a `blob.put` begin may declare. `None` sets no limit.
    pub(crate) max_blob_bytes: Option<u64>,
    /// Proof only: read an upload body at most this many bytes a second.
    pub(crate) upload_bytes_per_second: Option<u64>,
    /// Proof only: after this many bytes of an upload body, stop reading it.
    pub(crate) upload_stall_after: Option<u64>,
    /// Proof only: `close-once` drops the connection of the first large upload after part of its
    /// body; `400` answers every large upload with HTTP 400.
    pub(crate) upload_fault: Option<String>,
    pub(crate) upload_fault_done: std::sync::atomic::AtomicBool,
}

impl AppState {
    async fn check_read(&self, db: &DatabaseId, caller: &NodeKey) -> ApiResult<()> {
        let grants = self.grants.lock().await;
        gate::authorize_read(&grants, db, caller)
    }

    async fn check_log_read(
        &self,
        db: &DatabaseId,
        caller: &NodeKey,
        writer_key: &str,
        all_devices: bool,
    ) -> ApiResult<()> {
        let grants = self.grants.lock().await;
        gate::authorize_log_read(&grants, db, caller, writer_key, all_devices)
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
    let req: LogAppendBody = parse_body(&body, crate::MAX_JSON_BODY_BYTES)?;
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
) -> ApiResult<Response> {
    let verified = auth::verify_request(&headers, "/relay/v1/log/read", &body)?;
    let req: LogReadBody = parse_body(&body, SMALL_BODY_BYTES)?;
    verified.check_database(&req.database_id)?;
    let caller = verified.node_key;
    let db = gate::parse_id(&req.database_id, "database_id")?;
    let position = read_position(&req)?;
    let (writer_key, all_devices) = match &position {
        ReadPosition::Device { writer_key, .. } => (writer_key.as_str(), false),
        ReadPosition::Writer { writer_key, .. } => (writer_key.as_str(), true),
    };
    state
        .check_log_read(&db, &caller, writer_key, all_devices)
        .await?;
    let limit = req.limit.unwrap_or(90) as usize;
    let (entries, cursor) = match position {
        ReadPosition::Device {
            writer_key,
            device_id,
            from_seq,
        } => {
            state
                .store
                .read_entries(&req.database_id, &writer_key, &device_id, from_seq, limit)
                .await?
        }
        ReadPosition::Writer {
            writer_key,
            from_device,
            from_seq,
        } => {
            state
                .store
                .read_writer_entries(
                    &req.database_id,
                    &writer_key,
                    from_device.as_deref(),
                    from_seq,
                    limit,
                )
                .await?
        }
    };
    let returned = EntriesReturned(entries.len());
    let mut out = json!({ "entries": entries });
    if let Some(c) = cursor {
        out["cursor"] = Value::String(c);
    }
    let mut response = Json(out).into_response();
    response.extensions_mut().insert(returned);
    Ok(response)
}

/// `log/streams`: the device streams of one writer and how far each reaches.
///
/// This is a read of every device of `writer_key`, so it is gated by exactly the
/// check that gates an all-devices `log/read`. The answer comes from the heads
/// the store keeps in memory; no entry file is read.
pub(crate) async fn log_streams(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let verified = auth::verify_request(&headers, "/relay/v1/log/streams", &body)?;
    let req: LogStreamsBody = parse_body(&body, SMALL_BODY_BYTES)?;
    verified.check_database(&req.database_id)?;
    let caller = verified.node_key;
    let db = gate::parse_id(&req.database_id, "database_id")?;
    require_hex64(&req.writer_key, "writer_key")?;
    state
        .check_log_read(&db, &caller, &req.writer_key, true)
        .await?;
    let streams: Vec<Value> = state
        .store
        .list_streams(&req.database_id, &req.writer_key)
        .into_iter()
        .map(|s| {
            json!({
                "device_id": s.device_id,
                "head_seq": s.head_seq,
                "head_hash": s.head_hash,
            })
        })
        .collect();
    Ok(Json(json!({ "streams": streams })))
}

pub(crate) async fn blob_put(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let verified = auth::verify_request(&headers, "/relay/v1/blob/put", &body)?;
    let req: BlobPutBody = parse_body(&body, SMALL_BODY_BYTES)?;
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
    if state.max_blob_bytes.is_some_and(|limit| req.bytes > limit) {
        return Err(ApiError::invalid("bytes"));
    }
    let token = state.store.stage_begin(&req.sha256, req.bytes).await?;
    let (url, expires_at_ms) = presign::make_put_url(&state.public_base, &token, now_ms());
    Ok(Json(json!({
        "stage": "begin",
        "sha256": req.sha256,
        "upload": {
            "url": url,
            "headers": {
                "content-type": "application/octet-stream",
                "x-amz-checksum-sha256": checksum_header(&req.sha256)
            },
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
    let req: BlobGetBody = parse_body(&body, SMALL_BODY_BYTES)?;
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

enum ReadPosition {
    /// One device chain.
    Device {
        writer_key: String,
        device_id: String,
        from_seq: u64,
    },
    /// Every device under `writer_key`; cursor carries the device.
    Writer {
        writer_key: String,
        from_device: Option<String>,
        from_seq: u64,
    },
}

fn read_position(req: &LogReadBody) -> ApiResult<ReadPosition> {
    if let Some(cursor) = req.cursor.as_deref() {
        let (writer_key, device_id, from_seq) = store::parse_cursor(cursor)?;
        // `device_id` in the body keeps a single-device page; omit it to walk
        // every device under the writer (cursor still names the resume point).
        return if req.device_id.is_some() {
            Ok(ReadPosition::Device {
                writer_key,
                device_id,
                from_seq,
            })
        } else {
            Ok(ReadPosition::Writer {
                writer_key,
                from_device: Some(device_id),
                from_seq,
            })
        };
    }
    let writer_key = req
        .writer_key
        .clone()
        .ok_or_else(|| ApiError::invalid("writer_key"))?;
    require_hex64(&writer_key, "writer_key")?;
    let from_seq = req.from_seq.unwrap_or(1).max(1);
    match req.device_id.as_deref() {
        Some(device_id) => {
            require_hex64(device_id, "device_id")?;
            Ok(ReadPosition::Device {
                writer_key,
                device_id: device_id.to_owned(),
                from_seq,
            })
        }
        None => Ok(ReadPosition::Writer {
            writer_key,
            from_device: None,
            from_seq,
        }),
    }
}

/// The most bytes a request body may have when it carries no entries: a database id, a key,
/// a sequence number or a digest. `log/append` carries up to 90 entries and is bound by
/// the limit of the server instead.
pub(crate) const SMALL_BODY_BYTES: usize = 64 * 1024;

/// Decode a request body whose signature was already verified. Every handler
/// decodes through here, so there is one place that turns request bytes into a
/// typed body. The length is checked first, so a body larger than the route can
/// carry is refused before it is parsed.
pub(crate) fn parse_body<T: serde::de::DeserializeOwned>(
    body: &[u8],
    limit: usize,
) -> ApiResult<T> {
    if body.len() > limit {
        return Err(ApiError::invalid("body"));
    }
    serde_json::from_slice(body).map_err(|_| ApiError::invalid("body"))
}

fn require_hex64(s: &str, field: &str) -> ApiResult<()> {
    if s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(ApiError::invalid(field))
    }
}

pub(crate) fn now_ms() -> u64 {
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
struct LogStreamsBody {
    database_id: String,
    writer_key: String,
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
