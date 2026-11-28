//! Relay HTTP handlers. Each authenticated call is gated by the grant index in `gate.rs`.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::Json;
use mutation_log::{DatabaseId, NodeKey};
use relay_protocol::{
    AppendBody, Endpoint, LogAppend, LogAppendAnswer, LogRead, LogReadAnswer, LogReadRequest,
    LogStreams, LogStreamsAnswer, ReadItem,
};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::Mutex;

use crate::auth::{self, Caller};
use crate::error::{ApiError, ApiResult};
use crate::gate;
use crate::grant_index::GrantIndex;
use crate::request_log::EntriesReturned;
use crate::segment_api::{append_segment, entries_held, read_items};
use crate::store::{require_hex64, Store};
use crate::store_read::{read_limit, read_position, ReadPosition};

/// Shared process state.
pub(crate) struct AppState {
    pub(crate) store: Store,
    pub(crate) public_base: String,
    pub(crate) secret: Vec<u8>,
    pub(crate) grants: Mutex<GrantIndex>,
    pub(crate) trim_lock: Mutex<()>,
    /// A commit of a blob and a drop of blobs do not run beside each other.
    pub(crate) blob_lock: Mutex<()>,
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
    pub(crate) async fn check_read(&self, db: &DatabaseId, caller: &NodeKey) -> ApiResult<()> {
        let grants = self.grants.lock().await;
        gate::authorize_read(&grants, db, caller)
    }

    pub(crate) async fn check_blob_put(&self, db: &DatabaseId, caller: &NodeKey) -> ApiResult<()> {
        let grants = self.grants.lock().await;
        gate::authorize_blob_put(&grants, db, caller)
    }
}

pub(crate) async fn log_append(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<LogAppendAnswer>> {
    let (verified, req) = authenticate::<LogAppend>(&headers, &body)?;
    require_hex64(&req.database_id, "database_id")?;
    let caller = verified.node_key;
    let db = gate::parse_id(&req.database_id, "database_id")?;
    let entries = match req.body() {
        Ok(AppendBody::Entries(entries)) => entries,
        Ok(AppendBody::Segment(segment)) => {
            return append_segment(&state, &db, &caller, segment).await;
        }
        Err(field) => return Err(ApiError::invalid(field)),
    };
    let signed = gate::parse_entries(entries)?;
    let mut grants = state.grants.lock().await;
    let plan = gate::authorize_append(&grants, &db, &caller, &signed)?;
    let (stored, duplicate) = state
        .store
        .append_entries(&req.database_id, entries)
        .await?;
    plan.commit(&mut grants, db);
    Ok(Json(LogAppendAnswer {
        stored: u64::from(stored),
        duplicate,
    }))
}

pub(crate) async fn log_read(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Response> {
    let verified = auth::verify_request(&headers, LogRead::ROUTE.path(), &body)?;
    let req = parse_log_read(&body)?;
    verified.check_database(&req.database_id)?;
    require_hex64(&req.database_id, "database_id")?;
    let caller = verified.node_key;
    let db = gate::parse_id(&req.database_id, "database_id")?;
    let position = read_position(&req)?;
    let all_devices = matches!(&position, ReadPosition::Writer { .. });
    state.check_read(&db, &caller).await?;
    let limit = read_limit(req.limit)?;
    let (stored, cursor) = match position {
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
    let prefix = if all_devices { "w:" } else { "d:" };
    let items = read_items(&state, &req.database_id, stored)?;
    let (entries, cursor) = fit_response(items, cursor, prefix)?;
    // The request log counts entries, so a segment counts the entries it holds.
    let returned = EntriesReturned(entries_held(&entries));
    let answer = LogReadAnswer { entries, cursor };
    if json_bytes(&answer)? > crate::PLATFORM_PAYLOAD_MAX_BYTES {
        return Err(ApiError::batch_too_large("entries"));
    }
    let mut response = Json(answer).into_response();
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
) -> ApiResult<Json<LogStreamsAnswer>> {
    let (verified, req) = authenticate::<LogStreams>(&headers, &body)?;
    require_hex64(&req.database_id, "database_id")?;
    let caller = verified.node_key;
    let db = gate::parse_id(&req.database_id, "database_id")?;
    require_hex64(&req.writer_key, "writer_key")?;
    state.check_read(&db, &caller).await?;
    let streams = state.store.list_streams(&req.database_id, &req.writer_key);
    Ok(Json(LogStreamsAnswer { streams }))
}

/// Keep a `log/read` page inside the response limit. A page that stops early ends with a cursor
/// at the entry after its last one, so the reader continues there. The first entry is always
/// kept, so a reader always moves forward.
fn fit_response(
    mut entries: Vec<ReadItem>,
    cursor: Option<String>,
    prefix: &str,
) -> ApiResult<(Vec<ReadItem>, Option<String>)> {
    let longest_cursor = format!("{prefix}{}:{}:{}", "0".repeat(64), "0".repeat(64), u64::MAX);
    let envelope = LogReadAnswer {
        entries: Vec::new(),
        cursor: Some(longest_cursor),
    };
    let mut used = json_bytes(&envelope)?;
    let mut cut = None;
    for (index, entry) in entries.iter().enumerate() {
        // The entry and the comma before it.
        let size = json_bytes(entry)? + 1;
        if index > 0 && used + size > crate::PLATFORM_PAYLOAD_MAX_BYTES {
            cut = Some(index);
            break;
        }
        used += size;
    }
    let Some(cut) = cut else {
        return Ok((entries, cursor));
    };
    // A segment carries its stream and ends at `last_seq`; an entry at `writer_seq`.
    let (node_key, device_id, seq) = match &entries[cut - 1] {
        ReadItem::Segment { segment } => (
            Some(segment.node_key.clone()),
            Some(segment.device_id.clone()),
            Some(segment.last_seq),
        ),
        ReadItem::Entry(entry) => {
            let field = |name: &str| entry.get(name).and_then(Value::as_str).map(str::to_owned);
            (
                field("node_key"),
                field("device_id"),
                entry.get("writer_seq").and_then(Value::as_u64),
            )
        }
    };
    let (Some(node_key), Some(device_id), Some(seq)) = (node_key, device_id, seq) else {
        return Err(ApiError::invalid("entries"));
    };
    let next_seq = seq
        .checked_add(1)
        .ok_or_else(|| ApiError::invalid("entries"))?;
    entries.truncate(cut);
    Ok((
        entries,
        Some(format!("{prefix}{node_key}:{device_id}:{next_seq}")),
    ))
}

fn json_bytes<T: Serialize>(value: &T) -> ApiResult<usize> {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .map_err(|_| ApiError::invalid("entries"))
}

/// Decode a request body whose signature was already verified. Its size is bound by
/// `PLATFORM_PAYLOAD_MAX_BYTES`, which the server applies to every request.
pub(crate) fn parse_body<T: serde::de::DeserializeOwned>(body: &[u8]) -> ApiResult<T> {
    serde_json::from_slice(body).map_err(|_| ApiError::invalid("body"))
}

/// Verify a request to endpoint `E`, parse its body as `E::Request`, and check that the signed
/// database header names the body's database. A body that does not parse is 400
/// `invalid_request`, field `body`.
pub(crate) fn authenticate<E: Endpoint>(
    headers: &HeaderMap,
    body: &[u8],
) -> ApiResult<(Caller, E::Request)> {
    let verified = auth::verify_request(headers, E::ROUTE.path(), body)?;
    let req: E::Request = parse_body(body)?;
    verified.check_database(E::database_id(&req))?;
    Ok((verified, req))
}

/// A `log.read` body. A `limit` above what the protocol type holds parses as `u32::MAX`, so
/// [`read_limit`] refuses it as `batch_too_large`, field `limit`, in its place among the checks.
fn parse_log_read(body: &[u8]) -> ApiResult<LogReadRequest> {
    if let Ok(req) = serde_json::from_slice::<LogReadRequest>(body) {
        return Ok(req);
    }
    let mut value: Value = parse_body(body)?;
    let Some(limit) = value
        .get_mut("limit")
        .filter(|limit| limit.as_u64().is_some_and(|n| n > u64::from(u32::MAX)))
    else {
        return Err(ApiError::invalid("body"));
    };
    *limit = Value::from(u32::MAX);
    serde_json::from_value(value).map_err(|_| ApiError::invalid("body"))
}
