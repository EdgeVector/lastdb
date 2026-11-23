//! The presigned transfer routes of `blob.put` and `blob.get`. Bodies move in
//! chunks, so the relay's memory does not grow with the size of the blob.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use serde::Deserialize;
use tokio::io::AsyncReadExt;

use crate::error::ApiResult;
use crate::handlers::AppState;
use crate::presign;
use crate::store::now_ms;

/// Bytes read from disk per chunk of a download.
const CHUNK_BYTES: usize = 64 * 1024;

/// The smallest upload that `--upload-bytes-per-second` and `--upload-stall-after` apply to.
const PROOF_MIN_BYTES: u64 = 256 * 1024;

#[derive(Deserialize)]
pub(crate) struct SignedQuery {
    e: u64,
    s: String,
}

/// `PUT /relay/v1/blob/upload/{database_id}/{token}`: the body is streamed to staging.
/// Requires a valid presigned query (`e`, `s`) and `x-amz-checksum-sha256`.
pub(crate) async fn blob_upload(
    State(state): State<Arc<AppState>>,
    Path((database_id, token)): Path<(String, String)>,
    Query(q): Query<SignedQuery>,
    headers: HeaderMap,
    body: Body,
) -> ApiResult<StatusCode> {
    presign::check_put(&state.secret, &database_id, &token, q.e, &q.s, now_ms())?;
    let checksum = headers
        .get("x-amz-checksum-sha256")
        .and_then(|value| value.to_str().ok());
    let large = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|length| length >= PROOF_MIN_BYTES);
    let rate = state.upload_bytes_per_second.filter(|_| large);
    let stall_after = state.upload_stall_after.filter(|_| large);
    let mut close_after = None;
    match state.upload_fault.as_deref() {
        Some("400") if large => {
            let mut body = body.into_data_stream();
            while body.next().await.is_some() {}
            return Err(crate::error::ApiError::invalid("sha256"));
        }
        Some("close-once") if large && !state.upload_fault_done.swap(true, Ordering::SeqCst) => {
            close_after = Some(PROOF_MIN_BYTES / 2);
        }
        _ => {}
    }
    let mut seen = 0u64;
    let chunks = body.into_data_stream().then(move |chunk| {
        let len = chunk.as_ref().map_or(0, |bytes| bytes.len() as u64);
        if close_after.is_some_and(|after| seen >= after) {
            eprintln!("relay upload_closed");
            panic!("reference_relay: --upload-fault close-once closes this connection");
        }
        let stalled = stall_after.is_some_and(|after| seen >= after);
        seen += len;
        pace_chunk(chunk, len, stalled, rate)
    });
    state
        .store
        .stage_put(&database_id, &token, checksum, Box::pin(chunks))
        .await?;
    Ok(StatusCode::OK)
}

async fn pace_chunk(
    chunk: Result<Bytes, axum::Error>,
    len: u64,
    stalled: bool,
    rate: Option<u64>,
) -> Result<Bytes, axum::Error> {
    if stalled {
        tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
    } else if let Some(rate) = rate.filter(|rate| *rate > 0) {
        tokio::time::sleep(std::time::Duration::from_millis(len * 1000 / rate)).await;
    }
    chunk
}

/// `GET /relay/v1/blob/bytes/{database_id}/{sha256}`: the file is streamed from disk.
pub(crate) async fn blob_bytes(
    State(state): State<Arc<AppState>>,
    Path((database_id, sha256)): Path<(String, String)>,
    Query(q): Query<SignedQuery>,
) -> ApiResult<Response> {
    presign::check_get(&state.secret, &database_id, &sha256, q.e, &q.s, now_ms())?;
    let (file, len) = state.store.open_blob(&database_id, &sha256).await?;
    let chunks = futures::stream::unfold(file, read_chunk);
    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/octet-stream".to_owned()),
            (header::CONTENT_LENGTH, len.to_string()),
        ],
        Body::from_stream(chunks),
    )
        .into_response())
}

async fn read_chunk(
    mut file: tokio::fs::File,
) -> Option<(Result<Bytes, std::io::Error>, tokio::fs::File)> {
    let mut buf = vec![0u8; CHUNK_BYTES];
    match file.read(&mut buf).await {
        Ok(0) => None,
        Ok(n) => {
            buf.truncate(n);
            Some((Ok(Bytes::from(buf)), file))
        }
        Err(error) => Some((Err(error), file)),
    }
}
