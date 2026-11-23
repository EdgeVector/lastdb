//! The presigned transfer routes of `blob.put` and `blob.get`. Bodies move in
//! chunks, so the relay's memory does not grow with the size of the blob.

use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use serde::Deserialize;
use tokio::io::AsyncReadExt;

use crate::error::ApiResult;
use crate::handlers::{now_ms, AppState};
use crate::presign;

/// Bytes read from disk per chunk of a download.
const CHUNK_BYTES: usize = 64 * 1024;

/// The smallest upload that `--upload-bytes-per-second` and `--upload-stall-after` apply to.
const PROOF_MIN_BYTES: u64 = 256 * 1024;

/// `PUT /relay/v1/blob/upload/{token}`: the body is streamed to staging. The request must carry
/// `x-amz-checksum-sha256` with the base64 SHA-256 that `blob.put` begin returned.
pub(crate) async fn blob_upload(
    State(state): State<Arc<AppState>>,
    Path(token): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> ApiResult<StatusCode> {
    let checksum = headers
        .get("x-amz-checksum-sha256")
        .and_then(|value| value.to_str().ok());
    // Proof options apply to an upload of at least this declared size, so the small files of a
    // snapshot and the manifest pass at once and only the large file is slow or stalls.
    let large = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|length| length >= PROOF_MIN_BYTES);
    let rate = state.upload_bytes_per_second.filter(|_| large);
    let stall_after = state.upload_stall_after.filter(|_| large);
    let mut seen = 0u64;
    // Proof options: a slow or a stalled reader. Without them the body passes untouched.
    let chunks = body.into_data_stream().then(move |chunk| {
        let len = chunk.as_ref().map_or(0, |bytes| bytes.len() as u64);
        let stalled = stall_after.is_some_and(|after| seen >= after);
        seen += len;
        async move {
            if stalled {
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            } else if let Some(rate) = rate.filter(|rate| *rate > 0) {
                tokio::time::sleep(std::time::Duration::from_millis(len * 1000 / rate)).await;
            }
            chunk
        }
    });
    state
        .store
        .stage_put(&token, checksum, Box::pin(chunks))
        .await?;
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
pub(crate) struct BytesQuery {
    e: u64,
    s: String,
}

/// `GET /relay/v1/blob/bytes/{sha256}`: the file is streamed from disk.
pub(crate) async fn blob_bytes(
    State(state): State<Arc<AppState>>,
    Path(sha256): Path<String>,
    Query(q): Query<BytesQuery>,
) -> ApiResult<Response> {
    presign::check_get(&state.secret, &sha256, q.e, &q.s, now_ms())?;
    let (file, len) = state.store.open_blob(&sha256).await?;
    let chunks = futures::stream::unfold(file, |mut file| async move {
        let mut buf = vec![0u8; CHUNK_BYTES];
        match file.read(&mut buf).await {
            Ok(0) => None,
            Ok(n) => {
                buf.truncate(n);
                Some((Ok::<Bytes, std::io::Error>(Bytes::from(buf)), file))
            }
            Err(error) => Some((Err(error), file)),
        }
    });
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
