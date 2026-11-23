//! The presigned transfer routes of `blob.put` and `blob.get`. Bodies move in
//! chunks, so the relay's memory does not grow with the size of the blob.

use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use tokio::io::AsyncReadExt;

use crate::error::ApiResult;
use crate::handlers::{now_ms, AppState};
use crate::presign;

/// Bytes read from disk per chunk of a download.
const CHUNK_BYTES: usize = 64 * 1024;

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
    state
        .store
        .stage_put(&token, checksum, Box::pin(body.into_data_stream()))
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
