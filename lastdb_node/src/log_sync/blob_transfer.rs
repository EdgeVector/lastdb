//! Moving a blob between a file and the relay without holding it in memory.
//!
//! `blob.put` streams the file to the presigned upload URL in chunks via
//! positional reads (`read_at`). `blob.get` (in [`download`]) streams the
//! presigned download into a part file next to the destination, hashing as the
//! bytes pass, and renames it into place only when the size and SHA-256 match.
//! Memory use does not depend on the size of the blob.
//!
//! A transfer fails when the connection stalls, not when it is slow: a download
//! when no byte arrives for `STALL_TIMEOUT`, an upload when the transport takes
//! no byte of the body (or sends no answer after the last one) for that long.
//! No minimum rate is assumed.

mod download;
mod reclaim;
mod url_guard;

pub(super) use reclaim::{Dropped, HeadAt, DROP_BATCH, DROP_HEADS};

use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use relay_protocol::{BlobPut, BlobPutRequest, BlobPutStage, ErrorCode};
use reqwest::header::{HeaderName, HeaderValue};
use tokio::fs;
use tokio::io::AsyncSeekExt;

use super::relay::{CallError, RelayClient, STALL_TIMEOUT};

/// Bytes read from the file per chunk of an upload.
const CHUNK_BYTES: usize = 64 * 1024;

/// Starts the error of a `blob.put` begin that the relay refused for the size of the blob.
/// The relay answered HTTP 400 with code `invalid_request` and field `bytes`: it holds a limit
/// on one blob, and this blob is over it. The relay does not say what the limit is.
pub(super) const SIZE_REFUSED: &str = "the relay refused the blob size";

/// True when `error` is the answer of a relay to a blob that is larger than it accepts.
fn is_size_refusal(error: &CallError) -> bool {
    error
        .refusal(400)
        .is_some_and(|answer| answer.code == ErrorCode::InvalidRequest && answer.field == "bytes")
}

/// The error with its causes (`source` chain), since reqwest alone says "error sending request",
/// and the place: scheme, host and path of the URL. A presigned URL carries its signature and
/// credential in the query, so the query is never printed.
pub(super) fn describe(error: reqwest::Error, url: &str) -> String {
    use std::error::Error as _;
    let error = error.without_url();
    let mut text = error.to_string();
    let mut cause = error.source();
    while let Some(next) = cause {
        text.push_str(": ");
        text.push_str(&next.to_string());
        cause = next.source();
    }
    format!("{text} (for {})", place(url))
}

/// `scheme://host[:port]/path` of a URL, without the query or the fragment.
pub(super) fn place(url: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(url) => format!(
            "{}://{}{}{}",
            url.scheme(),
            url.host_str().unwrap_or("?"),
            url.port().map_or(String::new(), |port| format!(":{port}")),
            url.path()
        ),
        Err(_) => "an unreadable URL".to_owned(),
    }
}

/// An upload or download is tried at most this many times in all, after waits of 0.5 s and 1 s,
/// each cut to a random value between half and all of it.
pub(super) const UPLOAD_TRIES: u32 = 3;

pub(super) fn upload_backoff(tried: u32) -> Duration {
    use rand::Rng;
    let full = Duration::from_millis(500) * tried;
    rand::thread_rng().gen_range(full / 2..=full)
}

pub(super) fn is_retryable_store_status(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504)
}

/// What the kernel may still hold unsent when the transport has taken the last byte of a body, and
/// the rate at which that tail is assumed to drain.
const TAIL_BYTES: u64 = 4 << 20;
const TAIL_FLOOR_BYTES_PER_SECOND: u64 = 64 << 10;

/// Fails when `taken`, the bytes the transport has taken from an upload body, does not move for
/// [`STALL_TIMEOUT`]. Once the whole body is taken it waits for the answer that long, plus the
/// time the tail in the send buffer needs at the floor rate.
async fn stalled(taken: &AtomicU64, size: u64, url: &str) -> String {
    let tail = Duration::from_secs(size.min(TAIL_BYTES) / TAIL_FLOOR_BYTES_PER_SECOND);
    let (mut last, mut moved) = (0, Instant::now());
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let now = taken.load(Ordering::Relaxed);
        let allowed = if now >= size {
            STALL_TIMEOUT + tail
        } else {
            STALL_TIMEOUT
        };
        if now != last {
            (last, moved) = (now, Instant::now());
        } else if moved.elapsed() >= allowed {
            return format!(
                "blob upload stalled for {} s with {last} of {size} bytes sent (for {})",
                moved.elapsed().as_secs(),
                place(url)
            );
        }
    }
}

impl RelayClient {
    /// Put the file at `source` on the relay: `blob.put` begin, the presigned
    /// upload streamed from the file, commit.
    pub(super) async fn blob_put(
        &self,
        database_id: &str,
        sha256: &str,
        source: &Path,
    ) -> Result<(), String> {
        let file = fs::File::open(source)
            .await
            .map_err(|e| format!("blob file: {e}"))?;
        self.blob_put_file(database_id, sha256, file).await
    }

    /// The same as [`Self::blob_put`] for a file that is already open, read from
    /// its current position to its end. A snapshot holds its files open.
    pub(super) async fn blob_put_file(
        &self,
        database_id: &str,
        sha256: &str,
        mut file: fs::File,
    ) -> Result<(), String> {
        let len = file
            .metadata()
            .await
            .map_err(|e| format!("blob file: {e}"))?
            .len();
        let start = file
            .stream_position()
            .await
            .map_err(|e| format!("blob file: {e}"))?;
        let size = len.saturating_sub(start);
        let stage = |stage: BlobPutStage| BlobPutRequest {
            database_id: database_id.to_owned(),
            sha256: sha256.to_owned(),
            bytes: size,
            stage,
        };
        let begin = self
            .call_tracked::<BlobPut>(&stage(BlobPutStage::Begin))
            .await
            .map(|(answer, _)| answer)
            .map_err(|error| {
                if size > 0 && is_size_refusal(&error) {
                    format!("{SIZE_REFUSED}: {size} bytes")
                } else {
                    error.message
                }
            })?;
        if begin.upload.is_none() && begin.sha256 == sha256 {
            if let Some(usage) = &self.usage {
                usage.complete_pending_blob(database_id, sha256, size)?;
            }
            return Ok(());
        }
        let upload = begin.upload.ok_or("blob.put begin has no upload.url")?;
        url_guard::validate_presigned_url(&self.base, &upload.url)?;
        let headers = url_guard::presigned_headers(&upload.headers, true)?;
        let file = Arc::new(file.into_std().await);
        self.upload_with_retries(&upload.url, &headers, &file, start, size)
            .await?;
        self.commit_blob(database_id, sha256, size, &stage(BlobPutStage::Commit))
            .await
    }

    async fn upload_with_retries(
        &self,
        url: &str,
        headers: &[(HeaderName, HeaderValue)],
        file: &Arc<std::fs::File>,
        start: u64,
        size: u64,
    ) -> Result<(), String> {
        let mut tried = 0;
        let (status, final_url) = loop {
            tried += 1;
            match self.put_once(url, headers, file, start, size).await {
                Ok((status, _)) if tried < UPLOAD_TRIES && is_retryable_store_status(status) => {
                    tracing::warn!(
                        target: "lastdbd::log_sync",
                        status = status.as_u16(),
                        tried,
                        "blob upload received transient store status; sending the file again"
                    );
                    tokio::time::sleep(upload_backoff(tried)).await;
                }
                Ok(done) => break done,
                Err(error) if tried < UPLOAD_TRIES => {
                    tracing::warn!(
                        target: "lastdbd::log_sync",
                        %error,
                        tried,
                        "blob upload failed before an answer; sending the file again"
                    );
                    tokio::time::sleep(upload_backoff(tried)).await;
                }
                Err(error) => return Err(error),
            }
        };
        if status.is_redirection() {
            return Err(format!("blob upload unexpected redirect HTTP {status}"));
        }
        url_guard::validate_presigned_url(&self.base, &final_url)?;
        if !status.is_success() {
            return Err(format!("blob upload HTTP {status}"));
        }
        Ok(())
    }

    async fn commit_blob(
        &self,
        database_id: &str,
        sha256: &str,
        size: u64,
        request: &BlobPutRequest,
    ) -> Result<(), String> {
        if let Some(usage) = &self.usage {
            usage.blob_begins(database_id, sha256, size)?;
        }
        match self.call_tracked::<BlobPut>(request).await {
            Ok(_) => {
                if let Some(usage) = &self.usage {
                    usage.blob_ends(database_id, sha256, size, true)?;
                }
                Ok(())
            }
            Err(error) => {
                let refused = error
                    .status
                    .is_some_and(|status| (400..500).contains(&status));
                if let Some(usage) = self.usage.as_ref().filter(|_| refused) {
                    usage.blob_ends(database_id, sha256, size, false)?;
                }
                Err(error.message)
            }
        }
    }

    async fn put_once(
        &self,
        url: &str,
        headers: &[(HeaderName, HeaderValue)],
        file: &Arc<std::fs::File>,
        start: u64,
        size: u64,
    ) -> Result<(reqwest::StatusCode, String), String> {
        let taken = Arc::new(AtomicU64::new(0));
        let body = file_body(Arc::clone(file), start, size, Arc::clone(&taken));
        let mut upload = self
            .transfer
            .put(url)
            .header("content-length", size)
            .body(body);
        for (name, value) in headers {
            upload = upload.header(name, value);
        }
        let started = Instant::now();
        let sent = tokio::select! {
            sent = upload.send() => sent,
            message = stalled(&taken, size, url) => return Err(message),
        };
        sent.map(|response| (response.status(), response.url().as_str().to_owned()))
            .map_err(|e| {
                format!(
                    "blob upload: {} after {} s with {} of {size} bytes sent",
                    describe(e, url),
                    started.elapsed().as_secs(),
                    taken.load(Ordering::Relaxed)
                )
            })
    }
}

/// An upload body that reads `[start .. start + size]` via positional `read_at`
/// so concurrent or cancelled tasks never share kernel `f_pos`.
fn file_body(
    file: Arc<std::fs::File>,
    start: u64,
    size: u64,
    taken: Arc<AtomicU64>,
) -> reqwest::Body {
    let end = start.saturating_add(size);
    let chunks = futures::stream::unfold(start, move |offset| {
        let file = Arc::clone(&file);
        let taken = Arc::clone(&taken);
        async move {
            if offset >= end {
                return None;
            }
            let want = ((end - offset).min(CHUNK_BYTES as u64)) as usize;
            let read = tokio::task::spawn_blocking(move || read_at_chunk(&file, offset, want))
                .await
                .unwrap_or_else(|e| Err(std::io::Error::other(e.to_string())));
            match read {
                Ok(buf) if buf.is_empty() => None,
                Ok(buf) => {
                    let next = offset + buf.len() as u64;
                    taken.fetch_add(buf.len() as u64, Ordering::Relaxed);
                    Some((Ok::<Vec<u8>, std::io::Error>(buf), next))
                }
                Err(error) => Some((Err(error), end)),
            }
        }
    });
    reqwest::Body::wrap_stream(chunks)
}

fn read_at_chunk(file: &std::fs::File, offset: u64, want: usize) -> std::io::Result<Vec<u8>> {
    let mut buf = vec![0u8; want];
    loop {
        match file.read_at(&mut buf, offset) {
            Ok(n) => {
                buf.truncate(n);
                return Ok(buf);
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
}
