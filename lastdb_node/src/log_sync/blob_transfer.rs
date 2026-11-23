//! Moving a blob between a file and the relay without holding it in memory.
//!
//! `blob.put` streams the file to the presigned upload URL in chunks. `blob.get`
//! streams the presigned download into a part file next to the destination,
//! hashing as the bytes pass, and renames it into place only when the size and
//! the SHA-256 are the ones the bundle entry names. Memory use does not depend
//! on the size of the blob.
//!
//! A transfer fails when the connection stalls, not when it is slow: a download when no byte arrives
//! for `STALL_TIMEOUT` (the client's read timeout; an upload client has none, because that timeout runs until the answer starts), an upload when the transport takes no byte of the body (or sends no answer
//! after the last one) for that long. No minimum rate is assumed.

use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use app_identity_crypto::hex_lower;
use futures::StreamExt;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use super::relay::{header_pairs, RelayClient, STALL_TIMEOUT};

/// Bytes read from the file per chunk of an upload.
const CHUNK_BYTES: usize = 64 * 1024;

/// Starts the error of a `blob.put` begin that the relay refused for the size of the blob.
/// The relay answered HTTP 400 with code `invalid_request` and field `bytes`: it holds a limit
/// on one blob, and this blob is over it. The relay does not say what the limit is.
pub(super) const SIZE_REFUSED: &str = "the relay refused the blob size";

/// True when `error` is the answer of a relay to a blob that is larger than it accepts.
fn is_size_refusal(error: &str) -> bool {
    let body = error
        .find('{')
        .and_then(|at| serde_json::from_str::<serde_json::Value>(&error[at..]).ok());
    error.contains("HTTP 400")
        && body.is_some_and(|body| body["code"] == "invalid_request" && body["field"] == "bytes")
}

/// The error with its causes (`source` chain), since reqwest alone says "error sending request",
/// and the place: scheme, host and path of the URL. A presigned URL carries its signature and
/// credential in the query, so the query is never printed.
fn describe(error: reqwest::Error, url: &str) -> String {
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
fn place(url: &str) -> String {
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

/// An upload is tried at most this many times in all, after waits of 0.5 s and 1 s, each cut to a
/// random value between half and all of it. The added time of the two retries is at most 1.5 s of
/// waiting plus the time of the failed tries, each at most the stall allowance of
/// `stalled` (30 s without a byte, or 30 s plus the tail after the last byte).
const UPLOAD_TRIES: u32 = 3;

fn upload_backoff(tried: u32) -> Duration {
    use rand::Rng;
    let full = Duration::from_millis(500) * tried;
    rand::thread_rng().gen_range(full / 2..=full)
}

/// What the kernel may still hold unsent when the transport has taken the last byte of a body, and
/// the rate at which that tail is assumed to drain. The transport takes bytes from the body as the
/// socket buffer accepts them, so the last byte is taken while up to a send buffer of bytes is
/// still on its way, and no byte is taken meanwhile. This is the one place a floor rate is assumed,
/// and only for that tail: 4 MiB at 64 KiB/s is 64 s at most.
const TAIL_BYTES: u64 = 4 << 20;
const TAIL_FLOOR_BYTES_PER_SECOND: u64 = 64 << 10;

/// Fails when `taken`, the bytes the transport has taken from an upload body, does not move for
/// [`STALL_TIMEOUT`]. Once the whole body is taken it waits for the answer that long, plus the
/// time the tail in the send buffer needs at the floor rate. A slow link that keeps moving never
/// fails here: no minimum rate is assumed for the body.
async fn stalled(taken: &AtomicU64, size: u64) -> String {
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
                "blob upload stalled for {} s with {last} of {size} bytes sent",
                moved.elapsed().as_secs()
            );
        }
    }
}

impl RelayClient {
    /// Put the file at `source` on the relay: `blob.put` begin, the presigned
    /// upload streamed from the file, commit. The upload carries exactly the headers the relay
    /// returned (among them `x-amz-checksum-sha256`, the base64 SHA-256 of the file), and the
    /// object store or relay refuses it unless it is `bytes` long and has digest `sha256`.
    /// It also refuses a blob for a database whose birth it does not hold. A blob the
    /// relay already holds counts as put: its `begin` names no upload and nothing is sent.
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
        let size = file
            .metadata()
            .await
            .map_err(|e| format!("blob file: {e}"))?
            .len();
        let stage = |stage: &str| {
            json!({
                "database_id": database_id,
                "sha256": sha256,
                "bytes": size,
                "stage": stage,
            })
        };
        let begin = self
            .post("/relay/v1/blob/put", database_id, &stage("begin"))
            .await
            .map_err(|error| {
                if size > 0 && is_size_refusal(&error) {
                    format!("{SIZE_REFUSED}: {size} bytes")
                } else {
                    error
                }
            })?;
        // A relay that already holds this digest answers `begin` with the digest and no
        // `upload`: a committed blob is immutable, so there is nothing to send and nothing to
        // commit. A retried bundle and a snapshot that uploads files again both come here.
        if begin["upload"].is_null() && begin["sha256"].as_str() == Some(sha256) {
            return Ok(());
        }
        let url = begin["upload"]["url"]
            .as_str()
            .ok_or("blob.put begin has no upload.url")?;
        // A transport failure before any answer (the connection closed or reset, or the watch)
        // sends the file again from its start to the same URL: the file is on disk, the URL signs
        // the length and the digest and holds for about 15 minutes, and a second `begin` is not
        // needed (the Exemem relay would answer it with a new URL and make that the pending one).
        // An HTTP status from the store is an answer and is final.
        let start = file
            .stream_position()
            .await
            .map_err(|e| format!("blob file: {e}"))?;
        let mut tried = 0;
        let status = loop {
            tried += 1;
            let mut attempt = file
                .try_clone()
                .await
                .map_err(|e| format!("blob file: {e}"))?;
            attempt
                .seek(SeekFrom::Start(start))
                .await
                .map_err(|e| format!("blob file: {e}"))?;
            match self
                .put_once(url, &begin["upload"]["headers"], attempt, size)
                .await
            {
                Ok(status) => break status,
                Err(error) if tried < UPLOAD_TRIES => {
                    tracing::warn!(target: "lastdbd::log_sync", %error, tried, "blob upload failed before an answer; sending the file again");
                    tokio::time::sleep(upload_backoff(tried)).await;
                }
                Err(error) => return Err(error),
            }
        };
        if !status.is_success() {
            return Err(format!("blob upload HTTP {status}"));
        }
        self.post("/relay/v1/blob/put", database_id, &stage("commit"))
            .await?;
        Ok(())
    }

    /// One try of the presigned upload of `file` (read to its end): the status the store answered,
    /// or the error of a transport failure or of the stall watch.
    async fn put_once(
        &self,
        url: &str,
        headers: &serde_json::Value,
        file: fs::File,
        size: u64,
    ) -> Result<reqwest::StatusCode, String> {
        let taken = Arc::new(AtomicU64::new(0));
        let mut upload = self
            .transfer
            .put(url)
            .header("content-length", size)
            .body(file_body(file, Arc::clone(&taken)));
        for (name, value) in header_pairs(headers) {
            // The content-length is set above. The Exemem relay signs it and returns it among
            // the headers, and a second content-length header makes the object store answer 400.
            if name.eq_ignore_ascii_case("content-length") {
                continue;
            }
            upload = upload.header(name, value);
        }
        let started = Instant::now();
        let sent = tokio::select! {
            sent = upload.send() => sent,
            message = stalled(&taken, size) => return Err(message),
        };
        sent.map(|response| response.status()).map_err(|e| {
            format!(
                "blob upload: {} after {} s with {} of {size} bytes sent",
                describe(e, url),
                started.elapsed().as_secs(),
                taken.load(Ordering::Relaxed)
            )
        })
    }

    /// Fetch a blob into the file `dest`: `blob.get`, then the presigned
    /// download streamed to disk. `dest` appears only when the download is
    /// `expect_size` bytes and has digest `sha256`.
    pub(super) async fn blob_get(
        &self,
        database_id: &str,
        sha256: &str,
        expect_size: u64,
        dest: &Path,
    ) -> Result<(), String> {
        let body = json!({ "database_id": database_id, "sha256": sha256 });
        let meta = self.post("/relay/v1/blob/get", database_id, &body).await?;
        let url = meta["url"].as_str().ok_or("blob.get has no url")?;
        if meta["bytes"].as_u64().is_some_and(|n| n != expect_size) {
            return Err(format!("blob.get size differs from its entry for {sha256}"));
        }
        let mut download = self.download.get(url);
        for (name, value) in header_pairs(&meta["headers"]) {
            download = download.header(name, value);
        }
        let response = download
            .send()
            .await
            .map_err(|e| format!("blob download: {}", describe(e, url)))?;
        if !response.status().is_success() {
            return Err(format!("blob download HTTP {}", response.status()));
        }
        let part = part_path(dest, sha256);
        let written = write_verified(response, &part, sha256, expect_size, url).await;
        match written {
            Ok(()) => fs::rename(&part, dest)
                .await
                .map_err(|e| format!("blob file: {e}")),
            Err(error) => {
                let _ = fs::remove_file(&part).await;
                Err(error)
            }
        }
    }
}

/// An upload body that reads `file` in chunks.
fn file_body(file: fs::File, taken: Arc<AtomicU64>) -> reqwest::Body {
    let chunks = futures::stream::unfold(file, move |mut file| {
        let taken = Arc::clone(&taken);
        async move {
            let mut buf = vec![0u8; CHUNK_BYTES];
            match file.read(&mut buf).await {
                Ok(0) => None,
                Ok(n) => {
                    taken.fetch_add(n as u64, Ordering::Relaxed);
                    buf.truncate(n);
                    Some((Ok::<Vec<u8>, std::io::Error>(buf), file))
                }
                Err(error) => Some((Err(error), file)),
            }
        }
    });
    reqwest::Body::wrap_stream(chunks)
}

fn part_path(dest: &Path, sha256: &str) -> PathBuf {
    dest.with_file_name(format!(".{sha256}.part"))
}

/// Stream `response` into `part` (private file mode), then check size and digest.
async fn write_verified(
    response: reqwest::Response,
    part: &Path,
    sha256: &str,
    expect_size: u64,
    url: &str,
) -> Result<(), String> {
    if let Some(parent) = part.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|e| format!("blob file: {e}"))?;
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(part)
        .await
        .map_err(|e| format!("blob file: {e}"))?;
    let mut hasher = Sha256::new();
    let mut count = 0u64;
    let mut chunks = response.bytes_stream();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(|e| {
            format!(
                "blob download body: {} after {count} of {expect_size} bytes",
                describe(e, url)
            )
        })?;
        count += chunk.len() as u64;
        if count > expect_size {
            return Err(format!(
                "blob download is larger than its entry for {sha256}"
            ));
        }
        hasher.update(&chunk);
        file.write_all(&chunk)
            .await
            .map_err(|e| format!("blob file: {e}"))?;
    }
    file.sync_all()
        .await
        .map_err(|e| format!("blob file: {e}"))?;
    if count != expect_size {
        return Err(format!(
            "blob download is shorter than its entry for {sha256}"
        ));
    }
    if hex_lower(&hasher.finalize()) != sha256 {
        return Err("bundle blob does not match its digest".into());
    }
    Ok(())
}
