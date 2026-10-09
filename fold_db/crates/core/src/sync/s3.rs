use super::error::{redact_sync_error_text, SyncError, SyncResult};
use futures::stream::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::future::Future;
use std::io::Write;
use std::path::Path;
use std::process::Output;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::io::ReaderStream;

/// Escape a value for curl `--config` double-quoted strings.
fn curl_config_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Build argv for the large-object curl PUT. The presigned URL is **not** on
/// argv — it is only referenced via `-K <config>` so process listings cannot
/// harvest signature query material.
fn curl_large_put_argv(config_path: &Path, file_path: &Path, max_time_secs: u64) -> Vec<String> {
    vec![
        "-sS".to_string(),
        "-f".to_string(),
        "-o".to_string(),
        "/dev/null".to_string(),
        "-w".to_string(),
        "%{http_code}".to_string(),
        "--max-time".to_string(),
        max_time_secs.to_string(),
        "-X".to_string(),
        "PUT".to_string(),
        "-H".to_string(),
        "Content-Type: application/octet-stream".to_string(),
        "-T".to_string(),
        file_path.display().to_string(),
        "-K".to_string(),
        config_path.display().to_string(),
    ]
}

/// Blocking curl PUT used by [`S3Client::upload_snapshot_file_via_curl`].
///
/// Writes the presigned URL into a temporary curl config file and invokes curl
/// with only the config path on argv (never the raw URL).
fn upload_snapshot_file_via_curl_blocking(
    url: &str,
    path: &Path,
    max_time_secs: u64,
) -> SyncResult<Output> {
    let config = curl_upload_config(url)?;
    let args = curl_large_put_argv(config.path(), path, max_time_secs);
    // Defensive: never allow a bare URL-looking token onto argv.
    for arg in &args {
        if arg.starts_with("http://") || arg.starts_with("https://") {
            return Err(SyncError::Network(
                "S3 upload: internal error — refused to put URL on curl argv".to_string(),
            ));
        }
    }
    std::process::Command::new("curl")
        .args(&args)
        .output()
        .map_err(|e| {
            SyncError::Network(format!(
                "S3 upload: failed to spawn curl (is curl on PATH?): {e}"
            ))
        })
}

fn curl_upload_config(url: &str) -> SyncResult<tempfile::NamedTempFile> {
    let mut config = tempfile::NamedTempFile::new().map_err(|e| {
        SyncError::Network(format!(
            "S3 upload: failed to create curl config tempfile: {e}"
        ))
    })?;
    // curl --config: double-quoted string value for `url`.
    writeln!(config, "url = \"{}\"", curl_config_escape(url))
        .map_err(|e| SyncError::Network(format!("S3 upload: failed to write curl config: {e}")))?;
    config
        .flush()
        .map_err(|e| SyncError::Network(format!("S3 upload: failed to flush curl config: {e}")))?;

    Ok(config)
}

/// Keep the large-object curl transport while streaming only the verified
/// prefix from the retained handle. The stdin pipe is bounded; no chunk copy
/// is written to disk or held in RAM. The URL remains in the config file.
fn upload_backup_prefix_via_curl_blocking(
    url: &str,
    source: std::fs::File,
    bytes: u64,
    max_time_secs: u64,
) -> SyncResult<Output> {
    use std::io::Read;
    use std::process::Stdio;

    let config = curl_upload_config(url)?;
    let mut args = curl_large_put_argv(config.path(), Path::new("-"), max_time_secs);
    args.extend([
        "--http1.1".to_string(),
        "-H".to_string(),
        format!("Content-Length: {bytes}"),
        "-H".to_string(),
        "Transfer-Encoding:".to_string(),
        "-H".to_string(),
        "Expect:".to_string(),
    ]);
    let mut child = std::process::Command::new("curl")
        .args(&args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(SyncError::Io)?;
    let copied = {
        let mut stdin = child.stdin.take().expect("piped curl stdin");
        std::io::copy(&mut source.take(bytes), &mut stdin)
    };
    let output = child.wait_with_output().map_err(SyncError::Io)?;
    // Preserve curl's HTTP/transport error if it closed the pipe early.
    if output.status.success() && copied? != bytes {
        return Err(SyncError::Storage(
            "backup source became shorter during upload".into(),
        ));
    }
    Ok(output)
}

fn check_curl_upload_output(output: &Output) -> SyncResult<()> {
    let http_code = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stderr = super::error::redact_sync_error_text(&stderr);
    if output.status.success() && (http_code.starts_with('2') || http_code.is_empty()) {
        return Ok(());
    }
    Err(SyncError::Network(format!(
        "S3 upload via curl failed: exit={} http_code={http_code} stderr={stderr}",
        output.status.code().unwrap_or(-1)
    )))
}

const DEFAULT_S3_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const S3_SNAPSHOT_UPLOAD_BYTES_PER_SEC: u64 = 256 * 1024;
const S3_SNAPSHOT_UPLOAD_MIN_TIMEOUT_MULTIPLIER: u32 = 10;
const S3_PRESIGNED_EXPIRY_SAFETY_MARGIN: Duration = Duration::from_secs(30);

/// Client for interacting with S3 via presigned URLs.
///
/// This client never has AWS credentials — it only uses presigned URLs
/// obtained from the auth Lambda. Each URL is scoped to a single S3 object
/// and a single operation (GET or PUT), expiring after a short window.
///
/// `Clone` is cheap (bumps the inner `Arc<reqwest::Client>` refcount) so
/// companion sync components can hold their own handles alongside the
/// `SyncEngine`.
#[derive(Clone)]
pub struct S3Client {
    http: Arc<Client>,
    request_timeout: Duration,
}

/// A presigned URL for a specific S3 operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresignedUrl {
    pub url: String,
    pub method: String,
    pub expires_in_secs: u64,
}

/// A rescue PUT URL. The service signs the create-only condition with the URL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresignedCreateOnlyUrl {
    pub url: String,
    pub method: String,
    pub expires_in_secs: u64,
    pub headers: BTreeMap<String, String>,
}

impl S3Client {
    fn rescue_headers(signed: &PresignedCreateOnlyUrl) -> SyncResult<reqwest::header::HeaderMap> {
        if signed.method != "PUT"
            || signed.headers.get("If-None-Match").map(String::as_str) != Some("*")
            || signed.expires_in_secs == 0
        {
            return Err(SyncError::Storage(
                "rescue upload lacks a signed create-only PUT".into(),
            ));
        }
        let mut headers = reqwest::header::HeaderMap::new();
        for (name, value) in &signed.headers {
            let name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| SyncError::Storage("invalid rescue upload header name".into()))?;
            let value = reqwest::header::HeaderValue::from_str(value)
                .map_err(|_| SyncError::Storage("invalid rescue upload header value".into()))?;
            headers.insert(name, value);
        }
        Ok(headers)
    }

    /// Return false on HTTP 412. The caller must verify the stored object.
    pub async fn upload_rescue_file(
        &self,
        signed: &PresignedCreateOnlyUrl,
        source: std::fs::File,
        bytes: u64,
    ) -> SyncResult<bool> {
        use tokio::io::AsyncReadExt;

        let headers = Self::rescue_headers(signed)?;
        let len = usize::try_from(bytes)
            .map_err(|_| SyncError::Storage("rescue file is too large".into()))?;
        let timeout = self.snapshot_upload_timeout(len, signed.expires_in_secs);
        let source = tokio::fs::File::from_std(source).take(bytes);
        let body = reqwest::Body::wrap_stream(ReaderStream::new(source));
        let response = self
            .with_timeout(
                "rescue upload",
                timeout,
                self.http
                    .put(&signed.url)
                    .headers(headers)
                    .header(reqwest::header::CONTENT_LENGTH, bytes)
                    .version(reqwest::Version::HTTP_11)
                    .body(body)
                    .send(),
            )
            .await?;
        if response.status() == reqwest::StatusCode::PRECONDITION_FAILED {
            return Ok(false);
        }
        self.check_or_read_body(response, "rescue upload").await?;
        Ok(true)
    }

    /// Return false on HTTP 412. Commit checks the existing manifest bytes.
    pub async fn upload_rescue_bytes(
        &self,
        signed: &PresignedCreateOnlyUrl,
        bytes: Vec<u8>,
    ) -> SyncResult<bool> {
        let headers = Self::rescue_headers(signed)?;
        let timeout = self.snapshot_upload_timeout(bytes.len(), signed.expires_in_secs);
        let response = self
            .with_timeout(
                "rescue upload",
                timeout,
                self.http
                    .put(&signed.url)
                    .headers(headers)
                    .header(reqwest::header::CONTENT_LENGTH, bytes.len())
                    .version(reqwest::Version::HTTP_11)
                    .body(bytes)
                    .send(),
            )
            .await?;
        if response.status() == reqwest::StatusCode::PRECONDITION_FAILED {
            return Ok(false);
        }
        self.check_or_read_body(response, "rescue upload").await?;
        Ok(true)
    }
    pub fn new(http: Arc<Client>) -> Self {
        Self::with_request_timeout(http, DEFAULT_S3_REQUEST_TIMEOUT)
    }

    pub fn with_request_timeout(http: Arc<Client>, request_timeout: Duration) -> Self {
        Self {
            http,
            request_timeout,
        }
    }

    fn timeout_label(timeout: Duration) -> String {
        if timeout.as_millis().is_multiple_of(1000) {
            format!("{}s", timeout.as_secs())
        } else {
            format!("{}ms", timeout.as_millis())
        }
    }

    /// Size-aware transfer budget for large snapshot objects (upload **and**
    /// download). Floor is `request_timeout * 10` (5 min with the default 30s);
    /// then scale by assumed 256 KiB/s, capped just under the presigned URL
    /// expiry so we fail with a useful error before the URL goes cold.
    ///
    /// Download used to hard-cap the response body at `request_timeout` (30s),
    /// which cannot pull a ~1 GiB personal snapshot and blocked cloud-restore
    /// cutovers (rehearsal 2026-07-13: "S3 download response body timed out
    /// after 30s" on `latest.enc`).
    fn snapshot_transfer_timeout(&self, bytes: usize, expires_in_secs: u64) -> Duration {
        let transfer_secs = (bytes as u64).saturating_add(S3_SNAPSHOT_UPLOAD_BYTES_PER_SEC - 1)
            / S3_SNAPSHOT_UPLOAD_BYTES_PER_SEC;
        let slow_uplink_floor = self
            .request_timeout
            .saturating_mul(S3_SNAPSHOT_UPLOAD_MIN_TIMEOUT_MULTIPLIER);
        let size_budget = self
            .request_timeout
            .saturating_add(Duration::from_secs(transfer_secs));
        let timeout = slow_uplink_floor.max(size_budget);

        let expires_in = Duration::from_secs(expires_in_secs);
        let max_usable_timeout = expires_in.saturating_sub(S3_PRESIGNED_EXPIRY_SAFETY_MARGIN);
        if max_usable_timeout.is_zero() {
            // URL is already within (or past) the safety margin of its expiry.
            // Cap to remaining life — never fall back to the uncapped 300s+
            // slow-uplink floor, which would wait long after the signature dies.
            // Floor of 1s keeps a zero/negative remaining life diagnosable.
            let remaining = if expires_in.is_zero() {
                Duration::from_secs(1)
            } else {
                expires_in
            };
            timeout.min(remaining)
        } else {
            timeout.min(max_usable_timeout)
        }
    }

    fn snapshot_upload_timeout(&self, bytes: usize, expires_in_secs: u64) -> Duration {
        self.snapshot_transfer_timeout(bytes, expires_in_secs)
    }

    async fn with_timeout<T, F>(
        &self,
        operation: &str,
        timeout: Duration,
        future: F,
    ) -> SyncResult<T>
    where
        F: Future<Output = Result<T, reqwest::Error>>,
    {
        match tokio::time::timeout(timeout, future).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(e)) => Err(Self::classify_reqwest_error(operation, &e)),
            Err(_) => Err(SyncError::Network(format!(
                "S3 {operation} timed out after {}",
                Self::timeout_label(timeout)
            ))),
        }
    }

    fn classify_reqwest_error(operation: &str, err: &reqwest::Error) -> SyncError {
        let message = redact_sync_error_text(&err.to_string());
        if err.is_timeout() {
            SyncError::Network(format!("S3 {operation} timeout: {message}"))
        } else if err.is_connect() {
            SyncError::Network(format!("S3 {operation} unreachable: {message}"))
        } else {
            SyncError::Network(format!("S3 {operation} transport error: {message}"))
        }
    }

    /// Return successful responses untouched; read the response body only for S3 errors.
    async fn check_or_read_body(
        &self,
        response: reqwest::Response,
        operation: &str,
    ) -> SyncResult<reqwest::Response> {
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }

        let body_operation = format!("{operation} response body");
        let body = self
            .with_timeout(&body_operation, self.request_timeout, response.text())
            .await
            .unwrap_or_default();
        // Error bodies can echo request URLs / signed query material (S3/R2
        // XML, reverse-proxy HTML). Redact before the string becomes
        // SyncError::S3 — transport errors already go through
        // `redact_sync_error_text`; body path must match that contract.
        let body = redact_sync_error_text(&body);
        Err(SyncError::S3(format!(
            "{operation} failed: HTTP {status}: {body}"
        )))
    }

    pub async fn upload(&self, presigned: &PresignedUrl, data: Vec<u8>) -> SyncResult<()> {
        self.upload_with_timeout(presigned, data, self.request_timeout)
            .await
    }

    /// Upload bytes only while the remote object is still the version we read.
    ///
    /// `expected_etag=None` is a create-only write (`If-None-Match: *`). A
    /// failed precondition is normal CAS contention and returns `Ok(false)`;
    /// callers can re-read, merge, and retry without string-matching an S3
    /// error body.
    pub async fn upload_conditional(
        &self,
        presigned: &PresignedUrl,
        data: Vec<u8>,
        expected_etag: Option<&str>,
    ) -> SyncResult<bool> {
        let mut request = self
            .http
            .put(&presigned.url)
            .header("Content-Type", "application/octet-stream");
        request = if let Some(etag) = expected_etag {
            request.header(reqwest::header::IF_MATCH, etag)
        } else {
            request.header(reqwest::header::IF_NONE_MATCH, "*")
        };
        let response = self
            .with_timeout(
                "conditional upload",
                self.request_timeout,
                request.body(data).send(),
            )
            .await?;
        if response.status() == reqwest::StatusCode::PRECONDITION_FAILED {
            return Ok(false);
        }
        self.check_or_read_body(response, "conditional upload")
            .await?;
        Ok(true)
    }

    /// Upload snapshot bytes to S3 using a presigned PUT URL.
    pub async fn upload_snapshot(&self, presigned: &PresignedUrl, data: Vec<u8>) -> SyncResult<()> {
        let timeout = self.snapshot_upload_timeout(data.len(), presigned.expires_in_secs);
        self.upload_with_timeout(presigned, data, timeout).await
    }

    /// Above this size, multi-GiB snapshot PUTs use `curl -T` (streamed file
    /// body). Dogfood 2026-07-19: R2 accepts ~3.7 GiB with `curl -T` (HTTP 200
    /// in ~8 min) while reqwest `Body::wrap_stream` fails mid-transfer with a
    /// generic "error sending request" transport error under live Mini load.
    /// Keep the reqwest path for small tests / unit harnesses.
    const SNAPSHOT_FILE_CURL_THRESHOLD_BYTES: u64 = 32 * 1024 * 1024;

    /// Upload a sealed snapshot file to S3 using a presigned PUT URL.
    ///
    /// Snapshots on primary Mini homes can be multi-GiB. Streaming from disk
    /// keeps retry attempts from materializing a full ciphertext `Vec<u8>` for
    /// each PUT while preserving the same size-aware transfer timeout.
    pub async fn upload_snapshot_file(
        &self,
        presigned: &PresignedUrl,
        path: &std::path::Path,
    ) -> SyncResult<()> {
        let len = tokio::fs::metadata(path)
            .await
            .map_err(SyncError::Io)?
            .len();
        let len_usize = usize::try_from(len)
            .map_err(|_| SyncError::Storage(format!("snapshot file too large: {len} bytes")))?;
        let timeout = self.snapshot_upload_timeout(len_usize, presigned.expires_in_secs);

        if len >= Self::SNAPSHOT_FILE_CURL_THRESHOLD_BYTES {
            return self
                .upload_snapshot_file_via_curl(presigned, path, len, timeout)
                .await;
        }

        let file = tokio::fs::File::open(path).await.map_err(SyncError::Io)?;
        let body = reqwest::Body::wrap_stream(ReaderStream::new(file));

        let response = self
            .with_timeout(
                "upload",
                timeout,
                self.http
                    .put(&presigned.url)
                    .header("Content-Type", "application/octet-stream")
                    .header(reqwest::header::CONTENT_LENGTH, len)
                    .version(reqwest::Version::HTTP_11)
                    .body(body)
                    .send(),
            )
            .await?;

        self.check_or_read_body(response, "upload").await?;
        Ok(())
    }

    /// Upload the verified manifest prefix from the caller's open file handle.
    ///
    /// The backup packing lock prevents prefix rewrite. Bounded reads exclude
    /// later appends, and this never reopens a path after digest verification.
    /// Keep the snapshot transfer budget without making a local clone.
    pub async fn upload_backup_chunk_file(
        &self,
        presigned: &PresignedUrl,
        source: std::fs::File,
        bytes: u64,
    ) -> SyncResult<()> {
        use tokio::io::AsyncReadExt;

        let len = usize::try_from(bytes)
            .map_err(|_| SyncError::Storage(format!("backup chunk too large: {bytes} bytes")))?;
        let timeout = self.snapshot_upload_timeout(len, presigned.expires_in_secs);
        if bytes >= Self::SNAPSHOT_FILE_CURL_THRESHOLD_BYTES {
            let url = presigned.url.clone();
            let max_time_secs = timeout.as_secs().saturating_add(5).max(60);
            let span = tracing::Span::current();
            let output = tokio::task::spawn_blocking(move || {
                span.in_scope(|| {
                    upload_backup_prefix_via_curl_blocking(&url, source, bytes, max_time_secs)
                })
            })
            .await
            .map_err(|error| SyncError::Network(format!("S3 upload join error: {error}")))??;
            return check_curl_upload_output(&output);
        }
        let source = tokio::fs::File::from_std(source).take(bytes);
        let body = reqwest::Body::wrap_stream(ReaderStream::new(source));
        let response = self
            .with_timeout(
                "upload",
                timeout,
                self.http
                    .put(&presigned.url)
                    .header("Content-Type", "application/octet-stream")
                    .header(reqwest::header::CONTENT_LENGTH, bytes)
                    .version(reqwest::Version::HTTP_11)
                    .body(body)
                    .send(),
            )
            .await?;
        self.check_or_read_body(response, "upload").await?;
        Ok(())
    }

    /// Stream a large sealed snapshot with `curl -T` (file-backed PUT).
    ///
    /// Does not load the object into process RSS. Exit status / HTTP code are
    /// mapped into [`SyncError`] without echoing the presigned URL.
    ///
    /// The presigned URL is written into a temporary curl `--config` file and
    /// never passed as a process argv argument, so `ps` / crash reporters /
    /// shell history cannot harvest signature query material from the command
    /// line.
    async fn upload_snapshot_file_via_curl(
        &self,
        presigned: &PresignedUrl,
        path: &std::path::Path,
        len: u64,
        timeout: Duration,
    ) -> SyncResult<()> {
        let url = presigned.url.clone();
        let path = path.to_path_buf();
        // Leave a few seconds for curl process setup; floor at 60s.
        let max_time_secs = timeout.as_secs().saturating_add(5).max(60);

        tracing::info!(
            target: "fold_db::sync::memory",
            bytes = len,
            max_time_secs,
            "snapshot upload: streaming file via curl -T (large object path)"
        );

        let output = tokio::task::spawn_blocking(move || {
            upload_snapshot_file_via_curl_blocking(&url, &path, max_time_secs)
        })
        .await
        .map_err(|e| SyncError::Network(format!("S3 upload join error: {e}")))?;

        check_curl_upload_output(&output?)
    }

    /// Upload bytes to S3 using a presigned PUT URL.
    async fn upload_with_timeout(
        &self,
        presigned: &PresignedUrl,
        data: Vec<u8>,
        timeout: Duration,
    ) -> SyncResult<()> {
        let response = self
            .with_timeout(
                "upload",
                timeout,
                self.http
                    .put(&presigned.url)
                    .header("Content-Type", "application/octet-stream")
                    .body(data)
                    .send(),
            )
            .await?;

        self.check_or_read_body(response, "upload").await?;
        Ok(())
    }

    /// Download bytes from S3 using a presigned GET URL.
    ///
    /// Returns `None` if the object doesn't exist (404).
    ///
    /// The response **body** timeout scales with `Content-Length` when present
    /// (same budget as snapshot upload) so multi-hundred-MB `latest.enc`
    /// restores are not killed by the small log-entry 30s default.
    pub async fn download(&self, presigned: &PresignedUrl) -> SyncResult<Option<Vec<u8>>> {
        self.download_limited(presigned, None).await
    }

    /// Download a small mutable object together with its storage version.
    ///
    /// The ETag is needed by [`Self::upload_conditional`] to turn a remote
    /// read-modify-write into a real compare-and-swap. Large snapshot callers
    /// should keep using [`Self::download`] and its capped streaming path.
    pub async fn download_with_etag(
        &self,
        presigned: &PresignedUrl,
    ) -> SyncResult<Option<(Vec<u8>, Option<String>)>> {
        let response = self
            .with_timeout(
                "download",
                self.request_timeout,
                self.http.get(&presigned.url).send(),
            )
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = self.check_or_read_body(response, "download").await?;
        let etag = response
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let content_len = response.content_length().unwrap_or(0) as usize;
        let body_timeout =
            self.snapshot_transfer_timeout(content_len.max(1), presigned.expires_in_secs);
        let bytes = self
            .with_timeout("download response body", body_timeout, response.bytes())
            .await?;
        Ok(Some((bytes.to_vec(), etag)))
    }

    /// Download a byte range from S3 using a presigned GET URL.
    pub async fn download_range(
        &self,
        presigned: &PresignedUrl,
        offset: u64,
        len: u64,
    ) -> SyncResult<Option<Vec<u8>>> {
        if len == 0 {
            return Ok(Some(Vec::new()));
        }
        let end = offset.saturating_add(len).saturating_sub(1);
        let range = format!("bytes={offset}-{end}");
        let response = self
            .with_timeout(
                "range download",
                self.request_timeout,
                self.http
                    .get(&presigned.url)
                    .header(reqwest::header::RANGE, range)
                    .send(),
            )
            .await?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        let response = self.check_or_read_body(response, "range download").await?;
        let bytes = self
            .with_timeout(
                "range download response body",
                self.request_timeout,
                response.bytes(),
            )
            .await?;
        if bytes.len() as u64 != len {
            return Err(SyncError::S3(format!(
                "range download returned {} bytes, expected {len}",
                bytes.len()
            )));
        }
        Ok(Some(bytes.to_vec()))
    }

    /// Like [`Self::download`], but refuse objects larger than `max_bytes`
    /// (when `Some`) without buffering more than `max_bytes` into process RSS.
    ///
    /// - When `Content-Length` is present and exceeds the cap, the response is
    ///   dropped **before** any body bytes are read.
    /// - When `Content-Length` is missing/zero, the body is read **chunk by
    ///   chunk** and aborted as soon as the accumulated size would exceed the
    ///   cap — a poison multi-GB object that omits/strips `Content-Length`
    ///   cannot pin process RSS at the full object size.
    pub async fn download_limited(
        &self,
        presigned: &PresignedUrl,
        max_bytes: Option<usize>,
    ) -> SyncResult<Option<Vec<u8>>> {
        let response = self
            .with_timeout(
                "download",
                self.request_timeout,
                self.http.get(&presigned.url).send(),
            )
            .await?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        let response = self.check_or_read_body(response, "download").await?;

        let content_len = response.content_length().unwrap_or(0) as usize;
        if let Some(max) = max_bytes {
            if content_len > 0 && content_len > max {
                // Drop the response without reading the body.
                drop(response);
                return Err(SyncError::S3(format!(
                    "object content-length {content_len} exceeds max_download_entry_bytes {max}"
                )));
            }
        }

        let body_timeout = if content_len > 0 {
            self.snapshot_transfer_timeout(content_len, presigned.expires_in_secs)
        } else {
            // Unknown size: still allow large transfers (presigned expiry cap),
            // but if a max is set, prefer a tighter wall-clock budget so we
            // don't stream multi-GB into RAM for minutes.
            let assumed = max_bytes.unwrap_or(512 * 1024 * 1024).max(1);
            self.snapshot_transfer_timeout(assumed, presigned.expires_in_secs)
        };

        // Stream the body so a missing Content-Length still cannot pin RSS
        // above max_bytes. `response.bytes()` would fully buffer first.
        // Own timeout (not `with_timeout`) so cap-breach can return SyncError::S3.
        match tokio::time::timeout(body_timeout, Self::read_body_capped(response, max_bytes)).await
        {
            Ok(Ok(bytes)) => Ok(Some(bytes)),
            Ok(Err(err)) => Err(err),
            Err(_) => Err(SyncError::Network(format!(
                "S3 download response body timed out after {}",
                Self::timeout_label(body_timeout)
            ))),
        }
    }

    /// Read a successful response body, aborting as soon as `max_bytes` would
    /// be exceeded (so the in-memory buffer never grows past the cap by more
    /// than one stream chunk, which is then dropped without extend).
    async fn read_body_capped(
        response: reqwest::Response,
        max_bytes: Option<usize>,
    ) -> SyncResult<Vec<u8>> {
        let content_len = response.content_length().unwrap_or(0) as usize;
        let mut buf = if content_len > 0 {
            let reserve = match max_bytes {
                Some(max) => content_len.min(max),
                None => content_len,
            };
            Vec::with_capacity(reserve)
        } else {
            // Cap reserve when max is known so we never pre-allocate multi-GB.
            match max_bytes {
                Some(max) => Vec::with_capacity(max.min(64 * 1024)),
                None => Vec::new(),
            }
        };

        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk =
                chunk.map_err(|e| Self::classify_reqwest_error("download response body", &e))?;
            if let Some(max) = max_bytes {
                if buf.len().saturating_add(chunk.len()) > max {
                    // Drop the rest of the stream / response by dropping `stream`
                    // and `chunk` without extending `buf` past `max`.
                    return Err(SyncError::S3(format!(
                        "object body exceeds max_download_entry_bytes {max} (streamed; no Content-Length preflight)"
                    )));
                }
            }
            buf.extend_from_slice(&chunk);
        }
        Ok(buf)
    }

    /// Delete an S3 object using a presigned DELETE URL.
    pub async fn delete(&self, presigned: &PresignedUrl) -> SyncResult<()> {
        let response = self
            .with_timeout(
                "delete",
                self.request_timeout,
                self.http.delete(&presigned.url).send(),
            )
            .await?;

        self.check_or_read_body(response, "delete").await?;
        Ok(())
    }
}
