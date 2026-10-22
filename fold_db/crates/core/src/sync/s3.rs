use super::error::{redact_sync_error_text, SyncError, SyncResult};
use futures::stream::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::io::ReaderStream;

#[path = "s3/cloud_completion.rs"]
mod cloud_completion;
#[path = "s3/delete.rs"]
mod delete;
#[path = "s3/presigned.rs"]
mod presigned;
pub use presigned::PresignedUrl;
#[path = "s3/curl_transport.rs"]
mod curl_transport;
use curl_transport::{
    check_curl_upload_output, upload_backup_prefix_via_curl_blocking,
    upload_snapshot_file_via_curl_blocking,
};

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

/// A rescue PUT URL. The service signs the create-only condition with the URL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresignedCreateOnlyUrl {
    pub url: String,
    pub method: String,
    pub expires_in_secs: u64,
    pub headers: BTreeMap<String, String>,
}

impl S3Client {
    fn cloud_url(url: &str) -> SyncResult<reqwest::Url> {
        let parsed = reqwest::Url::parse(url)
            .map_err(|_| SyncError::Network("invalid cloud transfer URL".into()))?;
        if parsed.scheme() != "https"
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
        {
            return Err(SyncError::Network("cloud transfers require HTTPS".into()));
        }
        Ok(parsed)
    }
    fn rescue_headers(signed: &PresignedCreateOnlyUrl) -> SyncResult<reqwest::header::HeaderMap> {
        if signed.method != "PUT"
            || (!crate::sync::upload_completion::is_multipart(&signed.url)
                && signed.headers.get("If-None-Match").map(String::as_str) != Some("*"))
            || signed.expires_in_secs == 0
        {
            return Err(SyncError::Storage(
                "rescue upload lacks a signed create-only PUT".into(),
            ));
        }
        let mut headers = reqwest::header::HeaderMap::new();
        for (name, value) in &signed.headers {
            if crate::sync::upload_completion::is_multipart(&signed.url)
                && name.eq_ignore_ascii_case("If-None-Match")
            {
                continue;
            }
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
                    .put(Self::cloud_url(&signed.url)?)
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
        self.complete_upload_response(
            &PresignedUrl::provider(
                signed.url.clone(),
                signed.method.clone(),
                signed.expires_in_secs,
            ),
            response,
            None,
            true,
        )
        .await
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
                    .put(Self::cloud_url(&signed.url)?)
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
        self.complete_upload_response(
            &PresignedUrl::provider(
                signed.url.clone(),
                signed.method.clone(),
                signed.expires_in_secs,
            ),
            response,
            None,
            true,
        )
        .await
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
}

#[path = "s3/download.rs"]
mod download;
#[path = "s3/upload.rs"]
mod upload;
