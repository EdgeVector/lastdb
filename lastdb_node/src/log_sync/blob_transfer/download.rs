//! Presigned blob download (`blob.get`) with bounded retries and SHA-256 verification.

use std::collections::BTreeMap;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use app_identity_crypto::hex_lower;
use futures::StreamExt;
use relay_protocol::{BlobGet, BlobGetRequest};
use reqwest::header::{HeaderName, HeaderValue};
use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::io::AsyncWriteExt;

use super::{describe, is_retryable_store_status, upload_backoff, url_guard, UPLOAD_TRIES};
use crate::log_sync::relay::RelayClient;
use crate::profile::private_fs as disk;

enum DownloadError {
    Retryable(String),
    Fatal(String),
}

impl RelayClient {
    /// Fetch a blob into the file `dest`: `blob.get`, then the presigned
    /// download streamed to disk. `dest` appears only when the download is
    /// `expect_size` bytes and has digest `sha256`.
    pub(in crate::log_sync) async fn blob_get(
        &self,
        database_id: &str,
        sha256: &str,
        expect_size: u64,
        dest: &Path,
    ) -> Result<(), String> {
        mutation_log::Hex32::from_hex(sha256)
            .map_err(|_| "blob digest is not 64 lowercase hex characters".to_owned())?;
        let request = BlobGetRequest {
            database_id: database_id.to_owned(),
            sha256: sha256.to_owned(),
        };
        let meta = self.call::<BlobGet>(&request).await?;
        let url = meta.url.as_str();
        url_guard::validate_presigned_url(&self.base, url)?;
        let headers = url_guard::presigned_headers(&meta.headers, false)?;
        if meta.bytes != expect_size {
            return Err(format!("blob.get size differs from its entry for {sha256}"));
        }
        let part = part_path(dest, sha256);
        let mut tried = 0;
        loop {
            tried += 1;
            match self
                .get_once(url, &headers, &part, sha256, expect_size)
                .await
            {
                Ok(()) => {
                    return fs::rename(&part, dest)
                        .await
                        .map_err(|e| format!("blob file: {e}"));
                }
                Err(DownloadError::Retryable(error)) if tried < UPLOAD_TRIES => {
                    cleanup_part(&part).await;
                    tracing::warn!(
                        target: "lastdbd::log_sync",
                        %error,
                        tried,
                        "blob download failed transiently; retrying"
                    );
                    tokio::time::sleep(upload_backoff(tried)).await;
                }
                Err(DownloadError::Retryable(error) | DownloadError::Fatal(error)) => {
                    cleanup_part(&part).await;
                    return Err(error);
                }
            }
        }
    }

    /// The bytes of the blob at a presigned URL that a `log.read` named (a log segment), checked
    /// against `size` and `sha256`. A segment holds one sent batch, so it is read into memory.
    pub(in crate::log_sync) async fn fetch_bytes(
        &self,
        url: &str,
        headers: &BTreeMap<String, String>,
        sha256: &str,
        size: u64,
    ) -> Result<Vec<u8>, String> {
        url_guard::validate_presigned_url(&self.base, url)?;
        let headers = url_guard::presigned_headers(headers, false)?;
        let mut tried = 0;
        loop {
            tried += 1;
            match self.fetch_once(url, &headers, sha256, size).await {
                Ok(bytes) => return Ok(bytes),
                Err(DownloadError::Retryable(error)) if tried < UPLOAD_TRIES => {
                    tracing::warn!(
                        target: "lastdbd::log_sync",
                        %error,
                        tried,
                        "log segment download failed transiently; retrying"
                    );
                    tokio::time::sleep(upload_backoff(tried)).await;
                }
                Err(DownloadError::Retryable(error) | DownloadError::Fatal(error)) => {
                    return Err(error)
                }
            }
        }
    }

    async fn fetch_once(
        &self,
        url: &str,
        headers: &[(HeaderName, HeaderValue)],
        sha256: &str,
        size: u64,
    ) -> Result<Vec<u8>, DownloadError> {
        let mut download = self.download.get(url);
        for (name, value) in headers {
            download = download.header(name, value);
        }
        let response = download.send().await.map_err(|e| {
            DownloadError::Retryable(format!("log segment download: {}", describe(e, url)))
        })?;
        let status = response.status();
        if status.is_redirection() {
            return Err(DownloadError::Fatal(format!(
                "log segment download unexpected redirect HTTP {status}"
            )));
        }
        url_guard::validate_presigned_url(&self.base, response.url().as_str())
            .map_err(DownloadError::Fatal)?;
        if !status.is_success() {
            let msg = format!("log segment download HTTP {status}");
            return if is_retryable_store_status(status) {
                Err(DownloadError::Retryable(msg))
            } else {
                Err(DownloadError::Fatal(msg))
            };
        }
        let mut bytes = Vec::with_capacity(usize::try_from(size).unwrap_or(0));
        let mut chunks = response.bytes_stream();
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk.map_err(|e| {
                DownloadError::Retryable(format!(
                    "log segment download body: {} after {} of {size} bytes",
                    describe(e, url),
                    bytes.len()
                ))
            })?;
            if (bytes.len() + chunk.len()) as u64 > size {
                return Err(DownloadError::Fatal(format!(
                    "log segment {sha256} is larger than the relay said"
                )));
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() as u64 != size {
            return Err(DownloadError::Retryable(format!(
                "log segment {sha256} is shorter than the relay said"
            )));
        }
        if hex_lower(&Sha256::digest(&bytes)) != sha256 {
            return Err(DownloadError::Fatal(format!(
                "log segment {sha256} does not match its digest"
            )));
        }
        Ok(bytes)
    }

    async fn get_once(
        &self,
        url: &str,
        headers: &[(HeaderName, HeaderValue)],
        part: &Path,
        sha256: &str,
        expect_size: u64,
    ) -> Result<(), DownloadError> {
        let mut download = self.download.get(url);
        for (name, value) in headers {
            download = download.header(name, value);
        }
        let response = download.send().await.map_err(|e| {
            DownloadError::Retryable(format!("blob download: {}", describe(e, url)))
        })?;
        let status = response.status();
        if status.is_redirection() {
            return Err(DownloadError::Fatal(format!(
                "blob download unexpected redirect HTTP {status}"
            )));
        }
        url_guard::validate_presigned_url(&self.base, response.url().as_str())
            .map_err(DownloadError::Fatal)?;
        if !status.is_success() {
            let msg = format!("blob download HTTP {status}");
            return if is_retryable_store_status(status) {
                Err(DownloadError::Retryable(msg))
            } else {
                Err(DownloadError::Fatal(msg))
            };
        }
        write_verified(response, part, sha256, expect_size, url).await
    }
}

fn part_path(dest: &Path, sha256: &str) -> PathBuf {
    let suffix = uuid::Uuid::new_v4().simple();
    dest.with_file_name(format!(".{sha256}.{suffix}.part"))
}

fn open_part(part: &Path) -> std::io::Result<std::fs::File> {
    if let Some(parent) = part.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    disk::remove_private(part)?;
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(part)?;
    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    Ok(file)
}

async fn cleanup_part(part: &Path) {
    let path = part.to_path_buf();
    let _ = tokio::task::spawn_blocking(move || disk::remove_private(&path)).await;
}

/// Stream `response` into `part` (private file mode), then check size and digest.
async fn write_verified(
    response: reqwest::Response,
    part: &Path,
    sha256: &str,
    expect_size: u64,
    url: &str,
) -> Result<(), DownloadError> {
    let target = part.to_path_buf();
    let std_file = tokio::task::spawn_blocking(move || open_part(&target))
        .await
        .map_err(|e| DownloadError::Fatal(format!("blob file: {e}")))?
        .map_err(|e| DownloadError::Fatal(format!("blob file: {e}")))?;
    let mut file = fs::File::from_std(std_file);
    let mut hasher = Sha256::new();
    let mut count = 0u64;
    let mut chunks = response.bytes_stream();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(|e| {
            DownloadError::Retryable(format!(
                "blob download body: {} after {count} of {expect_size} bytes",
                describe(e, url)
            ))
        })?;
        count += chunk.len() as u64;
        if count > expect_size {
            return Err(DownloadError::Fatal(format!(
                "blob download is larger than its entry for {sha256}"
            )));
        }
        hasher.update(&chunk);
        file.write_all(&chunk)
            .await
            .map_err(|e| DownloadError::Fatal(format!("blob file: {e}")))?;
    }
    file.sync_all()
        .await
        .map_err(|e| DownloadError::Fatal(format!("blob file: {e}")))?;
    if count != expect_size {
        return Err(DownloadError::Retryable(format!(
            "blob download is shorter than its entry for {sha256}"
        )));
    }
    if hex_lower(&hasher.finalize()) != sha256 {
        return Err(DownloadError::Fatal(
            "bundle blob does not match its digest".into(),
        ));
    }
    Ok(())
}
