//! Presigned blob download (`blob.get`) with bounded retries and SHA-256 verification.

use std::path::{Path, PathBuf};

use app_identity_crypto::hex_lower;
use futures::StreamExt;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::io::AsyncWriteExt;

use super::{describe, is_retryable_store_status, upload_backoff, UPLOAD_TRIES};
use crate::log_sync::relay::{header_pairs, RelayClient};

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
        let body = json!({ "database_id": database_id, "sha256": sha256 });
        let meta = self.post("/relay/v1/blob/get", database_id, &body).await?;
        let url = meta["url"].as_str().ok_or("blob.get has no url")?;
        if meta["bytes"].as_u64().is_some_and(|n| n != expect_size) {
            return Err(format!("blob.get size differs from its entry for {sha256}"));
        }
        let part = part_path(dest, sha256);
        let mut tried = 0;
        loop {
            tried += 1;
            match self
                .get_once(url, &meta["headers"], &part, sha256, expect_size)
                .await
            {
                Ok(()) => {
                    return fs::rename(&part, dest)
                        .await
                        .map_err(|e| format!("blob file: {e}"));
                }
                Err(DownloadError::Retryable(error)) if tried < UPLOAD_TRIES => {
                    let _ = fs::remove_file(&part).await;
                    tracing::warn!(
                        target: "lastdbd::log_sync",
                        %error,
                        tried,
                        "blob download failed transiently; retrying"
                    );
                    tokio::time::sleep(upload_backoff(tried)).await;
                }
                Err(DownloadError::Retryable(error) | DownloadError::Fatal(error)) => {
                    let _ = fs::remove_file(&part).await;
                    return Err(error);
                }
            }
        }
    }

    async fn get_once(
        &self,
        url: &str,
        headers: &Value,
        part: &Path,
        sha256: &str,
        expect_size: u64,
    ) -> Result<(), DownloadError> {
        let mut download = self.download.get(url);
        for (name, value) in header_pairs(headers) {
            download = download.header(name, value);
        }
        let response = download.send().await.map_err(|e| {
            DownloadError::Retryable(format!("blob download: {}", describe(e, url)))
        })?;
        let status = response.status();
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
    dest.with_file_name(format!(".{sha256}.part"))
}

/// Stream `response` into `part` (private file mode), then check size and digest.
async fn write_verified(
    response: reqwest::Response,
    part: &Path,
    sha256: &str,
    expect_size: u64,
    url: &str,
) -> Result<(), DownloadError> {
    if let Some(parent) = part.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|e| DownloadError::Fatal(format!("blob file: {e}")))?;
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(part)
        .await
        .map_err(|e| DownloadError::Fatal(format!("blob file: {e}")))?;
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
