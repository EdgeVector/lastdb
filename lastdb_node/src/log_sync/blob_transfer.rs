//! Moving a blob between a file and the relay without holding it in memory.
//!
//! `blob.put` streams the file to the presigned upload URL in chunks via
//! positional reads (`read_at`). `blob.get` (in [`download`]) streams the
//! presigned download into a part file next to the destination, hashing as the
//! bytes pass, and renames it into place only when the size and SHA-256 match.
//! Memory use does not depend on the size of the blob.
//!
//! A transfer fails when the connection stalls, not when it is slow (see [`relay_transfer`],
//! the one uploader of the node and the relay e2e proof).

mod download;
mod reclaim;
mod url_guard;

pub(super) use reclaim::{Dropped, HeadAt, DROP_BATCH, DROP_HEADS};
use relay_transfer::UploadSource;
pub(super) use relay_transfer::{
    describe, is_retryable_store_status, upload_backoff, UPLOAD_TRIES,
};

use std::path::Path;
use std::sync::Arc;

use relay_protocol::{BlobPut, BlobPutRequest, BlobPutStage, ErrorCode};
use reqwest::header::{HeaderName, HeaderValue};
use tokio::fs;
use tokio::io::AsyncSeekExt;

use super::relay::{CallError, RelayClient};

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

enum RawSource<'a> {
    File(fs::File, u64),
    Bytes(&'a [u8]),
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
        self.put_source(database_id, sha256, size, RawSource::File(file, start))
            .await
    }

    /// Put `data` on the relay in memory without staging a temporary file on disk.
    pub(super) async fn blob_put_bytes(
        &self,
        database_id: &str,
        sha256: &str,
        data: &[u8],
    ) -> Result<(), String> {
        self.put_source(
            database_id,
            sha256,
            data.len() as u64,
            RawSource::Bytes(data),
        )
        .await
    }

    async fn put_source(
        &self,
        database_id: &str,
        sha256: &str,
        size: u64,
        raw: RawSource<'_>,
    ) -> Result<(), String> {
        let Some((url, headers)) = self.begin_upload(database_id, sha256, size).await? else {
            return Ok(());
        };
        let source = match raw {
            RawSource::File(file, start) => UploadSource::File {
                file: Arc::new(file.into_std().await),
                start,
                size,
            },
            RawSource::Bytes(data) => UploadSource::Bytes(Arc::from(data)),
        };
        self.upload_with_retries(&url, &headers, &source, size)
            .await?;
        self.commit_blob(database_id, sha256, size).await
    }

    async fn begin_upload(
        &self,
        database_id: &str,
        sha256: &str,
        size: u64,
    ) -> Result<Option<(String, Vec<(HeaderName, HeaderValue)>)>, String> {
        let request = BlobPutRequest {
            database_id: database_id.to_owned(),
            sha256: sha256.to_owned(),
            bytes: size,
            stage: BlobPutStage::Begin,
        };
        let begin = self
            .call_tracked::<BlobPut>(&request)
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
            return Ok(None);
        }
        let upload = begin.upload.ok_or("blob.put begin has no upload.url")?;
        url_guard::validate_presigned_url(&self.base, &upload.url)?;
        let headers = url_guard::presigned_headers(&upload.headers, true)?;
        Ok(Some((upload.url, headers)))
    }

    async fn upload_with_retries(
        &self,
        url: &str,
        headers: &[(HeaderName, HeaderValue)],
        source: &UploadSource,
        size: u64,
    ) -> Result<(), String> {
        let warn = |tried: u32, attempt: &relay_transfer::Attempt| {
            tracing::warn!(
                target: "lastdbd::log_sync",
                outcome = %attempt.outcome,
                seconds = attempt.seconds,
                sent_bytes = attempt.sent_bytes,
                tried,
                "blob upload got a transient store status or no answer; sending the blob again"
            );
        };
        let uploaded =
            relay_transfer::put_presigned(&self.transfer, url, headers, source, size, &warn)
                .await
                .map_err(|failed| failed.message)?;
        let status = uploaded.status;
        if status.is_redirection() {
            return Err(format!("blob upload unexpected redirect HTTP {status}"));
        }
        url_guard::validate_presigned_url(&self.base, &uploaded.final_url)?;
        if !status.is_success() {
            return Err(format!("blob upload HTTP {status}"));
        }
        Ok(())
    }

    async fn commit_blob(&self, database_id: &str, sha256: &str, size: u64) -> Result<(), String> {
        let request = BlobPutRequest {
            database_id: database_id.to_owned(),
            sha256: sha256.to_owned(),
            bytes: size,
            stage: BlobPutStage::Commit,
        };
        if let Some(usage) = &self.usage {
            usage.blob_begins(database_id, sha256, size)?;
        }
        match self.call_tracked::<BlobPut>(&request).await {
            Ok(_) => {
                if let Some(usage) = &self.usage {
                    usage.blob_ends(database_id, sha256, size, true)?;
                }
                Ok(())
            }
            Err(error) => {
                let refused = error
                    .status
                    .is_some_and(|status| matches!(status, 400 | 403 | 404 | 409));
                if let Some(usage) = self.usage.as_ref().filter(|_| refused) {
                    usage.blob_ends(database_id, sha256, size, false)?;
                }
                Err(error.message)
            }
        }
    }
}
