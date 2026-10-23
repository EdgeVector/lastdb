//! Exact authorized downloads into caller-owned private disk files.

use super::super::{PresignedUrl, S3Client, SyncError, SyncResult};
use futures::StreamExt;
use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use tokio::io::AsyncReadExt;
use tokio_util::io::StreamReader;

impl S3Client {
    /// The caller reserves disk space and owns cleanup of partial private files.
    /// A 404 returns None without creating a file. Success syncs exact bytes.
    pub async fn download_to_file_limited(
        &self,
        presigned: &PresignedUrl,
        path: &Path,
        expected_bytes: u64,
    ) -> SyncResult<Option<()>> {
        if presigned.method != "GET" || expected_bytes == 0 || presigned.expires_in_secs == 0 {
            return Err(SyncError::S3("invalid exact file download grant".into()));
        }
        let response = self
            .with_timeout(
                "file download",
                self.request_timeout,
                self.http.get(Self::cloud_url(&presigned.url)?).send(),
            )
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if response.status() != reqwest::StatusCode::OK
            || response
                .content_length()
                .is_some_and(|bytes| bytes != expected_bytes)
        {
            return Err(SyncError::S3(
                "file download status or length differs".into(),
            ));
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)?;
        let size = usize::try_from(expected_bytes)
            .map_err(|_| SyncError::S3("file download exceeds platform size".into()))?;
        let timeout = self.snapshot_transfer_timeout(size, presigned.expires_in_secs);
        tokio::time::timeout(timeout, copy_response(response, &mut file, expected_bytes))
            .await
            .map_err(|_| SyncError::Network("file download body timed out".into()))??;
        file.sync_all()?;
        Ok(Some(()))
    }
}

async fn copy_response(
    response: reqwest::Response,
    file: &mut std::fs::File,
    expected_bytes: u64,
) -> SyncResult<()> {
    let stream = response.bytes_stream().map(|result| {
        result.map_err(|error| {
            std::io::Error::other(S3Client::classify_reqwest_error(
                "file download body",
                &error,
            ))
        })
    });
    let mut source = StreamReader::new(stream);
    let mut buffer = vec![0; cloud_backup_protocol::limits::STREAM_BUFFER_MAX_BYTES as usize];
    let mut copied = 0u64;
    loop {
        let count = source.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        copied = copied
            .checked_add(count as u64)
            .filter(|bytes| *bytes <= expected_bytes)
            .ok_or_else(|| SyncError::S3("file download body exceeds exact length".into()))?;
        file.write_all(&buffer[..count])?;
    }
    if copied != expected_bytes {
        return Err(SyncError::S3(
            "file download body is shorter than expected".into(),
        ));
    }
    Ok(())
}
