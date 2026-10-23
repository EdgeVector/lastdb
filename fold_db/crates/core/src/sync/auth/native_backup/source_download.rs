//! Exact disk output from one already authorized consumer-copy grant.
use super::*;
use cloud_backup_protocol::source::{SourceReadGrant, SourceReadSelector};
use futures::StreamExt;
use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use tokio::io::AsyncReadExt;
use tokio_util::io::StreamReader;

impl NativeBackupClient {
    pub(super) async fn download_source(
        &self,
        selector: &SourceReadSelector,
        grant: &SourceReadGrant,
        path: &Path,
    ) -> Result<()> {
        grant.validate_for(selector)?;
        let url = urls::signed_headers(grant.url.as_str(), "host;if-match")?;
        let condition =
            reqwest::header::HeaderValue::from_bytes(grant.if_match.as_str().as_bytes())
                .map_err(|_| NativeBackupError::InvalidResponse("file source condition"))?;
        let budget = self.transfer_timeout(grant.bytes.get(), grant.expires_ms.get())?;
        tokio::time::timeout(
            budget,
            self.copy_source(url, condition, grant.bytes.get(), path),
        )
        .await
        .map_err(|_| NativeBackupError::Timeout)?
    }

    async fn copy_source(
        &self,
        url: Url,
        condition: reqwest::header::HeaderValue,
        expected: u64,
        path: &Path,
    ) -> Result<()> {
        let _permit = Arc::clone(&self.uploads)
            .acquire_owned()
            .await
            .map_err(|_| NativeBackupError::Task)?;
        let response = self
            .http
            .get(url)
            .header(reqwest::header::IF_MATCH, condition)
            .send()
            .await
            .map_err(|_| NativeBackupError::Transport)?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(NativeBackupError::Http(response.status().as_u16()));
        }
        if response
            .content_length()
            .is_some_and(|bytes| bytes != expected)
        {
            return Err(NativeBackupError::Integrity("file source length"));
        }
        // Synchronous bounded writes finish before the future can be cancelled;
        // no detached disk writer outlives the caller's source/capacity guards.
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .map_err(|_| NativeBackupError::Source)?;
        copy_response(response, &mut file, expected).await?;
        file.sync_all().map_err(|_| NativeBackupError::Source)
    }
}

async fn copy_response(
    response: reqwest::Response,
    file: &mut std::fs::File,
    expected: u64,
) -> Result<()> {
    let stream = response
        .bytes_stream()
        .map(|item| item.map_err(|_| std::io::Error::other("native source transport failed")));
    let mut input = StreamReader::new(stream);
    let mut buffer = vec![0; lastdb_storage_policy::BACKUP_RESOURCES.stream_buffer_bytes as usize];
    let mut copied = 0u64;
    loop {
        let count = input
            .read(&mut buffer)
            .await
            .map_err(|_| NativeBackupError::Transport)?;
        if count == 0 {
            break;
        }
        copied = copied
            .checked_add(count as u64)
            .filter(|bytes| *bytes <= expected)
            .ok_or(NativeBackupError::Integrity("file source length"))?;
        file.write_all(&buffer[..count])
            .map_err(|_| NativeBackupError::Source)?;
    }
    if copied != expected {
        return Err(NativeBackupError::Integrity("file source truncated"));
    }
    Ok(())
}
