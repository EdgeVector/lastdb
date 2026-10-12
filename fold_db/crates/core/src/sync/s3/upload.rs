//! Presigned PUT uploads: log entries, snapshots, and sealed backup chunks.

use super::*;

impl S3Client {
    pub async fn upload(&self, presigned: &PresignedUrl, data: Vec<u8>) -> SyncResult<()> {
        self.upload_with_timeout(presigned, data, self.request_timeout)
            .await
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
    /// Keep the reqwest path for small files.
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
                    .put(Self::cloud_url(&presigned.url)?)
                    .header("Content-Type", "application/octet-stream")
                    .header(reqwest::header::CONTENT_LENGTH, len)
                    .version(reqwest::Version::HTTP_11)
                    .body(body)
                    .send(),
            )
            .await?;

        self.complete_upload_response(presigned, response, None, false)
            .await?;
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
            return self.complete_curl_upload(presigned, output).await;
        }
        let source = tokio::fs::File::from_std(source).take(bytes);
        let body = reqwest::Body::wrap_stream(ReaderStream::new(source));
        let response = self
            .with_timeout(
                "upload",
                timeout,
                self.http
                    .put(Self::cloud_url(&presigned.url)?)
                    .header("Content-Type", "application/octet-stream")
                    .header(reqwest::header::CONTENT_LENGTH, bytes)
                    .version(reqwest::Version::HTTP_11)
                    .body(body)
                    .send(),
            )
            .await?;
        self.complete_upload_response(presigned, response, None, false)
            .await?;
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

        self.complete_curl_upload(presigned, output?).await
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
                    .put(Self::cloud_url(&presigned.url)?)
                    .header("Content-Type", "application/octet-stream")
                    .body(data)
                    .send(),
            )
            .await?;

        self.complete_upload_response(presigned, response, None, false)
            .await?;
        Ok(())
    }
}
