//! Presigned GET downloads with Content-Length-scaled timeouts and byte caps.

use super::*;

impl S3Client {
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
                self.http.get(Self::cloud_url(&presigned.url)?).send(),
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
                    .get(Self::cloud_url(&presigned.url)?)
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

    /// Optional header and streaming cap; retain the cap-based timeout estimate.
    pub async fn download_limited(
        &self,
        presigned: &PresignedUrl,
        max_bytes: Option<usize>,
    ) -> SyncResult<Option<Vec<u8>>> {
        self.download_bounded(presigned, max_bytes, max_bytes.unwrap_or(512 * 1024 * 1024))
            .await
    }

    /// Bound a listed immutable object without changing the uncapped timeout.
    pub(crate) async fn download_listed(
        &self,
        presigned: &PresignedUrl,
        listed_bytes: usize,
    ) -> SyncResult<Option<Vec<u8>>> {
        self.download_bounded(presigned, Some(listed_bytes), 512 * 1024 * 1024)
            .await
    }

    async fn download_bounded(
        &self,
        presigned: &PresignedUrl,
        max_bytes: Option<usize>,
        unknown_length_bytes: usize,
    ) -> SyncResult<Option<Vec<u8>>> {
        let response = self
            .with_timeout(
                "download",
                self.request_timeout,
                self.http.get(Self::cloud_url(&presigned.url)?).send(),
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
            // Keep the caller's unknown-length timeout estimate independent of the cap.
            let assumed = unknown_length_bytes.max(1);
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
    pub(super) async fn read_body_capped(
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
}
