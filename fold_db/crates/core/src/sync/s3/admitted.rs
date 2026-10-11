//! Exact response metadata admits the full allocation before a body read.

use super::*;
use crate::sync::cloud_purge::memory::{self, Admission};

impl S3Client {
    pub(crate) async fn download_range_admitted(
        &self,
        presigned: &PresignedUrl,
        offset: u64,
        bytes: u64,
        admission: &Admission,
    ) -> SyncResult<Option<Vec<u8>>> {
        admission.check(memory::row_peak(bytes)?)?;
        let end = offset
            .checked_add(bytes)
            .and_then(|value| value.checked_sub(1))
            .ok_or_else(|| SyncError::S3("invalid admitted byte range".into()))?;
        let response = self
            .with_timeout(
                "range download",
                self.request_timeout,
                self.http
                    .get(Self::cloud_url(&presigned.url)?)
                    .header(reqwest::header::RANGE, format!("bytes={offset}-{end}"))
                    .send(),
            )
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if response.status() != reqwest::StatusCode::PARTIAL_CONTENT
            || response.content_length() != Some(bytes)
            || !response
                .headers()
                .get(reqwest::header::CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.starts_with(&format!("bytes {offset}-{end}/")))
        {
            return Err(SyncError::S3(
                "cloud replacement range response differs".into(),
            ));
        }
        let limit = usize::try_from(bytes)
            .map_err(|_| SyncError::S3("range exceeds platform size".into()))?;
        let timeout = self.snapshot_transfer_timeout(limit, presigned.expires_in_secs);
        let body = tokio::time::timeout(timeout, Self::read_body_capped(response, Some(limit)))
            .await
            .map_err(|_| SyncError::Network("cloud replacement range timeout".into()))??;
        if body.len() as u64 != bytes {
            return Err(SyncError::S3("range body size differs".into()));
        }
        Ok(Some(body))
    }

    pub(crate) async fn download_admitted(
        &self,
        presigned: &PresignedUrl,
        admission: &Admission,
        expected_bytes: Option<u64>,
        retained_peak: u64,
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
        if !response.status().is_success() {
            return Err(SyncError::S3(
                "cloud replacement download was refused".into(),
            ));
        }
        let bytes = response.content_length().ok_or_else(|| {
            SyncError::S3(
                "cloud replacement requires exact response size before memory admission".into(),
            )
        })?;
        if expected_bytes.is_some_and(|expected| expected != bytes) {
            return Err(SyncError::S3(
                "cloud replacement response size differs".into(),
            ));
        }
        // Includes JSON container/string expansion, normalization clones,
        // ciphertext/plaintext buffers and one compressed logical-row scratch.
        admission.check(memory::checked_sum(&[
            memory::checked_scale(bytes, 24)?,
            memory::row_peak(0)?,
            retained_peak,
        ])?)?;
        let limit = usize::try_from(bytes)
            .map_err(|_| SyncError::S3("cloud replacement exceeds platform size".into()))?;
        let timeout = self.snapshot_transfer_timeout(limit, presigned.expires_in_secs);
        let body = tokio::time::timeout(timeout, Self::read_body_capped(response, Some(limit)))
            .await
            .map_err(|_| SyncError::Network("cloud replacement body timeout".into()))??;
        if body.len() as u64 != bytes {
            return Err(SyncError::S3("cloud replacement body size differs".into()));
        }
        Ok(Some(body))
    }
}
