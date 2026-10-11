//! Presigned URLs and presence checks for backup chunks and manifests.

use super::super::AuthClient;
use super::presign::BackupUploadPresign;
use crate::sync::error::{SyncError, SyncResult};
use crate::sync::s3::PresignedUrl;

impl AuthClient {
    pub async fn presign_backup_chunk_upload(
        &self,
        chunk_sha256: &str,
        estimated_size_bytes: u64,
    ) -> SyncResult<BackupUploadPresign> {
        self.presign_backup_upload(
            serde_json::json!({
                "action": "presign_backup_chunk_upload",
                "chunk_sha256": chunk_sha256,
                "estimated_size_bytes": estimated_size_bytes,
            }),
            "presign_backup_chunk_upload",
        )
        .await
    }

    pub async fn presign_backup_manifest_upload(
        &self,
        manifest_sha256: &str,
        estimated_size_bytes: u64,
    ) -> SyncResult<BackupUploadPresign> {
        self.presign_backup_upload(
            serde_json::json!({
                "action": "presign_backup_manifest_upload",
                "manifest_sha256": manifest_sha256,
                "estimated_size_bytes": estimated_size_bytes,
            }),
            "presign_backup_manifest_upload",
        )
        .await
    }

    pub async fn presign_backup_chunk_download(
        &self,
        chunk_sha256: &str,
    ) -> SyncResult<PresignedUrl> {
        self.presign_single_url(serde_json::json!({
            "action": "presign_backup_chunk_download",
            "chunk_sha256": chunk_sha256,
        }))
        .await
    }

    /// Presign DELETE for one content-addressed backup chunk (orphan GC path).
    /// Did not exist before 2026-07-31 — client+Lambda both lacked a chunk
    /// delete action, so unreclaimed cloud objects accumulated indefinitely.
    pub async fn presign_backup_chunk_delete(
        &self,
        chunk_sha256: &str,
    ) -> SyncResult<PresignedUrl> {
        self.presign_single_url(serde_json::json!({
            "action": "presign_backup_chunk_delete",
            "chunk_sha256": chunk_sha256,
        }))
        .await
    }

    pub async fn presign_backup_manifest_download(
        &self,
        manifest_sha256: &str,
    ) -> SyncResult<PresignedUrl> {
        self.presign_single_url(serde_json::json!({
            "action": "presign_backup_manifest_download",
            "manifest_sha256": manifest_sha256,
        }))
        .await
    }

    /// Returns `Ok(true)` when the chunk is confirmed present in cloud backup
    /// storage, `Ok(false)` when the server confirmed it is not. An `Err`
    /// means presence could not be determined (transport/auth/server
    /// failure) and callers must not treat that the same as a confirmed
    /// absence — see `probe_missing_manifest_chunk_shas` /
    /// `resolve_source_missing_via_cloud_presence`.
    pub async fn require_backup_chunk_present(&self, chunk_sha256: &str) -> SyncResult<bool> {
        self.require_backup_object_present(serde_json::json!({
            "action": "presign_backup_chunk_upload",
            "chunk_sha256": chunk_sha256,
            "estimated_size_bytes": 0_u64,
        }))
        .await
    }

    /// Same present/absent/undetermined contract as
    /// [`Self::require_backup_chunk_present`], for a manifest object.
    pub async fn require_backup_manifest_present(&self, manifest_sha256: &str) -> SyncResult<bool> {
        self.require_backup_object_present(serde_json::json!({
            "action": "presign_backup_manifest_upload",
            "manifest_sha256": manifest_sha256,
            "estimated_size_bytes": 0_u64,
        }))
        .await
    }

    async fn require_backup_object_present(&self, body: serde_json::Value) -> SyncResult<bool> {
        let response = self.presign_response(body).await?;
        Ok(response.already_present)
    }

    async fn presign_backup_upload(
        &self,
        body: serde_json::Value,
        action: &str,
    ) -> SyncResult<BackupUploadPresign> {
        let response = self.presign_response(body).await?;
        if response.already_present {
            return Ok(BackupUploadPresign {
                key: response.key,
                url: None,
                already_present: true,
            });
        }
        let url =
            response.urls.into_iter().next().ok_or_else(|| {
                SyncError::Storage(format!("{action}: no presigned URL returned"))
            })?;
        Ok(BackupUploadPresign {
            key: response.key,
            url: Some(url),
            already_present: false,
        })
    }
}
