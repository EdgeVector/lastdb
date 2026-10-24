//! Presigned URLs for referenced-file blobs, thumbnails and thumbnail packs.

use super::super::helpers::{
    cas_presign_body, cas_presign_body_with_owner_scope, thumb_pack_presign_body,
    thumb_presign_body,
};
use super::super::AuthClient;
use super::presign::THUMB_MAX_ENCRYPTED_BYTES;
use crate::sync::error::{SyncError, SyncResult};
use crate::sync::s3::PresignedUrl;

impl AuthClient {
    /// Request a presigned URL for uploading a referenced-file blob.
    ///
    /// `file_hash` is the storage digest from `blob_ref`: ciphertext SHA-256 for
    /// the current suite, plaintext SHA-256 for older suites. The server
    /// gates on storage quota. `estimated_size_bytes` is the encrypted blob
    /// length, used for the quota pre-check.
    pub async fn presign_file_upload(
        &self,
        file_hash: &str,
        estimated_size_bytes: u64,
    ) -> SyncResult<PresignedUrl> {
        self.presign_file_upload_app(file_hash, estimated_size_bytes, None)
            .await
    }

    pub async fn presign_file_upload_app(
        &self,
        file_hash: &str,
        estimated_size_bytes: u64,
        app_id: Option<&str>,
    ) -> SyncResult<PresignedUrl> {
        self.presign_single_url(cas_presign_body(
            "presign_file_upload",
            file_hash,
            Some(estimated_size_bytes),
            app_id,
        ))
        .await
    }

    /// Request a presigned URL for downloading a referenced-file blob.
    pub async fn presign_file_download(&self, file_hash: &str) -> SyncResult<PresignedUrl> {
        self.presign_file_download_owner_scope(file_hash, None)
            .await
    }

    pub async fn presign_file_download_owner_scope(
        &self,
        file_hash: &str,
        owner_scope: Option<&str>,
    ) -> SyncResult<PresignedUrl> {
        self.presign_file_download_owner_scope_app(file_hash, owner_scope, None)
            .await
    }

    pub async fn presign_file_download_owner_scope_app(
        &self,
        file_hash: &str,
        owner_scope: Option<&str>,
        app_id: Option<&str>,
    ) -> SyncResult<PresignedUrl> {
        self.presign_single_url(cas_presign_body_with_owner_scope(
            "presign_file_download",
            file_hash,
            None,
            app_id,
            owner_scope,
        ))
        .await
    }

    /// Request a presigned URL for deleting a referenced-file blob.
    pub async fn presign_file_delete(&self, file_hash: &str) -> SyncResult<PresignedUrl> {
        self.presign_single_url(cas_presign_body(
            "presign_file_delete",
            file_hash,
            None,
            None,
        ))
        .await
    }

    /// Request a presigned URL for uploading one sealed loose thumbnail to
    /// `{scope}/thumbs/loose/sha256/{thumb_hash}` (R2). Fails fast client-side
    /// on the per-thumbnail byte cap; the server re-enforces it at presign
    /// (declared size) and at confirm/metering (real size).
    pub async fn presign_thumb_upload(
        &self,
        thumb_hash: &str,
        encrypted_size_bytes: u64,
    ) -> SyncResult<PresignedUrl> {
        if encrypted_size_bytes > THUMB_MAX_ENCRYPTED_BYTES {
            return Err(SyncError::QuotaExceeded(format!(
                "thumbnail is {encrypted_size_bytes} bytes, over the \
                 {THUMB_MAX_ENCRYPTED_BYTES}-byte per-thumbnail cap"
            )));
        }
        self.presign_single_url(thumb_presign_body(
            "presign_thumb_upload",
            thumb_hash,
            Some(encrypted_size_bytes),
        ))
        .await
    }

    /// Request a presigned URL for downloading one sealed loose thumbnail.
    pub async fn presign_thumb_download(&self, thumb_hash: &str) -> SyncResult<PresignedUrl> {
        self.presign_single_url(thumb_presign_body(
            "presign_thumb_download",
            thumb_hash,
            None,
        ))
        .await
    }

    /// Request a presigned URL for deleting one sealed loose thumbnail
    /// (pack-cadence GC of packed/orphaned loose objects).
    pub async fn presign_thumb_delete(&self, thumb_hash: &str) -> SyncResult<PresignedUrl> {
        self.presign_single_url(thumb_presign_body("presign_thumb_delete", thumb_hash, None))
            .await
    }

    /// Request a presigned URL for uploading a thumbnail pack to
    /// `{scope}/thumbs/packs/{pack_id}`. Packs are exempt from the loose cap.
    pub async fn presign_thumb_pack_upload(
        &self,
        pack_id: &str,
        estimated_size_bytes: u64,
    ) -> SyncResult<PresignedUrl> {
        self.presign_single_url(thumb_pack_presign_body(
            "presign_thumb_pack_upload",
            pack_id,
            Some(estimated_size_bytes),
        ))
        .await
    }

    /// Request a presigned URL for downloading a thumbnail pack.
    pub async fn presign_thumb_pack_download(&self, pack_id: &str) -> SyncResult<PresignedUrl> {
        self.presign_single_url(thumb_pack_presign_body(
            "presign_thumb_pack_download",
            pack_id,
            None,
        ))
        .await
    }

    /// Request a presigned URL for deleting a superseded thumbnail pack.
    pub async fn presign_thumb_pack_delete(&self, pack_id: &str) -> SyncResult<PresignedUrl> {
        self.presign_single_url(thumb_pack_presign_body(
            "presign_thumb_pack_delete",
            pack_id,
            None,
        ))
        .await
    }
}
