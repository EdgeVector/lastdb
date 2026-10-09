use super::super::helpers::{
    attach_target_scope, cas_presign_body, cas_presign_body_with_owner_scope,
    snapshot_presign_body, snapshot_presign_body_for_target, thumb_pack_presign_body,
    thumb_presign_body,
};
use super::super::{AuthClient, PresignedResponse};
use super::op_failed;
use crate::sync::error::{SyncError, SyncResult};
use crate::sync::s3::PresignedUrl;

/// Hard cap on one sealed loose thumbnail object. The tier exists so media
/// galleries render from local state on every device; the cap is what keeps
/// it from becoming a general file-smuggling route around B2 CAS metering.
/// Keep in sync with `exemem_common::storage::THUMB_MAX_ENCRYPTED_BYTES`.
pub const THUMB_MAX_ENCRYPTED_BYTES: u64 = 65_536;

#[derive(Debug, Clone)]
pub struct BackupUploadPresign {
    pub key: Option<String>,
    pub url: Option<PresignedUrl>,
    pub already_present: bool,
}

impl AuthClient {
    /// Presign a single URL: post the body, parse a PresignedResponse, extract one URL.
    async fn presign_single_url(&self, body: serde_json::Value) -> SyncResult<PresignedUrl> {
        self.presign_urls(body)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| SyncError::Storage("no presigned URL returned".to_string()))
    }

    async fn presign_single_url_legacy_personal(
        &self,
        body: serde_json::Value,
    ) -> SyncResult<PresignedUrl> {
        self.presign_response_without_default_db_hash(body)
            .await?
            .urls
            .into_iter()
            .next()
            .ok_or_else(|| SyncError::Storage("no presigned URL returned".to_string()))
    }

    /// Request a presigned URL for uploading a snapshot.
    pub async fn presign_snapshot_upload(&self, snapshot_name: &str) -> SyncResult<PresignedUrl> {
        self.presign_single_url(snapshot_presign_body(
            "presign_snapshot_upload",
            snapshot_name,
        ))
        .await
    }

    /// Request a presigned URL for uploading a snapshot under a sync target's prefix.
    pub async fn presign_snapshot_upload_for_target(
        &self,
        target: &crate::sync::org_sync::SyncTarget,
        snapshot_name: &str,
    ) -> SyncResult<PresignedUrl> {
        self.presign_single_url(snapshot_presign_body_for_target(
            "presign_snapshot_upload",
            snapshot_name,
            target,
        ))
        .await
    }

    /// Request a presigned URL for downloading a snapshot.
    pub async fn presign_snapshot_download(&self, snapshot_name: &str) -> SyncResult<PresignedUrl> {
        self.presign_single_url(snapshot_presign_body(
            "presign_snapshot_download",
            snapshot_name,
        ))
        .await
    }

    /// Presign a snapshot download from the legacy personal root even when the
    /// client defaults new requests to a db_hash root.
    pub async fn presign_snapshot_download_legacy_personal(
        &self,
        snapshot_name: &str,
    ) -> SyncResult<PresignedUrl> {
        self.presign_single_url_legacy_personal(snapshot_presign_body(
            "presign_snapshot_download",
            snapshot_name,
        ))
        .await
    }

    /// Request a presigned URL for downloading a snapshot under a sync target's prefix.
    pub async fn presign_snapshot_download_for_target(
        &self,
        target: &crate::sync::org_sync::SyncTarget,
        snapshot_name: &str,
    ) -> SyncResult<PresignedUrl> {
        self.presign_single_url(snapshot_presign_body_for_target(
            "presign_snapshot_download",
            snapshot_name,
            target,
        ))
        .await
    }

    /// Request a presigned URL to upload to another user's inbox
    pub async fn presign_inbox_upload(
        &self,
        target_user_hash: &str,
        file_name: &str,
    ) -> SyncResult<PresignedUrl> {
        self.presign_single_url(serde_json::json!({
            "action": "presign_inbox_upload",
            "target_user_hash": target_user_hash,
            "snapshot_name": file_name,
        }))
        .await
    }

    /// Request a presigned URL to download an item from your own inbox
    pub async fn presign_inbox_download(&self, file_name: &str) -> SyncResult<PresignedUrl> {
        self.presign_single_url(serde_json::json!({
            "action": "presign_inbox_download",
            "snapshot_name": file_name,
        }))
        .await
    }

    /// Presign a DELETE URL for removing an inbox object (e.g., accepted/declined invite).
    pub async fn presign_inbox_delete(&self, file_name: &str) -> SyncResult<PresignedUrl> {
        self.presign_single_url(serde_json::json!({
            "action": "presign_inbox_delete",
            "snapshot_name": file_name,
        }))
        .await
    }

    /// Request presigned URLs for deleting log entries.
    pub async fn presign_log_delete(&self, seq_numbers: &[u64]) -> SyncResult<Vec<PresignedUrl>> {
        self.presign_log_delete_for_target(None, seq_numbers).await
    }

    /// Request presigned URLs for deleting log entries under a sync target's prefix.
    pub async fn presign_log_delete_target(
        &self,
        target: &crate::sync::org_sync::SyncTarget,
        seq_numbers: &[u64],
    ) -> SyncResult<Vec<PresignedUrl>> {
        self.presign_log_delete_for_target(Some(target), seq_numbers)
            .await
    }

    pub async fn presign_log_delete_object_keys(
        &self,
        target: &crate::sync::org_sync::SyncTarget,
        seq_numbers: &[u64],
        log_object_keys: &[String],
    ) -> SyncResult<Vec<PresignedUrl>> {
        self.presign_log_urls_with_keys(
            "presign_log_delete",
            target,
            seq_numbers,
            log_object_keys,
            None,
        )
        .await
    }

    async fn presign_log_delete_for_target(
        &self,
        target: Option<&crate::sync::org_sync::SyncTarget>,
        seq_numbers: &[u64],
    ) -> SyncResult<Vec<PresignedUrl>> {
        match target {
            Some(target) => {
                self.presign_log_urls("presign_log_delete", target, seq_numbers, None)
                    .await
            }
            None => {
                self.presign_urls(serde_json::json!({
                    "action": "presign_log_delete",
                    "seq_numbers": seq_numbers,
                }))
                .await
            }
        }
    }

    /// Presign PUT URLs for ephemeral p2p deltas this device pushes to a peer.
    ///
    /// Keys land at `<user_hash>/p2p/<src_device>__<dst_device>/<seq>.enc` on
    /// R2 and are auto-expired after 24h by the bucket lifecycle rule. The
    /// caller's `user_hash` scope is enforced server-side; quota checks are
    /// skipped for this action (free for all plans).
    pub async fn presign_p2p_upload(
        &self,
        src_device: &str,
        dst_device: &str,
        seq_numbers: &[u64],
    ) -> SyncResult<Vec<PresignedUrl>> {
        self.presign_urls(serde_json::json!({
            "action": "presign_p2p_upload",
            "src_device": src_device,
            "dst_device": dst_device,
            "seq_numbers": seq_numbers,
        }))
        .await
    }

    /// Presign GET URLs for fetching ephemeral p2p deltas a peer pushed.
    pub async fn presign_p2p_download(
        &self,
        src_device: &str,
        dst_device: &str,
        seq_numbers: &[u64],
    ) -> SyncResult<Vec<PresignedUrl>> {
        self.presign_urls(serde_json::json!({
            "action": "presign_p2p_download",
            "src_device": src_device,
            "dst_device": dst_device,
            "seq_numbers": seq_numbers,
        }))
        .await
    }

    /// Presign a PUT URL for sending an encrypted bulletin board message.
    ///
    /// The path is `bulletin/<recipient_pseudonym>/<message_id>.enc`. The
    /// caller's account must be on a paid plan; the server enforces the
    /// gate via `BillingTable.is_paid`. R2 lifecycle expires bulletin
    /// objects after 7 days.
    pub async fn presign_bulletin_send(
        &self,
        recipient_pseudonym: &str,
        message_id: &str,
    ) -> SyncResult<PresignedUrl> {
        self.presign_single_url(serde_json::json!({
            "action": "presign_bulletin_send",
            "recipient_pseudonym": recipient_pseudonym,
            "message_id": message_id,
        }))
        .await
    }

    /// Presign a GET URL for reading an encrypted bulletin board message.
    ///
    /// No server-side ownership check — knowing the pseudonym is the right
    /// to read it. The recipient-public-key encryption is the actual
    /// protection (same trust model as `messaging_service`).
    pub async fn presign_bulletin_read(
        &self,
        recipient_pseudonym: &str,
        message_id: &str,
    ) -> SyncResult<PresignedUrl> {
        self.presign_single_url(serde_json::json!({
            "action": "presign_bulletin_read",
            "recipient_pseudonym": recipient_pseudonym,
            "message_id": message_id,
        }))
        .await
    }

    /// Presign a DELETE URL for a snapshot object (e.g., `latest.enc` or
    /// `{seq}.enc`). Used by the cloud-aware reset path that purges the
    /// personal sync log along with its snapshots.
    pub async fn presign_snapshot_delete(&self, snapshot_name: &str) -> SyncResult<PresignedUrl> {
        self.presign_single_url(snapshot_presign_body(
            "presign_snapshot_delete",
            snapshot_name,
        ))
        .await
    }

    /// Presign a DELETE URL for a snapshot object under a sync target's prefix.
    pub async fn presign_snapshot_delete_for_target(
        &self,
        target: &crate::sync::org_sync::SyncTarget,
        snapshot_name: &str,
    ) -> SyncResult<PresignedUrl> {
        self.presign_single_url(snapshot_presign_body_for_target(
            "presign_snapshot_delete",
            snapshot_name,
            target,
        ))
        .await
    }

    /// Request a presigned URL for uploading a referenced-file blob.
    ///
    /// `file_hash` is the SHA-256 of the plaintext; the blob is content-
    /// addressed at `{scope}/cas/sha256/{file_hash}` (a B2 path). The server
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

    /// Request a presigned URL for deleting a superseded thumbnail pack.
    pub async fn presign_thumb_pack_delete(&self, pack_id: &str) -> SyncResult<PresignedUrl> {
        self.presign_single_url(thumb_pack_presign_body(
            "presign_thumb_pack_delete",
            pack_id,
            None,
        ))
        .await
    }

    /// Post to the presign endpoint and parse the response, returning all URLs.
    async fn presign_urls(&self, body: serde_json::Value) -> SyncResult<Vec<PresignedUrl>> {
        self.presign_response(body).await.map(|r| r.urls)
    }

    /// Post to the presign endpoint and return the full parsed response —
    /// including any server-assigned `seq_numbers` (for server-allocated
    /// scoped uploads).
    async fn presign_response(&self, body: serde_json::Value) -> SyncResult<PresignedResponse> {
        self.presign_response_with_post(body, true).await
    }

    async fn presign_response_without_default_db_hash(
        &self,
        body: serde_json::Value,
    ) -> SyncResult<PresignedResponse> {
        self.presign_response_with_post(body, false).await
    }

    async fn presign_response_with_post(
        &self,
        body: serde_json::Value,
        apply_default_db_hash: bool,
    ) -> SyncResult<PresignedResponse> {
        let action = body
            .get("action")
            .and_then(|v| v.as_str())
            .unwrap_or("presign")
            .to_string();
        let resp = if apply_default_db_hash {
            self.post("/api/sync/presign", body).await?
        } else {
            self.post_no_default_db_hash("/api/sync/presign", body)
                .await?
        };
        let parsed: PresignedResponse = serde_json::from_value(resp)?;
        if !parsed.ok {
            return Err(op_failed(&action, parsed.error.or(parsed.reason)));
        }
        Ok(parsed)
    }

    /// Presign multiple URLs for a log action (upload or download) on a sync target.
    ///
    /// `estimated_size_bytes_per_entry` is the **per-entry** ciphertext size the
    /// storage service multiplies by `seq_numbers.len()` for the free/paid
    /// quota pre-check. When omitted, the server defaults to **1 MiB per entry**
    /// — so a 1000-entry batch is treated as ~1 GiB and falsely trips a free
    /// 1 GiB quota even when real sealed bodies are a few KiB. Always pass the
    /// real (or max) sealed size for uploads.
    async fn presign_log_urls(
        &self,
        action: &str,
        target: &crate::sync::org_sync::SyncTarget,
        seq_numbers: &[u64],
        estimated_size_bytes_per_entry: Option<u64>,
    ) -> SyncResult<Vec<PresignedUrl>> {
        self.presign_log_urls_with_keys(
            action,
            target,
            seq_numbers,
            &[],
            estimated_size_bytes_per_entry,
        )
        .await
    }

    async fn presign_log_urls_with_keys(
        &self,
        action: &str,
        target: &crate::sync::org_sync::SyncTarget,
        seq_numbers: &[u64],
        log_object_keys: &[String],
        estimated_size_bytes_per_entry: Option<u64>,
    ) -> SyncResult<Vec<PresignedUrl>> {
        let mut body = serde_json::json!({
            "action": action,
            "seq_numbers": seq_numbers,
        });
        if !log_object_keys.is_empty() {
            body["log_object_keys"] = serde_json::Value::Array(
                log_object_keys
                    .iter()
                    .cloned()
                    .map(serde_json::Value::String)
                    .collect(),
            );
        }
        if let Some(sz) = estimated_size_bytes_per_entry {
            body["estimated_size_bytes"] = serde_json::Value::Number(sz.into());
        }
        attach_target_scope(&mut body, target);
        self.presign_urls(body).await
    }

    /// Presign URLs for uploading log entries to a sync target.
    ///
    /// `estimated_size_bytes_per_entry` should be the max (or average) sealed
    /// ciphertext size in this batch — used only for the server-side quota
    /// gate (see [`Self::presign_log_urls`]).
    pub async fn presign_upload(
        &self,
        target: &crate::sync::org_sync::SyncTarget,
        seq_numbers: &[u64],
        estimated_size_bytes_per_entry: Option<u64>,
    ) -> SyncResult<Vec<PresignedUrl>> {
        self.presign_log_urls(
            "presign_log_upload",
            target,
            seq_numbers,
            estimated_size_bytes_per_entry,
        )
        .await
    }

    /// Presign typed mutation-log segments. The server derives each object key
    /// and rejects any client key that does not match the typed identity.
    pub async fn presign_upload_segments(
        &self,
        target: &crate::sync::org_sync::SyncTarget,
        segments: &[crate::sync::snapshot_log::MutationLogSegmentId],
        estimated_size_bytes_per_entry: Option<u64>,
    ) -> SyncResult<Vec<PresignedUrl>> {
        let seq_numbers = segments
            .iter()
            .map(|segment| segment.through_id)
            .collect::<Vec<_>>();
        let log_object_keys = segments
            .iter()
            .map(|segment| segment.object_key.clone())
            .collect::<Vec<_>>();
        let mut body = serde_json::json!({
            "action": "presign_log_upload",
            "seq_numbers": seq_numbers,
            "log_object_keys": log_object_keys,
        });
        if segments.iter().all(|segment| {
            segment.writer_id.is_some()
                && segment.schema_name.is_some()
                && segment.utc_nanos.is_some()
                && segment.sequence.is_some()
        }) {
            body["mutation_log_segments"] = serde_json::to_value(segments)
                .map_err(|e| SyncError::Storage(format!("encode mutation-log identity: {e}")))?;
        }
        if let Some(size) = estimated_size_bytes_per_entry {
            body["estimated_size_bytes"] = serde_json::Value::Number(size.into());
        }
        attach_target_scope(&mut body, target);
        self.presign_urls(body).await
    }

    /// Presign typed mutation-log objects for restore or peer apply. The
    /// server derives every key, including keys under a share scope.
    pub async fn presign_download_segments(
        &self,
        target: &crate::sync::org_sync::SyncTarget,
        segments: &[crate::sync::snapshot_log::MutationLogSegmentId],
    ) -> SyncResult<Vec<PresignedUrl>> {
        let seq_numbers = segments
            .iter()
            .map(|segment| segment.through_id)
            .collect::<Vec<_>>();
        let log_object_keys = segments
            .iter()
            .map(|segment| segment.object_key.clone())
            .collect::<Vec<_>>();
        let mut body = serde_json::json!({
            "action": "presign_log_download",
            "seq_numbers": seq_numbers,
            "log_object_keys": log_object_keys,
        });
        if segments.iter().all(|segment| {
            segment.writer_id.is_some()
                && segment.schema_name.is_some()
                && segment.utc_nanos.is_some()
                && segment.sequence.is_some()
        }) {
            body["mutation_log_segments"] = serde_json::to_value(segments)
                .map_err(|e| SyncError::Storage(format!("encode mutation-log identity: {e}")))?;
        }
        attach_target_scope(&mut body, target);
        self.presign_urls(body).await
    }

    /// Ask the server to atomically allocate `count` sequence numbers for a
    /// scoped upload and presign matching URLs in the same request. Returns the
    /// server-assigned seqs paired with their presigned URLs, sorted in
    /// ascending seq order.
    ///
    /// Only valid for scoped targets (non-empty prefix) —
    /// personal uploads must keep client-assigned nanosecond seqs because they
    /// use the single-writer device lock for ordering. Share targets are
    /// single-writer (only the sender writes), but the server-allocated seq
    /// path is reused for them: the storage service keys the seq counter by the
    /// share prefix and authorizes the caller as the owning sender.
    pub async fn presign_upload_alloc(
        &self,
        target: &crate::sync::org_sync::SyncTarget,
        count: u32,
    ) -> SyncResult<Vec<(u64, PresignedUrl)>> {
        if target.prefix.is_empty() {
            // Local precondition (no network). Must not be SyncError::Auth —
            // Auth is matched by cycle/backup as "refresh credential and retry".
            return Err(SyncError::Storage(
                "presign_upload_alloc requires a scoped target; the prefix is empty".to_string(),
            ));
        }
        if count == 0 {
            return Ok(Vec::new());
        }
        let mut body = serde_json::json!({
            "action": "presign_log_upload",
            "count": count,
        });
        attach_target_scope(&mut body, target);
        let resp = self.presign_response(body).await?;
        if resp.seq_numbers.len() != resp.urls.len() {
            return Err(SyncError::Storage(format!(
                "presign_upload_alloc: seq_numbers ({}) and urls ({}) length mismatch",
                resp.seq_numbers.len(),
                resp.urls.len(),
            )));
        }
        if resp.seq_numbers.len() != count as usize {
            return Err(SyncError::Storage(format!(
                "presign_upload_alloc: expected {} seqs, got {}",
                count,
                resp.seq_numbers.len(),
            )));
        }
        Ok(resp.seq_numbers.into_iter().zip(resp.urls).collect())
    }

    /// Presign URLs for downloading log entries from a sync target.
    pub async fn presign_download(
        &self,
        target: &crate::sync::org_sync::SyncTarget,
        seq_numbers: &[u64],
    ) -> SyncResult<Vec<PresignedUrl>> {
        self.presign_log_urls("presign_log_download", target, seq_numbers, None)
            .await
    }

    pub async fn presign_download_object_keys(
        &self,
        target: &crate::sync::org_sync::SyncTarget,
        seq_numbers: &[u64],
        log_object_keys: &[String],
    ) -> SyncResult<Vec<PresignedUrl>> {
        self.presign_log_urls_with_keys(
            "presign_log_download",
            target,
            seq_numbers,
            log_object_keys,
            None,
        )
        .await
    }

    /// Presign personal log downloads from the legacy principal root.
    pub async fn presign_download_legacy_personal(
        &self,
        seq_numbers: &[u64],
    ) -> SyncResult<Vec<PresignedUrl>> {
        self.presign_response_without_default_db_hash(serde_json::json!({
            "action": "presign_log_download",
            "seq_numbers": seq_numbers,
        }))
        .await
        .map(|resp| resp.urls)
    }
}
