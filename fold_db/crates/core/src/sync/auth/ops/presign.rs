use super::super::failure::decode_service_response;
use super::super::helpers::{
    attach_target_scope, snapshot_presign_body, snapshot_presign_body_for_target,
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
    pub(super) async fn presign_single_url(
        &self,
        body: serde_json::Value,
    ) -> SyncResult<PresignedUrl> {
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

    /// Post to the presign endpoint and parse the response, returning all URLs.
    pub(super) async fn presign_urls(
        &self,
        body: serde_json::Value,
    ) -> SyncResult<Vec<PresignedUrl>> {
        self.presign_response(body).await.map(|r| r.urls)
    }

    /// Post to the presign endpoint and return the full parsed response —
    /// including any server-assigned `seq_numbers` (for server-allocated
    /// scoped uploads).
    pub(super) async fn presign_response(
        &self,
        body: serde_json::Value,
    ) -> SyncResult<PresignedResponse> {
        self.presign_response_with_post(body, true).await
    }

    pub(super) async fn presign_response_without_default_db_hash(
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
        let parsed: PresignedResponse = decode_service_response(&action, resp)?;
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
    pub(super) async fn presign_log_urls(
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

    pub(super) async fn presign_log_urls_with_keys(
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
}
