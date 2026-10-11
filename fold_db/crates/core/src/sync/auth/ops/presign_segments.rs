//! Presigned URLs for log segments and generic object-key downloads.

use super::super::helpers::attach_target_scope;
use super::super::AuthClient;
use crate::sync::error::{SyncError, SyncResult};
use crate::sync::s3::PresignedUrl;

impl AuthClient {
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
