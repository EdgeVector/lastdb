use super::super::helpers::{
    attach_target_scope, log_confirm_body, log_confirm_body_for_target, snapshot_confirm_body,
    snapshot_confirm_body_for_target,
};
use super::super::AuthClient;
use super::op_failed;
use crate::sync::error::SyncResult;

impl AuthClient {
    /// POST `/api/sync/presign` and require a top-level `ok: true`.
    /// Shared by upload and delete confirms so HTTP 200 + `{"ok":false}` never
    /// looks like success to callers (file_blob / thumb_pack non-fatal warns).
    async fn confirm_presign_action(
        &self,
        op: &str,
        body: serde_json::Value,
    ) -> SyncResult<serde_json::Value> {
        let value = self.post("/api/sync/presign", body).await?;
        if !value
            .get("ok")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            return Err(op_failed(op, confirm_upload_detail(&value)));
        }
        Ok(value)
    }

    /// After a successful PUT: credit actual object size (HEAD on server).
    /// Idempotent — safe to retry. Call for files, snapshots, and log entries.
    pub async fn confirm_upload(&self, body: serde_json::Value) -> SyncResult<serde_json::Value> {
        self.confirm_presign_action("confirm_upload", body).await
    }

    pub async fn confirm_file_upload(
        &self,
        file_hash: &str,
        app_id: Option<&str>,
    ) -> SyncResult<serde_json::Value> {
        let mut body = serde_json::json!({
            "action": "confirm_upload",
            "file_hash": file_hash,
        });
        if let Some(app_id) = app_id {
            body["app_id"] = serde_json::Value::String(app_id.to_string());
        }
        self.confirm_upload(body).await
    }

    pub async fn confirm_snapshot_upload(
        &self,
        snapshot_name: &str,
    ) -> SyncResult<serde_json::Value> {
        self.confirm_upload(snapshot_confirm_body(snapshot_name))
            .await
    }

    pub async fn confirm_snapshot_upload_for_target(
        &self,
        target: &crate::sync::org_sync::SyncTarget,
        snapshot_name: &str,
    ) -> SyncResult<serde_json::Value> {
        self.confirm_upload(snapshot_confirm_body_for_target(snapshot_name, target))
            .await
    }

    pub async fn confirm_log_upload(&self, seq_numbers: &[u64]) -> SyncResult<serde_json::Value> {
        self.confirm_upload(log_confirm_body(seq_numbers)).await
    }

    pub async fn confirm_log_upload_for_target(
        &self,
        target: &crate::sync::org_sync::SyncTarget,
        seq_numbers: &[u64],
    ) -> SyncResult<serde_json::Value> {
        self.confirm_upload(log_confirm_body_for_target(seq_numbers, target))
            .await
    }

    pub async fn confirm_log_upload_segments_for_target(
        &self,
        target: &crate::sync::org_sync::SyncTarget,
        segments: &[crate::sync::snapshot_log::MutationLogSegmentId],
    ) -> SyncResult<serde_json::Value> {
        let mut body = serde_json::json!({
            "action": "confirm_upload",
            "seq_numbers": segments.iter().map(|segment| segment.through_id).collect::<Vec<_>>(),
            "log_object_keys": segments.iter().map(|segment| segment.object_key.clone()).collect::<Vec<_>>(),
        });
        if segments.iter().all(|segment| {
            segment.writer_id.is_some()
                && segment.schema_name.is_some()
                && segment.utc_nanos.is_some()
                && segment.sequence.is_some()
        }) {
            body["mutation_log_segments"] = serde_json::to_value(segments).map_err(|e| {
                crate::sync::error::SyncError::Storage(format!("encode mutation-log identity: {e}"))
            })?;
        }
        attach_target_scope(&mut body, target);
        self.confirm_upload(body).await
    }

    pub async fn confirm_file_delete(&self, file_hash: &str) -> SyncResult<serde_json::Value> {
        self.confirm_presign_action(
            "confirm_file_delete",
            serde_json::json!({
                "action": "confirm_delete",
                "file_hash": file_hash,
            }),
        )
        .await
    }

    pub async fn confirm_thumb_pack_upload(&self, pack_id: &str) -> SyncResult<serde_json::Value> {
        self.confirm_upload(serde_json::json!({
            "action": "confirm_upload",
            "pack_id": pack_id,
        }))
        .await
    }

    pub async fn confirm_backup_chunk_upload(
        &self,
        chunk_sha256: &str,
    ) -> SyncResult<serde_json::Value> {
        self.confirm_upload(serde_json::json!({
            "action": "confirm_upload",
            "chunk_sha256": chunk_sha256,
        }))
        .await
    }

    pub async fn confirm_backup_manifest_upload(
        &self,
        manifest_sha256: &str,
    ) -> SyncResult<serde_json::Value> {
        self.confirm_upload(serde_json::json!({
            "action": "confirm_upload",
            "manifest_sha256": manifest_sha256,
        }))
        .await
    }

    /// After a successful DELETE: ask the server to debit the storage meter
    /// for the deleted objects. The server HEADs each key and debits only an
    /// object that is gone, through the idempotent credit ledger, so a retry
    /// or a confirm for a never-credited object debits nothing.
    ///
    /// Before this, only file and loose-thumb deletes confirmed. Log
    /// compaction, snapshot and thumb-pack prunes, reset purges and backup
    /// chunk GC deleted without a debit, so the meter only grew (764 GB
    /// metered vs 49.9 GB real on 2026-09-23).
    pub async fn confirm_delete(&self, body: serde_json::Value) -> SyncResult<serde_json::Value> {
        self.confirm_presign_action("confirm_delete", body).await
    }

    /// Debit deleted mutation-log objects. Same body shape as
    /// `presign_log_delete_object_keys`, so the server resolves the same keys.
    pub async fn confirm_log_delete_object_keys(
        &self,
        target: &crate::sync::org_sync::SyncTarget,
        seq_numbers: &[u64],
        log_object_keys: &[String],
    ) -> SyncResult<serde_json::Value> {
        let mut body = serde_json::json!({
            "action": "confirm_delete",
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
        attach_target_scope(&mut body, target);
        self.confirm_delete(body).await
    }

    /// Debit one deleted snapshot / photograph object of a sync target.
    pub async fn confirm_snapshot_delete_for_target(
        &self,
        target: &crate::sync::org_sync::SyncTarget,
        snapshot_name: &str,
    ) -> SyncResult<serde_json::Value> {
        let mut body = serde_json::json!({
            "action": "confirm_delete",
            "snapshot_name": snapshot_name,
        });
        attach_target_scope(&mut body, target);
        self.confirm_delete(body).await
    }

    /// Debit one deleted personal thumbnail pack.
    pub async fn confirm_thumb_pack_delete(&self, pack_id: &str) -> SyncResult<serde_json::Value> {
        self.confirm_delete(serde_json::json!({
            "action": "confirm_delete",
            "pack_id": pack_id,
        }))
        .await
    }

    /// Debit one deleted backup chunk (backup GC).
    pub async fn confirm_backup_chunk_delete(
        &self,
        chunk_sha256: &str,
    ) -> SyncResult<serde_json::Value> {
        self.confirm_delete(serde_json::json!({
            "action": "confirm_delete",
            "chunk_sha256": chunk_sha256,
        }))
        .await
    }

    pub async fn confirm_thumb_delete(&self, thumb_hash: &str) -> SyncResult<serde_json::Value> {
        self.confirm_presign_action(
            "confirm_thumb_delete",
            serde_json::json!({
                "action": "confirm_delete",
                "key": format!("thumbs/loose/sha256/{thumb_hash}"),
            }),
        )
        .await
    }
}

fn confirm_upload_detail(value: &serde_json::Value) -> Option<String> {
    value
        .get("reason")
        .or_else(|| value.get("error"))
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
        .or_else(|| {
            value
                .get("results")
                .and_then(serde_json::Value::as_array)
                .and_then(|results| {
                    results
                        .iter()
                        .find(|result| {
                            result.get("ok").and_then(serde_json::Value::as_bool) == Some(false)
                        })
                        .and_then(|result| {
                            result
                                .get("error")
                                .or_else(|| result.get("reason"))
                                .and_then(serde_json::Value::as_str)
                                .map(ToString::to_string)
                        })
                })
        })
}
