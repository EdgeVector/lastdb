//! Permanent cloud generation admission without local adoption or repair.

use super::AuthClient;
use crate::sync::error::{SyncError, SyncResult};

impl AuthClient {
    pub fn with_auth_refresh_callback(mut self, callback: super::AuthRefreshCallback) -> Self {
        self.auth_refresh = Some(callback);
        self
    }

    pub(crate) fn with_cloud_generation_error(mut self, error: String) -> Self {
        self.cloud_generation_error = Some(error);
        self
    }

    /// Resolve the account on the service. A database hash may be either a
    /// personal root or an org root, so the client never infers that authority.
    pub(super) async fn guard_cloud_request(
        &self,
        path: &str,
        body: &mut serde_json::Value,
    ) -> SyncResult<()> {
        let action = body
            .get("action")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_string();
        if is_control_action(&action) {
            return Ok(());
        }
        if let Some(error) = &self.cloud_generation_error {
            return Err(SyncError::CloudPurgeHeld(error.clone()));
        }
        let is_read = is_cloud_read_action(path, &action, body);
        if super::recipient_delivery::is_recipient_delivery(&action) {
            return self.guard_recipient_delivery_request(body).await;
        }
        let response = self.read_cloud_control_for_request(body).await?;
        if response.notification_only {
            return guard_share_notification(body, &response, is_read);
        }
        let account = response
            .account
            .as_ref()
            .or_else(|| response.control.as_ref().map(|control| &control.account))
            .ok_or_else(|| {
                SyncError::CloudPurgeHeld("cloud control did not resolve the account".into())
            })?;
        if !response.protocol_required {
            if response.generation != 0 || response.control.is_some() {
                return Err(SyncError::CloudPurgeHeld(
                    "invalid legacy cloud control response".into(),
                ));
            }
            if is_read && body.get("share_prefix").is_some() {
                super::cloud_share::bind_read(body, 0, 0)?;
            }
            put_generation(body, account, 0, None)?;
            return Ok(());
        }
        Err(SyncError::CloudPurgeHeld(
            "cloud generation requires retired local adoption; this build cannot adopt or repair cloud data".into(),
        ))
    }

    pub(super) async fn read_cloud_control_for_request(
        &self,
        request: &serde_json::Value,
    ) -> SyncResult<crate::sync::cloud_purge::protocol::CloudControlGet> {
        let mut control_body = request.clone();
        let action = request
            .get("action")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        control_body["cloud_original_action"] = action;
        control_body["action"] = serde_json::json!("cloud_control_get");
        // Never forward one operation's private credentials to a status read.
        if let Some(object) = control_body.as_object_mut() {
            for key in [
                "cloud_capability",
                "cloud_adoption_token",
                "cloud_sender_adoption_token",
                "completion_ticket",
                "deletion_ticket",
            ] {
                object.remove(key);
            }
        }
        let value =
            Box::pin(self.post_no_default_db_hash("/api/sync/presign", control_body)).await?;
        if value.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
            return Err(SyncError::CloudPurgeHeld(
                "cloud control read failed".into(),
            ));
        }
        let response = serde_json::from_value(value)?;
        self.observe_cloud_control(&response)?;
        Ok(response)
    }

    pub(crate) async fn cloud_target_preflight(
        &self,
        target: &crate::sync::SyncTarget,
    ) -> SyncResult<Option<crate::sync::cloud_share::ShareReadNotice>> {
        let mut body = serde_json::json!({"action":"list_objects"});
        super::helpers::attach_target_scope(&mut body, target);
        if target.prefix.is_empty() {
            self.apply_default_db_hash(&mut body);
        }
        if let Some(body) = body.as_object_mut() {
            body.remove("cloud_generation");
            body.remove("cloud_read_data_generation");
        }
        self.cloud_scope_preflight(body).await
    }

    async fn cloud_scope_preflight(
        &self,
        mut body: serde_json::Value,
    ) -> SyncResult<Option<crate::sync::cloud_share::ShareReadNotice>> {
        let response = self.read_cloud_control_for_request(&body).await?;
        if response.notification_only {
            guard_share_notification(&mut body, &response, true)?;
            return super::cloud_share::notice(
                &response,
                body["share_prefix"].as_str().unwrap_or(""),
            )
            .map(Some);
        }
        self.guard_cloud_request("/api/sync/list", &mut body)
            .await?;
        Ok(None)
    }
}

fn put_generation(
    body: &mut serde_json::Value,
    account: &crate::sync::cloud_purge::protocol::CloudAccount,
    generation: u64,
    operation_id: Option<&str>,
) -> SyncResult<()> {
    if !body.is_object() {
        return Err(SyncError::Serialization(
            "cloud request must be an object".into(),
        ));
    }
    body["account"] = serde_json::to_value(account)?;
    body["cloud_generation"] = serde_json::json!(generation);
    if let Some(operation_id) = operation_id {
        body["cloud_operation_id"] = serde_json::json!(operation_id);
    }
    Ok(())
}

fn guard_share_notification(
    body: &mut serde_json::Value,
    response: &crate::sync::cloud_purge::protocol::CloudControlGet,
    is_read: bool,
) -> SyncResult<()> {
    let prefix = body.get("share_prefix").and_then(serde_json::Value::as_str);
    if !is_read
        || response.notification_scope.as_deref() != Some("share_read")
        || prefix.is_none()
        || prefix != response.scope_root.as_deref()
    {
        return Err(SyncError::CloudPurgeHeld(
            "a cloud notification cannot authorize this account request".into(),
        ));
    }
    let (account, generation, operation) =
        super::recipient_delivery::resolved_notification(response)?;
    let notice = super::cloud_share::notice(response, prefix.unwrap_or(""))?;
    super::cloud_share::bind_read(body, generation, notice.data_generation)?;
    put_generation(body, &account, generation, operation.as_deref())
}

fn is_control_action(action: &str) -> bool {
    matches!(
        action,
        "cloud_control_get"
            | "cloud_org_authority_get"
            | "cloud_org_authority_publish"
            | "cloud_recipient_enroll"
    )
}

fn is_cloud_read_action(path: &str, action: &str, body: &serde_json::Value) -> bool {
    if path == "/api/sync/list" && action == "list_objects" {
        return true;
    }
    matches!(
        action,
        "list_objects"
            | "backup_latest_get"
            | "backup_latest_cas_capabilities"
            | "photograph_latest_get"
            | "rescue_s0_get"
            | "rescue_s0_list"
            | "presign_log_download"
            | "presign_snapshot_download"
            | "presign_inbox_download"
            | "presign_p2p_download"
            | "presign_bulletin_read"
            | "presign_file_download"
            | "presign_backup_chunk_download"
            | "presign_backup_manifest_download"
            | "presign_thumb_download"
            | "presign_thumb_pack_download"
            | "storage_status"
    ) || (action == "audit_storage"
        && body.get("apply").and_then(serde_json::Value::as_bool) != Some(true))
}
