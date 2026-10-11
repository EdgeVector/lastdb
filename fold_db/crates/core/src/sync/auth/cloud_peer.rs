//! Permanent cloud generation admission and exact local adoption.

use super::AuthClient;
use crate::sync::error::{SyncError, SyncResult};
use std::sync::Arc;

impl AuthClient {
    pub fn with_auth_refresh_callback(mut self, callback: super::AuthRefreshCallback) -> Self {
        self.auth_refresh = Some(callback);
        self
    }

    pub fn with_cloud_purge_peer(
        mut self,
        peer: Arc<crate::sync::cloud_purge_peer::CloudPurgePeer>,
    ) -> Self {
        self.cloud_purge_peer = Some(peer);
        self.cloud_purge_peer_error = None;
        self
    }

    pub(crate) fn with_cloud_purge_peer_error(mut self, error: String) -> Self {
        self.cloud_purge_peer_error = Some(error);
        self
    }

    pub fn cloud_purge_peer(&self) -> Option<Arc<crate::sync::cloud_purge_peer::CloudPurgePeer>> {
        self.cloud_purge_peer.clone()
    }

    /// Restore reads address only this authenticated immutable replacement.
    /// This clone refuses every mutation and never acknowledges adoption.
    pub fn for_cloud_adoption_reads(
        &self,
        plan: &crate::sync::cloud_purge_peer::AdoptionPlan,
    ) -> Self {
        let mut reader = self.clone().without_db_auto_claim();
        reader.cloud_adoption_read = Some(plan.clone());
        reader
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
        if self.cloud_adoption_read.is_none() && is_control_action(&action) {
            return Ok(());
        }
        if let Some(error) = &self.cloud_purge_peer_error {
            return Err(SyncError::CloudPurgeHeld(error.clone()));
        }
        let is_read = is_cloud_read_action(path, &action, body);
        if let Some(plan) = &self.cloud_adoption_read {
            if !is_read
                && !matches!(
                    action.as_str(),
                    "cloud_control_get" | "cloud_control_download" | "cloud_purge_download_batch"
                )
            {
                return Err(SyncError::CloudPurgeHeld(
                    "inactive cloud adoption clients cannot mutate cloud data".into(),
                ));
            }
            put_generation(
                body,
                &plan.account,
                plan.generation,
                Some(&plan.operation_id),
            )?;
            return Ok(());
        }
        if super::recipient_delivery::is_recipient_delivery(&action) {
            return self.guard_recipient_delivery_request(body).await;
        }
        let response = self.read_cloud_control_for_request(body).await?;
        if let Some(peer) = &self.cloud_purge_peer {
            peer.observe(&response)?;
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
            put_generation(body, account, 0, None)?;
            return Ok(());
        }
        let control = response.control.as_ref().ok_or_else(|| {
            SyncError::CloudPurgeHeld("the permanent cloud control is missing".into())
        })?;
        if !control.permits_local_adoption() {
            return Err(SyncError::CloudPurgeHeld(format!(
                "account generation {} is held",
                control.generation
            )));
        }
        let Some(peer) = self.local_adoption_peer(body, account, control, is_read)? else {
            return Ok(());
        };
        let token = peer.ensure_adopted(self, control).await?;
        put_generation(
            body,
            account,
            control.generation,
            Some(&control.operation_id),
        )?;
        body["cloud_adoption_token"] = serde_json::Value::String(token);
        Ok(())
    }

    fn local_adoption_peer(
        &self,
        body: &mut serde_json::Value,
        account: &crate::sync::cloud_purge::protocol::CloudAccount,
        control: &crate::sync::cloud_purge::protocol::CloudPurgeControl,
        is_read: bool,
    ) -> SyncResult<Option<&Arc<crate::sync::cloud_purge_peer::CloudPurgePeer>>> {
        // Ordinary downloads cannot merge new data into the old local home
        // before the exact inactive adoption. The host's read-only clone above
        // supplies the narrow exception for that adoption itself.
        let peer = self.cloud_purge_peer.as_ref().ok_or_else(|| {
            SyncError::CloudPurgeHeld(
                "cloud generation adoption requires a durable local peer receipt".into(),
            )
        })?;
        let personal_file_consumer = body.get("action").and_then(serde_json::Value::as_str)
            == Some("presign_file_download")
            && body.get("owner_scope").is_some()
            && body.get("consumer_account").is_none()
            && account.kind == crate::sync::cloud_purge::protocol::CloudAccountKind::Personal;
        if is_own_unqualified_scope(body, self.db_hash.as_deref()) || personal_file_consumer {
            peer.bind_local_account(account)?;
        }
        if !peer.is_local_account(account)? {
            // An explicit consumer org may allow a shared file read without
            // making that org a configured local database on this device.
            if is_read && body.get("owner_scope").is_some() {
                put_generation(
                    body,
                    account,
                    control.generation,
                    Some(&control.operation_id),
                )?;
                return Ok(None);
            }
            return Err(SyncError::CloudPurgeHeld(
                "a foreign account cannot replace this device's local data".into(),
            ));
        }
        Ok(Some(peer))
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
        Ok(serde_json::from_value(value)?)
    }

    /// Startup/reconnect admission runs before any upload preparation or peer
    /// replay. Independent publishers also pass the request guard above.
    pub async fn cloud_peer_preflight(&self, share_prefix: Option<&str>) -> SyncResult<()> {
        let mut body = serde_json::json!({"action":"list_objects"});
        if let Some(prefix) = share_prefix {
            body["share_prefix"] = serde_json::json!(prefix);
        } else {
            self.apply_default_db_hash(&mut body);
        }
        let response = self.read_cloud_control_for_request(&body).await?;
        if let Some(peer) = &self.cloud_purge_peer {
            peer.observe(&response)?;
            let account = response
                .account
                .as_ref()
                .or_else(|| response.control.as_ref().map(|control| &control.account))
                .ok_or_else(|| {
                    SyncError::CloudPurgeHeld(
                        "cloud preflight did not resolve a configured account".into(),
                    )
                })?;
            peer.bind_local_account(account)?;
        }
        self.guard_cloud_request("/api/sync/list", &mut body).await
    }
}

fn is_own_unqualified_scope(body: &serde_json::Value, default_db_hash: Option<&str>) -> bool {
    if [
        "owner_scope",
        "target_user_hash",
        "recipient_hash",
        "share_prefix",
        "org_hash",
    ]
    .iter()
    .any(|key| body.get(key).is_some())
    {
        return false;
    }
    match (
        body.get("db_hash").and_then(serde_json::Value::as_str),
        default_db_hash,
    ) {
        (None, None) => true,
        (Some(actual), Some(expected)) => actual == expected,
        _ => false,
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

fn is_control_action(action: &str) -> bool {
    matches!(
        action,
        "cloud_control_get"
            | "cloud_control_adopt"
            | "cloud_control_continue"
            | "cloud_control_download"
            | "cloud_purge_prepare"
            | "cloud_purge_begin"
            | "cloud_purge_step"
            | "cloud_purge_upload_batch"
            | "cloud_purge_confirm"
            | "cloud_purge_publish"
            | "cloud_purge_pause"
            | "cloud_purge_cancel"
            | "cloud_purge_status"
            | "cloud_purge_resume"
            | "cloud_purge_commit"
            | "cloud_purge_replace_source"
            | "cloud_purge_download_batch"
            | "cloud_purge_upload_complete"
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
