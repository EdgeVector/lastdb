//! Deliveries freeze the recipient generation before entering a durable queue.

use super::AuthClient;
use crate::sync::cloud_purge::protocol::{CloudAccount, CloudAccountKind, CloudControlGet};
use crate::sync::error::{SyncError, SyncResult};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RecipientDeliveryTarget {
    Inbox {
        target_user_hash: String,
        file_name: String,
    },
    Bulletin {
        recipient_pseudonym: String,
        message_id: String,
    },
    Messaging {
        target_pseudonym: String,
    },
}

impl RecipientDeliveryTarget {
    fn request(&self) -> Value {
        match self {
            Self::Inbox {
                target_user_hash,
                file_name,
            } => json!({
                "action":"presign_inbox_upload", "target_user_hash":target_user_hash,
                "snapshot_name":file_name,
            }),
            Self::Bulletin {
                recipient_pseudonym,
                message_id,
            } => json!({
                "action":"presign_bulletin_send", "recipient_pseudonym":recipient_pseudonym,
                "message_id":message_id,
            }),
            Self::Messaging { target_pseudonym } => json!({
                "action":"messaging_connect", "target_pseudonym":target_pseudonym,
            }),
        }
    }

    fn from_request(body: &Value) -> SyncResult<Self> {
        let text = |key: &str| -> SyncResult<String> {
            body.get(key)
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .ok_or_else(|| held("recipient delivery identity is incomplete"))
        };
        match body.get("action").and_then(Value::as_str) {
            Some("presign_inbox_upload") => Ok(Self::Inbox {
                target_user_hash: text("target_user_hash")?,
                file_name: text("snapshot_name")?,
            }),
            Some("presign_bulletin_send") => Ok(Self::Bulletin {
                recipient_pseudonym: text("recipient_pseudonym")?,
                message_id: text("message_id")?,
            }),
            Some("messaging_connect") => Ok(Self::Messaging {
                target_pseudonym: text("target_pseudonym")?,
            }),
            _ => Err(held("the request is not a recipient delivery")),
        }
    }
}

/// This non-secret record belongs beside the staged message. A retry reuses
/// it exactly; it never assigns an old message to a newer recipient generation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecipientDeliveryBinding {
    pub format_version: u32,
    pub account: CloudAccount,
    pub generation: u64,
    pub operation_id: Option<String>,
    pub delivery_nonce: String,
    pub target: RecipientDeliveryTarget,
}

impl AuthClient {
    /// Send the immutable staged envelope with the same admission and bounded
    /// credential refresh as every ordinary cloud request.
    pub async fn send_recipient_messaging(
        &self,
        target_pseudonym: &str,
        delivery_nonce: &str,
        binding: Option<&RecipientDeliveryBinding>,
        encrypted_blob: &str,
    ) -> SyncResult<()> {
        let mut body = json!({"action":"messaging_connect", "target_pseudonym":target_pseudonym,
            "encrypted_blob":encrypted_blob, "cloud_delivery_nonce":delivery_nonce});
        if let Some(binding) = binding {
            if binding.delivery_nonce != delivery_nonce {
                return Err(held("the staged delivery nonce differs"));
            }
            body["cloud_delivery_binding"] = serde_json::to_value(binding)?;
        }
        let path = if self.base_url.trim_end_matches('/').ends_with("/api") {
            "/messaging/connect"
        } else {
            "/api/messaging/connect"
        };
        let response = self.post_no_default_db_hash(path, body).await?;
        if response.get("ok").and_then(Value::as_bool) != Some(true) {
            return Err(held("the recipient messaging delivery was refused"));
        }
        Ok(())
    }

    /// Request an inbox or bulletin upload for an already staged binding.
    /// The frozen record survives retries and never contains a private token.
    pub async fn presign_recipient_delivery(
        &self,
        binding: &RecipientDeliveryBinding,
    ) -> SyncResult<crate::sync::s3::PresignedUrl> {
        if matches!(binding.target, RecipientDeliveryTarget::Messaging { .. }) {
            return Err(held(
                "messaging deliveries use the node messaging transport",
            ));
        }
        let mut body = binding.target.request();
        body["cloud_delivery_binding"] = serde_json::to_value(binding)?;
        let value = self.post("/api/sync/presign", body).await?;
        let response: super::PresignedResponse = serde_json::from_value(value)?;
        if !response.ok {
            return Err(held("the recipient delivery upload was refused"));
        }
        response
            .urls
            .into_iter()
            .next()
            .ok_or_else(|| held("the recipient upload grant is absent"))
    }

    /// Call when the user creates a new explicit delivery, before its durable
    /// queue record. A ready recipient is fenced but never locally adopted.
    pub async fn prepare_recipient_delivery(
        &self,
        target: RecipientDeliveryTarget,
        delivery_nonce: &str,
    ) -> SyncResult<RecipientDeliveryBinding> {
        if self.cloud_adoption_read.is_some() {
            return Err(held("an inactive adoption reader cannot create a delivery"));
        }
        validate_nonce(delivery_nonce)?;
        let response = self
            .read_cloud_control_for_request(&target.request())
            .await?;
        let (account, generation, operation_id) = resolved_ready(&response)?;
        if let Some(peer) = &self.cloud_purge_peer {
            peer.observe(&response)?;
        }
        Ok(RecipientDeliveryBinding {
            format_version: 1,
            account,
            generation,
            operation_id,
            delivery_nonce: delivery_nonce.into(),
            target,
        })
    }

    /// The node's direct messaging transport also uses this admission just
    /// before each request. Only sender adoption can change this local home.
    pub async fn guard_recipient_delivery(
        &self,
        binding: &RecipientDeliveryBinding,
        body: &mut Value,
    ) -> SyncResult<()> {
        body["cloud_delivery_binding"] = serde_json::to_value(binding)?;
        self.guard_recipient_delivery_request(body).await
    }

    pub(super) async fn guard_recipient_delivery_request(
        &self,
        body: &mut Value,
    ) -> SyncResult<()> {
        if self.cloud_adoption_read.is_some() {
            return Err(held("an inactive adoption reader cannot send a delivery"));
        }
        if let Some(error) = &self.cloud_purge_peer_error {
            return Err(SyncError::CloudPurgeHeld(error.clone()));
        }
        let target = RecipientDeliveryTarget::from_request(body)?;
        let response = self
            .read_cloud_control_for_request(&target.request())
            .await?;
        let (account, generation, operation_id) = resolved_ready(&response)?;
        if let Some(peer) = &self.cloud_purge_peer {
            peer.observe(&response)?;
        }
        let binding = match body.get("cloud_delivery_binding") {
            Some(value) => serde_json::from_value::<RecipientDeliveryBinding>(value.clone())?,
            None => RecipientDeliveryBinding {
                format_version: 1,
                account: account.clone(),
                generation: 0,
                operation_id: None,
                delivery_nonce: body
                    .get("cloud_delivery_nonce")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or(format!(
                        "legacy:{}",
                        crate::hex::sha256_hex(serde_json::to_vec(&target)?)
                    )),
                target: target.clone(),
            },
        };
        validate_nonce(&binding.delivery_nonce)?;
        if binding.format_version != 1
            || binding.target != target
            || binding.account != account
            || binding.generation != generation
            || binding.operation_id != operation_id
        {
            return Err(held("the queued delivery belongs to an older recipient generation; create a new delivery"));
        }
        let (sender_generation, sender_token) = self.sender_delivery_admission().await?;
        body["account"] = serde_json::to_value(&binding.account)?;
        body["cloud_generation"] = json!(binding.generation);
        body["cloud_delivery_nonce"] = json!(binding.delivery_nonce);
        body["cloud_sender_generation"] = json!(sender_generation);
        if let Some(operation) = binding.operation_id {
            body["cloud_operation_id"] = json!(operation);
        }
        if let Some(object) = body.as_object_mut() {
            object.remove("cloud_delivery_binding");
            object.remove("cloud_adoption_token");
            object.remove("cloud_sender_adoption_token");
        }
        if let Some(token) = sender_token {
            body["cloud_sender_adoption_token"] = json!(token);
        }
        Ok(())
    }

    async fn sender_delivery_admission(&self) -> SyncResult<(u64, Option<String>)> {
        // Omit every target selector and db_hash: this is the authenticated
        // sender's personal account, independent of the recipient account.
        let response = self
            .read_cloud_control_for_request(&json!({"action":"list_objects"}))
            .await?;
        let (account, generation, _) = resolved_ready(&response)?;
        if account.kind != CloudAccountKind::Personal {
            return Err(held(
                "the service did not resolve the sender personal account",
            ));
        }
        if generation == 0 {
            if let Some(peer) = &self.cloud_purge_peer {
                peer.observe(&response)?;
                peer.bind_local_account(&account)?;
            }
            return Ok((0, None));
        }
        let peer = self
            .cloud_purge_peer
            .as_ref()
            .ok_or_else(|| held("sender adoption requires a durable local peer receipt"))?;
        peer.observe(&response)?;
        peer.bind_local_account(&account)?;
        let control = response
            .control
            .as_ref()
            .ok_or_else(|| held("sender control is absent"))?;
        let token = peer.ensure_adopted(self, control).await?;
        Ok((generation, Some(token)))
    }
}

pub(super) fn is_recipient_delivery(action: &str) -> bool {
    matches!(
        action,
        "presign_inbox_upload" | "presign_bulletin_send" | "messaging_connect"
    )
}

fn resolved_ready(response: &CloudControlGet) -> SyncResult<(CloudAccount, u64, Option<String>)> {
    let account = response
        .account
        .as_ref()
        .or_else(|| response.control.as_ref().map(|control| &control.account))
        .ok_or_else(|| held("recipient control did not resolve its account"))?;
    if account.id.is_empty() {
        return Err(held("recipient account identity is empty"));
    }
    if !response.protocol_required {
        if response.generation != 0 || response.control.is_some() {
            return Err(held("recipient legacy control response is invalid"));
        }
        return Ok((account.clone(), 0, None));
    }
    let control = response
        .control
        .as_ref()
        .ok_or_else(|| held("recipient permanent control is absent"))?;
    control
        .validate_identity()
        .map_err(SyncError::CloudPurgeHeld)?;
    if control.account != *account
        || control.generation != response.generation
        || !control.permits_local_adoption()
    {
        return Err(held("the recipient or sender account remains held"));
    }
    Ok((
        account.clone(),
        control.generation,
        Some(control.operation_id.clone()),
    ))
}

fn validate_nonce(nonce: &str) -> SyncResult<()> {
    if nonce.is_empty() || nonce.len() > 256 || nonce.chars().any(char::is_control) {
        return Err(held("recipient delivery nonce is invalid"));
    }
    Ok(())
}

fn held(reason: &str) -> SyncError {
    SyncError::CloudPurgeHeld(reason.into())
}
