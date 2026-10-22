//! Deliveries freeze sender and recipient generations before entering a durable queue.

use super::AuthClient;
mod notification;
use crate::sync::cloud_purge::protocol::{CloudAccount, CloudAccountKind};
use crate::sync::error::{SyncError, SyncResult};
pub(super) use notification::resolved_notification;
use notification::{resolved_ready, resolved_recipient_notification};
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
    #[serde(default)]
    pub sender_account: Option<CloudAccount>,
    #[serde(default)]
    pub sender_generation: u64,
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

    /// Call when the user creates a new explicit delivery, before its durable
    /// queue record. A ready recipient is fenced but never locally adopted.
    pub async fn prepare_recipient_delivery(
        &self,
        target: RecipientDeliveryTarget,
        delivery_nonce: &str,
    ) -> SyncResult<RecipientDeliveryBinding> {
        validate_nonce(delivery_nonce)?;
        let (sender_account, sender_generation, _) = self.sender_delivery_admission().await?;
        let response = self
            .read_cloud_control_for_request(&target.request())
            .await?;
        let (account, generation, operation_id) = resolved_recipient_notification(&response)?;
        Ok(RecipientDeliveryBinding {
            format_version: 1,
            account,
            generation,
            operation_id,
            sender_account: Some(sender_account),
            sender_generation,
            delivery_nonce: delivery_nonce.into(),
            target,
        })
    }

    pub(super) async fn guard_recipient_delivery_request(
        &self,
        body: &mut Value,
    ) -> SyncResult<()> {
        if let Some(error) = &self.cloud_generation_error {
            return Err(SyncError::CloudPurgeHeld(error.clone()));
        }
        let target = RecipientDeliveryTarget::from_request(body)?;
        let response = self
            .read_cloud_control_for_request(&target.request())
            .await?;
        let (account, generation, operation_id) = resolved_recipient_notification(&response)?;
        let binding = match body.get("cloud_delivery_binding") {
            Some(value) => serde_json::from_value::<RecipientDeliveryBinding>(value.clone())?,
            None => RecipientDeliveryBinding {
                format_version: 1,
                account: account.clone(),
                generation: 0,
                operation_id: None,
                sender_account: None,
                sender_generation: 0,
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
        let (sender_account, sender_generation, sender_token) =
            self.sender_delivery_admission().await?;
        if binding.sender_generation != sender_generation
            || binding
                .sender_account
                .as_ref()
                .is_some_and(|account| account != &sender_account)
        {
            return Err(held(
                "the queued delivery belongs to an older sender generation; create a new delivery",
            ));
        }
        body["account"] = serde_json::to_value(&binding.account)?;
        body["cloud_generation"] = json!(binding.generation);
        body["cloud_delivery_nonce"] = json!(binding.delivery_nonce);
        body["cloud_sender_generation"] = json!(binding.sender_generation);
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

    async fn sender_delivery_admission(&self) -> SyncResult<(CloudAccount, u64, Option<String>)> {
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
        if generation != 0 || response.protocol_required {
            return Err(held("sender generation requires retired cloud adoption"));
        }
        self.observe_cloud_control(&response)?;
        Ok((account, 0, None))
    }
}

pub(super) fn is_recipient_delivery(action: &str) -> bool {
    matches!(
        action,
        "presign_inbox_upload" | "presign_bulletin_send" | "messaging_connect"
    )
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
