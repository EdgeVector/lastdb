//! A new recipient address is authenticated account metadata, not a legacy UUID claim.
use super::super::AuthClient;
use crate::sync::cloud_purge::protocol::{CloudAccount, CloudAccountKind, CloudPurgePhase};
use crate::sync::{SyncError, SyncResult};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CloudRecipientEnrollment {
    pub format_version: u32,
    pub account: CloudAccount,
    pub enrollment_id: String,
    pub recipient_pseudonym: String,
}

impl AuthClient {
    /// The caller flushes the enrollment UUID before this call. A server retry
    /// returns the same address and never claims a client-selected old address.
    pub async fn cloud_recipient_enroll(
        &self,
        account: &CloudAccount,
        enrollment_id: &str,
    ) -> SyncResult<CloudRecipientEnrollment> {
        if account.kind != CloudAccountKind::Personal || !canonical_uuid(enrollment_id) {
            return Err(SyncError::Storage(
                "invalid personal recipient enrollment".into(),
            ));
        }
        let control = self.cloud_control_get(account).await?;
        if control.protocol_required
            || control.generation != 0
            || control.notification_only
            || control.account.as_ref() != Some(account)
            || control.control.as_ref().is_some_and(|value| {
                value.phase != CloudPurgePhase::Ready && value.unchanged_continuation().is_none()
            })
        {
            return Err(SyncError::CloudPurgeHeld(
                "recipient enrollment requires the current open personal account".into(),
            ));
        }
        let enrolled: CloudRecipientEnrollment = self
            .control_call(
                json!({
                    "account":account,"cloud_operation_id":enrollment_id,
                    "cloud_generation":control.generation,
                }),
                "cloud_recipient_enroll",
            )
            .await?;
        if enrolled.format_version != 1
            || enrolled.account != *account
            || enrolled.enrollment_id != enrollment_id
            || !canonical_uuid(&enrolled.recipient_pseudonym)
        {
            return Err(SyncError::Storage(
                "the recipient enrollment response differs from its immutable claim".into(),
            ));
        }
        Ok(enrolled)
    }
}

fn canonical_uuid(value: &str) -> bool {
    uuid::Uuid::parse_str(value).is_ok_and(|uuid| uuid.to_string() == value)
}
