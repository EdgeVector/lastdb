//! Exact operation cancellation acknowledgment; it never advances an account generation.
use super::{CloudAccount, CloudAccountKind, CloudPurgeControl};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CloudPurgeCancellationReceipt {
    pub format_version: u32,
    pub account: CloudAccount,
    pub operation_id: String,
    pub principal_hash: String,
    pub deny_begin: bool,
    pub cancelled_before_accept: bool,
    pub capability_binding_sha256: String,
}

impl CloudPurgeCancellationReceipt {
    pub fn validate(
        &self,
        account: &CloudAccount,
        operation: &str,
        capability: &str,
    ) -> Result<(), String> {
        let kind = match account.kind {
            CloudAccountKind::Personal => "personal",
            CloudAccountKind::Org => "org",
        };
        let capability_hash = crate::hex::sha256_hex(capability.as_bytes());
        let binding = crate::hex::sha256_hex(
            format!(
                "cloud-purge-cancel:v1:{kind}:{}:{operation}:{capability_hash}",
                account.id
            )
            .as_bytes(),
        );
        if self.format_version != 1
            || &self.account != account
            || self.operation_id != operation
            || !self.deny_begin
            || self.principal_hash.is_empty()
            || self.capability_binding_sha256 != binding
        {
            return Err("cloud cancellation receipt does not bind this exact operation".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CloudPurgeCancellation {
    #[serde(default)]
    pub control: Option<CloudPurgeControl>,
    pub cancellation_receipt: CloudPurgeCancellationReceipt,
}
