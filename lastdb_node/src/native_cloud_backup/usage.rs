//! Timestamped local observations. A failed observation never discards a receipt.
use super::{store::Store, BackupError, Result};
use cloud_backup_protocol::usage::{UsageReadRequest, UsageReadResponse};
use cloud_backup_protocol::{AccountId, Version};
use fold_db::sync::auth::native_backup::NativeBackupClient;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CachedUsage {
    pub observed_at_ms: u64,
    pub usage: UsageReadResponse,
}
impl CachedUsage {
    pub fn validate(&self, account: &AccountId) -> Result<()> {
        self.usage.validate_for(&UsageReadRequest {
            version: Version,
            account_id: account.clone(),
        })?;
        if self.observed_at_ms == 0 {
            return Err(BackupError::Invalid);
        }
        Ok(())
    }
}
pub(super) async fn refresh(
    store: &Store,
    client: &NativeBackupClient,
    account: &AccountId,
) -> Result<()> {
    let usage = client
        .usage_read(&UsageReadRequest {
            version: Version,
            account_id: account.clone(),
        })
        .await?;
    let observed_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| BackupError::Invalid)?
        .as_millis()
        .try_into()
        .map_err(|_| BackupError::Limit)?;
    let value = CachedUsage {
        observed_at_ms,
        usage,
    };
    value.validate(account)?;
    store.write_base("usage.json", &value)
}
