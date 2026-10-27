use super::{identity, BackupError, Result};
use cloud_backup_protocol::{AccountId, Digest, Id};
use serde::{Deserialize, Serialize};
use zeroize::Zeroize;

/// A dedicated owner request. The API key never enters the ordinary backup command enum.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SensitiveConnectionRequest {
    pub environment: String,
    pub expected_user_hash: Option<AccountId>,
    pub api_key: String,
}

impl Drop for SensitiveConnectionRequest {
    fn drop(&mut self) {
        self.api_key.zeroize();
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Credentials {
    pub api_url: String,
    pub api_key: String,
}

impl Drop for Credentials {
    fn drop(&mut self) {
        self.api_key.zeroize();
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ManualConnectionRecord {
    pub version: u32,
    pub account_id: AccountId,
    pub source_home: Id,
    pub environment: String,
    pub endpoint: String,
    pub credential_sha256: Digest,
    pub origin: String,
    pub registration_attempted: bool,
    pub automatic_backup_enabled: bool,
}

impl ManualConnectionRecord {
    pub fn validate(&self, host: &crate::host::Host) -> Result<()> {
        let (account, source) = identity(host)?;
        if self.version != 1
            || self.account_id != account
            || self.source_home != source
            || self.origin != "imported"
            || self.registration_attempted
            || self.automatic_backup_enabled
        {
            return Err(BackupError::Identity);
        }
        Ok(())
    }
}

/// Public connection state contains no credentials and starts no work.
#[derive(Clone, Debug, Serialize)]
pub struct ManualConnectionStatus {
    pub account_id: AccountId,
    pub source_home: Id,
    pub environment: String,
    pub manual_backup_enabled: bool,
    pub automatic_backup_enabled: bool,
    pub registration_attempted: bool,
}

impl From<ManualConnectionRecord> for ManualConnectionStatus {
    fn from(record: ManualConnectionRecord) -> Self {
        Self {
            account_id: record.account_id,
            source_home: record.source_home,
            environment: record.environment,
            manual_backup_enabled: true,
            automatic_backup_enabled: false,
            registration_attempted: false,
        }
    }
}
