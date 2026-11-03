use crate::cloud::SavedConnection;
use cloud_backup_protocol::{AccountId, Id};
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

impl From<SavedConnection> for ManualConnectionStatus {
    fn from(record: SavedConnection) -> Self {
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
