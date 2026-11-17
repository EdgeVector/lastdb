use crate::{AccountId, DecimalU64, ProtocolError};
use serde::{Deserialize, Serialize};

/// Personal account incarnation issued by cloud authority.
/// Physical provider routing remains private service state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloudScope {
    /// 32-character lowercase hex account identity.
    pub account_id: AccountId,
    /// Account generation issued with this account id.
    pub account_generation: DecimalU64,
}

impl CloudScope {
    /// Validate the account scope. `AccountId` enforces 32-character lowercase hex on construction.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        Ok(())
    }
}
