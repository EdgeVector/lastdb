use super::{limits, Version};
use crate::{AccountId, DecimalU64, ProtocolError};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageReadRequest {
    pub version: Version,
    pub account_id: AccountId,
}
impl UsageReadRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.account_id.as_str().len() != 32 {
            return Err(ProtocolError::invalid("v2.usage.account_id"));
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}

/// One shared account ledger covers V1, V2 and separately accounted obligations.
/// The three used categories are disjoint. A quota downgrade can leave used > quota.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageReadResponse {
    pub version: Version,
    pub account_id: AccountId,
    pub other_used_bytes: DecimalU64,
    pub reserved_bytes: DecimalU64,
    pub retained_bytes: DecimalU64,
    pub quota_bytes: DecimalU64,
}
impl UsageReadResponse {
    pub fn used_bytes(&self) -> Result<DecimalU64, ProtocolError> {
        self.other_used_bytes
            .get()
            .checked_add(self.reserved_bytes.get())
            .and_then(|used| used.checked_add(self.retained_bytes.get()))
            .map(DecimalU64::new)
            .ok_or_else(|| ProtocolError::limit("v2.usage.used_bytes"))
    }

    pub fn validate_for(&self, request: &UsageReadRequest) -> Result<(), ProtocolError> {
        request.validate()?;
        if self.account_id != request.account_id {
            return Err(ProtocolError::invalid("v2.usage.response.account_id"));
        }
        self.used_bytes()?;
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}
