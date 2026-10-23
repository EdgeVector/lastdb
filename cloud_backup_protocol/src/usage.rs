//! Account-wide allocation observation. Legacy observed bytes exclude future grants.
use crate::{limits, AccountId, DecimalU64, ProtocolError, Version};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageReadRequest {
    pub version: Version,
    pub account_id: AccountId,
}
impl UsageReadRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        limits::encoded_json_bytes(self, limits::REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageReadResponse {
    pub version: Version,
    pub account_id: AccountId,
    pub legacy_observed_bytes: DecimalU64,
    pub reserved_bytes: DecimalU64,
    pub retained_bytes: DecimalU64,
    pub quota_bytes: DecimalU64,
    pub quota_used_bytes: DecimalU64,
}
impl UsageReadResponse {
    pub fn validate_for(&self, request: &UsageReadRequest) -> Result<(), ProtocolError> {
        request.validate()?;
        let total = self
            .legacy_observed_bytes
            .get()
            .checked_add(self.reserved_bytes.get())
            .and_then(|bytes| bytes.checked_add(self.retained_bytes.get()))
            .ok_or_else(|| ProtocolError::limit("usage.total"))?;
        if self.account_id != request.account_id || self.quota_used_bytes.get() != total {
            return Err(ProtocolError::invalid("usage.response"));
        }
        limits::encoded_json_bytes(self, limits::REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}
