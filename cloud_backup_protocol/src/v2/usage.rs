//! Account usage read. The three used-byte categories are disjoint.

use super::{limits, Version};
use crate::{AccountId, DecimalU64, ProtocolError};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Read one account's usage ledger.
pub struct UsageReadRequest {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// 32-character lowercase hex account identity.
    pub account_id: AccountId,
}
impl UsageReadRequest {
    /// Reject an account id that is not 32 characters, or oversized JSON.
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
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// 32-character lowercase hex account identity.
    pub account_id: AccountId,
    /// Bytes in use outside the reserved and retained categories.
    pub other_used_bytes: DecimalU64,
    /// Bytes reserved for work that is not yet retained.
    pub reserved_bytes: DecimalU64,
    /// Bytes retained by accepted backups.
    pub retained_bytes: DecimalU64,
    /// Account quota, in bytes. Used bytes may exceed it after a downgrade.
    pub quota_bytes: DecimalU64,
}
impl UsageReadResponse {
    /// Sum the three used categories. Overflow is a resource-limit error.
    pub fn used_bytes(&self) -> Result<DecimalU64, ProtocolError> {
        self.other_used_bytes
            .get()
            .checked_add(self.reserved_bytes.get())
            .and_then(|used| used.checked_add(self.retained_bytes.get()))
            .map(DecimalU64::new)
            .ok_or_else(|| ProtocolError::limit("v2.usage.used_bytes"))
    }

    /// Require the same account id, a sum that fits in u64, and a bounded encoding.
    pub fn validate_for(&self, request: &UsageReadRequest) -> Result<(), ProtocolError> {
        request.validate()?;
        if self.account_id != request.account_id {
            return Err(ProtocolError::invalid("v2.usage.response.account_id"));
        }
        self.used_bytes()?;
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}
