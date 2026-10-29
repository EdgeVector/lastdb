//! Account context read. The reply is advisory; each mutation still enforces authority.

use super::{limits, Destination, Version};
use crate::{AccountId, CloudScope, ProtocolError};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Ask which account generation is current and whether new work is allowed.
pub struct ContextReadRequest {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// 32-character lowercase hex account identity.
    pub account_id: AccountId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    /// Providers the caller requires. More than two, or a duplicate, is rejected.
    pub required_destinations: Vec<Destination>,
}
impl ContextReadRequest {
    /// Reject a bad destination list, an account id that is not 32 characters, or oversized JSON.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        super::common::validate_destinations(&self.required_destinations)?;
        if self.account_id.as_str().len() != 32 {
            return Err(ProtocolError::invalid("v2.context.account_id"));
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}

/// Advisory context only. Each mutation transaction enforces current authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextReadResponse {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Account scope the service reports for the requested account id.
    pub scope: CloudScope,
    /// Whether the service currently allows new backup work. Mutations still enforce authority themselves.
    pub new_work_allowed: bool,
}
impl ContextReadResponse {
    /// Require the response account id to match the request, and check encoded size.
    pub fn validate_for(&self, request: &ContextReadRequest) -> Result<(), ProtocolError> {
        request.validate()?;
        self.scope.validate()?;
        if self.scope.account_id != request.account_id {
            return Err(ProtocolError::invalid("v2.context.response.account_id"));
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}
