use super::{limits, Version};
use crate::{AccountId, CloudScope, ProtocolError};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextReadRequest {
    pub version: Version,
    pub account_id: AccountId,
}
impl ContextReadRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
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
    pub version: Version,
    pub scope: CloudScope,
    pub new_work_allowed: bool,
}
impl ContextReadResponse {
    pub fn validate_for(&self, request: &ContextReadRequest) -> Result<(), ProtocolError> {
        request.validate()?;
        self.scope.validate()?;
        if self.scope.account_id != request.account_id {
            return Err(ProtocolError::invalid("v2.context.response.account_id"));
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}
