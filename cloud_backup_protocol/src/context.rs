//! Authenticated personal-account context, without a job or caller-selected scope.
//!
//! This observation never replaces conditional authority checks on new effects.

use crate::{limits, AccountId, CloudScope, ProtocolError, Version};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextReadRequest {
    pub version: Version,
}

impl ContextReadRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        limits::encoded_json_bytes(self, limits::REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

/// The service derives scope from its authenticated subject and current control.
/// `new_work_enabled` reports the native feature gate, not quota admission.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextReadResponse {
    pub version: Version,
    pub scope: CloudScope,
    pub open_for_writes: bool,
    pub new_work_enabled: bool,
}

impl ContextReadResponse {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.scope.validate()?;
        // Generation zero represents authoritative absence of a control record.
        if self.scope.account_generation.get() == 0 && !self.open_for_writes {
            return Err(ProtocolError::invalid("context.legacy_authority"));
        }
        limits::encoded_json_bytes(self, limits::REQUEST_JSON_MAX_BYTES).map(|_| ())
    }

    /// Bind the credential's service subject to the independently derived owner.
    pub fn validate_for(
        &self,
        request: &ContextReadRequest,
        expected_account: &AccountId,
    ) -> Result<(), ProtocolError> {
        request.validate()?;
        self.validate()?;
        if self.scope.account_id != *expected_account {
            return Err(ProtocolError::invalid("context.account_id"));
        }
        Ok(())
    }
}
