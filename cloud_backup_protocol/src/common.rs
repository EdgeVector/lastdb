use crate::{AccountId, DecimalU64, ProtocolError};
use serde::{Deserialize, Serialize};

/// Personal account incarnation issued by cloud authority.
/// Physical provider routing remains private service state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloudScope {
    pub account_id: AccountId,
    pub account_generation: DecimalU64,
}

impl CloudScope {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.account_id.as_str().len() != 32 {
            return Err(ProtocolError::invalid("cloud_scope.account_id"));
        }
        Ok(())
    }
}
