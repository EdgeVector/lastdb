//! Temporary presigned object upload grants.

use super::limits::{encoded, CONTROL_JSON_MAX_BYTES};
use crate::{DecimalU64, ProtocolError, Text};
use serde::{Deserialize, Serialize};

/// Temporary upload authority for a signed conditional PUT with an exact `Content-Length`.
/// Never save this URL in a durable record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadGrant {
    /// Temporary upload URL. Do not store it on a durable record.
    pub url: Text<8192>,
    /// Unix epoch milliseconds when this grant expires. Zero is rejected.
    pub expires_ms: DecimalU64,
}

impl UploadGrant {
    /// Reject a zero expiry or oversized JSON.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.expires_ms.get() == 0 {
            return Err(ProtocolError::invalid("upload_grant.expires_ms"));
        }
        encoded(self, CONTROL_JSON_MAX_BYTES)
    }
}
