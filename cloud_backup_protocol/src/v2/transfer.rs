//! Temporary presigned object upload grants.

use super::limits::{
    encoded as encoded_json_bytes, CONTROL_JSON_MAX_BYTES as REQUEST_JSON_MAX_BYTES,
};
use crate::{DecimalU64, ProtocolError, Text};
use serde::{Deserialize, Serialize};

/// A single presigned multipart UploadPart, with partNumber=1 and one exact uploadId.
/// Its signed headers are content-length and host; completion stays service-owned.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferMode {
    /// One presigned multipart UploadPart. Completion stays with the service.
    MultipartPart,
}

/// Temporary upload authority. Never save this URL in a durable record.
/// Absent mode is the signed conditional PUT with an exact Content-Length.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadGrant {
    /// Temporary upload URL. Do not store it on a durable record.
    pub url: Text<8192>,
    /// Unix epoch milliseconds when this grant expires. Zero is rejected.
    pub expires_ms: DecimalU64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// How the URL is signed. Absent means a conditional PUT with an exact content length.
    pub transfer_mode: Option<TransferMode>,
}

impl UploadGrant {
    /// Reject a zero expiry or oversized JSON.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.expires_ms.get() == 0 {
            return Err(ProtocolError::invalid("upload_grant.expires_ms"));
        }
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES)
    }
}
