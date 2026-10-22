//! Public retention references. Only authoritative service records grant holds.

use crate::limits::{encoded_json_bytes, RECEIPT_JSON_MAX_BYTES, REQUEST_JSON_MAX_BYTES};
use crate::{BackupStream, Digest, Id, ProtocolError, Version};
use serde::{Deserialize, Serialize};

/// The service binds this hold to its exact same-scope receipt, stream and commit.
/// A confirmed commit retains the hold for that backup's complete lifetime.
/// This public value alone establishes neither authority nor retained bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionHoldReference {
    pub receipt_id: Digest,
    pub hold_id: Id,
}

impl RetentionHoldReference {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        encoded_json_bytes(self, RECEIPT_JSON_MAX_BYTES).map(|_| ())
    }
}

/// The same stream, commit and receipt tuple returns its original hold.
/// The service verifies current authority, receipt scope and BlobPart kind.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionPrepareRequest {
    pub version: Version,
    pub stream: BackupStream,
    pub commit_id: Id,
    pub receipt_id: Digest,
}

impl RetentionPrepareRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.stream.validate()?;
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum RetentionPrepareResult {
    Ok {
        hold: RetentionHoldReference,
    },
    Error {
        receipt_id: Digest,
        error: ProtocolError,
    },
}

impl RetentionPrepareResult {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if let Self::Ok { hold } = self {
            hold.validate()?;
        }
        encoded_json_bytes(self, RECEIPT_JSON_MAX_BYTES).map(|_| ())
    }

    pub fn validate_for(&self, request: &RetentionPrepareRequest) -> Result<(), ProtocolError> {
        self.validate()?;
        request.validate()?;
        let receipt_id = match self {
            Self::Ok { hold } => &hold.receipt_id,
            Self::Error { receipt_id, .. } => receipt_id,
        };
        if receipt_id != &request.receipt_id {
            return Err(ProtocolError::invalid("retention_prepare.receipt_id"));
        }
        Ok(())
    }
}
