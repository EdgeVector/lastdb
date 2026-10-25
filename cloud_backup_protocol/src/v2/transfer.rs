//! Public create-once object transfer identities. Provider locations and worker state stay private.
use super::limits::{
    encoded as encoded_json_bytes, CONTROL_JSON_MAX_BYTES as RECEIPT_JSON_MAX_BYTES,
    CONTROL_JSON_MAX_BYTES as REQUEST_JSON_MAX_BYTES,
};
use super::{Job, Object, Version};
use crate::{CloudScope, Digest, Id, PendingReason, ProtocolError, TerminalFailure};
use serde::{Deserialize, Serialize};

pub use super::limits::MAX_BATCH_ITEMS;
/// Legacy and R2 upload grants require this signed condition and exact Content-Length.
pub const UPLOAD_IF_NONE_MATCH: &str = "*";

/// The service returns an authoritative receipt only after exact byte verification.
/// An internally consistent client value does not establish service authority or retention.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectReceipt {
    pub version: Version,
    pub receipt_id: Digest,
    pub scope: CloudScope,
    pub object: Object,
}

impl ObjectReceipt {
    pub fn identity_sha256(&self) -> Result<Digest, ProtocolError> {
        super::hash::input(
            "lastdb:cloud-byte-backup:object-receipt:v2",
            &(self.version, &self.scope, &self.object),
        )
    }

    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.scope.validate()?;
        self.object.validate()?;
        if self.receipt_id != self.identity_sha256()? {
            return Err(ProtocolError::invalid("object_receipt.receipt_id"));
        }
        encoded_json_bytes(self, RECEIPT_JSON_MAX_BYTES)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferInput {
    pub version: Version,
    pub request_id: Id,
    pub job: Job,
    pub object: Object,
}

impl TransferInput {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        self.object.validate()?;
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES)
    }

    pub fn input_sha256(&self) -> Result<Digest, ProtocolError> {
        self.validate()?;
        super::hash::input("lastdb:cloud-byte-backup:transfer:v2", self)
    }
}

/// Pending is not a terminal failure. A timeout cannot produce Verified or Rejected.
/// Verified covers both a new upload and safe reuse of the same protected bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum TransferStatus {
    Pending { reason: PendingReason },
    Verified { receipt: ObjectReceipt },
    Rejected { failure: TerminalFailure },
}

impl TransferStatus {
    pub fn validate_for(&self, input: &TransferInput) -> Result<(), ProtocolError> {
        input.validate()?;
        match self {
            Self::Pending { .. } => {}
            Self::Verified { receipt } => {
                receipt.validate()?;
                if receipt.scope != input.job.stream.scope || receipt.object != input.object {
                    return Err(ProtocolError::invalid("transfer.receipt.identity"));
                }
            }
            Self::Rejected { failure } => encoded_json_bytes(failure, REQUEST_JSON_MAX_BYTES)?,
        }
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES)
    }
}

/// The input digest binds the response to the complete saved job and object identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferView {
    pub version: Version,
    pub request_id: Id,
    pub input_sha256: Digest,
    pub state: TransferStatus,
}

impl TransferView {
    pub fn validate_for(&self, input: &TransferInput) -> Result<(), ProtocolError> {
        if self.request_id != input.request_id || self.input_sha256 != input.input_sha256()? {
            return Err(ProtocolError::invalid("transfer.response.identity"));
        }
        self.state.validate_for(input)?;
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES)
    }
}

mod api;
pub use api::*;
