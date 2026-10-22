//! Public create-once object transfer identities. Provider locations and worker state stay private.
use crate::limits::{encoded_json_bytes, RECEIPT_JSON_MAX_BYTES, REQUEST_JSON_MAX_BYTES};
use crate::{
    BackupJob, CloudScope, Digest, Id, ObjectIdentity, PendingReason, ProtocolError,
    TerminalFailure, Version,
};
use serde::{Deserialize, Serialize};

pub const MAX_BATCH_ITEMS: usize = 4;
/// Every upload grant requires this signed condition and the input's exact Content-Length.
pub const UPLOAD_IF_NONE_MATCH: &str = "*";

/// The service returns an authoritative receipt only after exact byte verification.
/// An internally consistent client value does not establish service authority or retention.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectReceipt {
    pub version: Version,
    pub receipt_id: Digest,
    pub scope: CloudScope,
    pub object: ObjectIdentity,
}

impl ObjectReceipt {
    pub fn identity_sha256(&self) -> Result<Digest, ProtocolError> {
        crate::input_digest(
            "lastdb:native-backup:object-receipt:v1",
            &(self.version, &self.scope, &self.object),
        )
    }

    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.scope.validate()?;
        self.object.validate()?;
        if self.receipt_id != self.identity_sha256()? {
            return Err(ProtocolError::invalid("object_receipt.receipt_id"));
        }
        encoded_json_bytes(self, RECEIPT_JSON_MAX_BYTES).map(|_| ())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferInput {
    pub version: Version,
    pub request_id: Id,
    pub job: BackupJob,
    pub object: ObjectIdentity,
}

impl TransferInput {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        self.object.validate()?;
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES).map(|_| ())
    }

    pub fn input_sha256(&self) -> Result<Digest, ProtocolError> {
        crate::input_digest("lastdb:native-backup:transfer:v1", self)
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
            Self::Rejected { failure } => failure.validate()?,
        }
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES).map(|_| ())
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
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

mod api;
pub use api::*;
