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
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Id of this receipt. Validation requires it to equal `identity_sha256`.
    pub receipt_id: Digest,
    /// Account id and account generation.
    pub scope: CloudScope,
    /// Stored object this receipt names.
    pub object: Object,
}

impl ObjectReceipt {
    /// Domain-separated SHA-256 of the version, scope, and object. The receipt id is not an input.
    pub fn identity_sha256(&self) -> Result<Digest, ProtocolError> {
        super::hash::input(
            "lastdb:cloud-byte-backup:object-receipt:v2",
            &(self.version, &self.scope, &self.object),
        )
    }

    /// Validate scope and object, require `receipt_id` to equal `identity_sha256`, and check encoded size.
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
/// One object a job asks to upload or reuse.
pub struct TransferInput {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Client-chosen id of one transfer item.
    pub request_id: Id,
    /// Stream and commit id this value belongs to.
    pub job: Job,
    /// Object bytes to transfer.
    pub object: Object,
}

impl TransferInput {
    /// Validate the job and object, and reject oversized JSON.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        self.object.validate()?;
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES)
    }

    /// Domain-separated SHA-256 of this input, after validation.
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
    /// The transfer is not finished. A timeout does not become Verified or Rejected.
    Pending {
        /// Why the transfer is still unfinished.
        reason: PendingReason,
    },
    /// The service verified these exact bytes, whether newly uploaded or reused.
    Verified {
        /// Receipt for the verified object bytes.
        receipt: ObjectReceipt,
    },
    /// This input cannot succeed.
    Rejected {
        /// Why this input cannot succeed.
        failure: TerminalFailure,
    },
}

impl TransferStatus {
    /// Check a verified receipt's scope and object, or a rejected failure's encoded size, against `input`.
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
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Client-chosen id of one transfer item.
    pub request_id: Id,
    /// SHA-256 of the transfer input this view answers.
    pub input_sha256: Digest,
    /// Pending, verified, or rejected state of that input.
    pub state: TransferStatus,
}

impl TransferView {
    /// Require this view's request id and input digest to match `input`, then validate the state.
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
