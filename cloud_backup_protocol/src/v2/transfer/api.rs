use super::super::limits::{
    encoded as encoded_json_bytes, CONTROL_JSON_MAX_BYTES as REQUEST_JSON_MAX_BYTES,
};
use super::super::{Destination, Job, Version};
use super::{TransferInput, TransferView, MAX_BATCH_ITEMS};
use crate::{DecimalU64, Digest, Id, ProtocolError, Text};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Prepare uploads for several objects on one job.
pub struct TransferPrepareRequest {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Objects to prepare. They must share one job and use distinct request ids.
    pub items: Vec<TransferInput>,
}

impl TransferPrepareRequest {
    /// Reject an empty or oversized batch, mixed jobs, a duplicate request id, or oversized JSON.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_count(self.items.len())?;
        for (offset, item) in self.items.iter().enumerate() {
            item.validate()?;
            if item.job != self.items[0].job {
                return Err(ProtocolError::invalid("transfer.items.job"));
            }
            if self.items[..offset]
                .iter()
                .any(|old| old.request_id == item.request_id)
            {
                return Err(ProtocolError::invalid("transfer.items.duplicate"));
            }
        }
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Ask for the state of several transfer requests on one job.
pub struct TransferStatusRequest {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Stream and commit id this value belongs to.
    pub job: Job,
    /// Request ids to read. Duplicates and an empty list are rejected.
    pub request_ids: Vec<Id>,
}

impl TransferStatusRequest {
    /// Validate the job, reject a bad id list, and check encoded size.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        validate_count(self.request_ids.len())?;
        for (offset, id) in self.request_ids.iter().enumerate() {
            if self.request_ids[..offset].contains(id) {
                return Err(ProtocolError::invalid("transfer.request_ids.duplicate"));
            }
        }
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES)
    }

    /// Require one input per request id, on this job, in the same order.
    pub fn validate_inputs(&self, inputs: &[TransferInput]) -> Result<(), ProtocolError> {
        self.validate()?;
        if inputs.len() != self.request_ids.len() {
            return Err(ProtocolError::invalid("v2.transfer_status.inputs"));
        }
        for (input, request_id) in inputs.iter().zip(&self.request_ids) {
            input.validate()?;
            if input.job != self.job || &input.request_id != request_id {
                return Err(ProtocolError::invalid("v2.transfer_status.identity"));
            }
        }
        Ok(())
    }
}

/// Resume and final verification select the same saved transfer, without worker revisions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferIdentityRequest {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Stream and commit id this value belongs to.
    pub job: Job,
    /// Client-chosen id of one transfer item.
    pub request_id: Id,
    /// SHA-256 of the transfer input this request names.
    pub input_sha256: Digest,
}

impl TransferIdentityRequest {
    /// Validate the job and the encoded size.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES)
    }

    /// Require this request's job, request id, and input digest to match `input`.
    pub fn validate_for(&self, input: &TransferInput) -> Result<(), ProtocolError> {
        self.validate()?;
        input.validate()?;
        if self.job != input.job
            || self.request_id != input.request_id
            || self.input_sha256 != input.input_sha256()?
        {
            return Err(ProtocolError::invalid("transfer.request.identity"));
        }
        Ok(())
    }
}

/// Resume request. It is a `TransferIdentityRequest`.
pub type TransferResumeRequest = TransferIdentityRequest;
/// Complete request. It is a `TransferIdentityRequest`.
pub type TransferCompleteRequest = TransferIdentityRequest;

/// A single presigned multipart UploadPart, with partNumber=1 and one exact uploadId.
/// Its signed headers are content-length and host; completion stays service-owned.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferMode {
    /// One presigned multipart UploadPart. Completion stays with the service.
    MultipartPart,
}

/// Temporary upload authority. Never save this URL in a durable record.
/// Absent mode preserves the signed conditional PUT and exact Content-Length.
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
    /// Allow an absent mode for R2 or an unbound destination, and multipart only for R2 or B2.
    pub fn validate_for(&self, input: &TransferInput) -> Result<(), ProtocolError> {
        self.validate()?;
        input.validate()?;
        if !matches!(
            (input.object.destination, self.transfer_mode),
            (None | Some(Destination::R2), None)
                | (
                    Some(Destination::R2 | Destination::B2),
                    Some(TransferMode::MultipartPart)
                )
        ) {
            return Err(ProtocolError::invalid("v2.upload_grant.transfer_mode"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// A new temporary upload URL for one saved transfer.
pub struct TransferResumeResponse {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Stream and commit id this value belongs to.
    pub job: Job,
    /// Client-chosen id of one transfer item.
    pub request_id: Id,
    /// SHA-256 of the transfer input this resume answers.
    pub input_sha256: Digest,
    /// Upload URL to use for this transfer.
    pub grant: UploadGrant,
}

impl TransferResumeResponse {
    /// Require the job, request id, and input digest to match `input`, and check the grant's mode.
    pub fn validate_for(&self, input: &TransferInput) -> Result<(), ProtocolError> {
        input.validate()?;
        self.grant.validate_for(input)?;
        if self.job != input.job
            || self.request_id != input.request_id
            || self.input_sha256 != input.input_sha256()?
        {
            return Err(ProtocolError::invalid("transfer.resume.identity"));
        }
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES)
    }
}

/// A resume race can return the current state instead of another temporary upload grant.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum TransferResumeResult {
    /// A new upload URL.
    Grant {
        /// Resume body carrying that URL.
        response: Box<TransferResumeResponse>,
    },
    /// The current transfer state, instead of another URL.
    State {
        /// Current view of the transfer.
        transfer: TransferView,
    },
    /// The resume was refused.
    Error {
        /// Request id the refusal names.
        request_id: Id,
        /// Why the resume was refused.
        error: ProtocolError,
    },
}

impl TransferResumeResult {
    /// Check a grant or state against `input`. An error must name that input's request id.
    pub fn validate_for(&self, input: &TransferInput) -> Result<(), ProtocolError> {
        input.validate()?;
        match self {
            Self::Grant { response } => response.validate_for(input)?,
            Self::State { transfer } => transfer.validate_for(input)?,
            Self::Error { request_id, .. } if request_id != &input.request_id => {
                return Err(ProtocolError::invalid("transfer.resume.error.request_id"));
            }
            Self::Error { .. } => {}
        }
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
/// One item in a prepare or status batch.
pub enum TransferItemResult {
    /// The item has a transfer view.
    Ok {
        /// View of this transfer item.
        transfer: TransferView,
    },
    /// The item failed.
    Error {
        /// Request id the failure names.
        request_id: Id,
        /// Why this item failed.
        error: ProtocolError,
    },
}

impl TransferItemResult {
    /// Check a view against `input`. An error must name that input's request id.
    pub fn validate_for(&self, input: &TransferInput) -> Result<(), ProtocolError> {
        input.validate()?;
        match self {
            Self::Ok { transfer } => transfer.validate_for(input)?,
            Self::Error { request_id, .. } if request_id != &input.request_id => {
                return Err(ProtocolError::invalid("transfer.error.request_id"));
            }
            Self::Error { .. } => {}
        }
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// One result per input, in the same order.
pub struct TransferBatchResponse {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Results paired with the request inputs.
    pub items: Vec<TransferItemResult>,
}

impl TransferBatchResponse {
    /// Require one valid result per input, one shared job, and distinct request ids.
    pub fn validate_for(&self, inputs: &[TransferInput]) -> Result<(), ProtocolError> {
        validate_count(inputs.len())?;
        if self.items.len() != inputs.len() {
            return Err(ProtocolError::invalid("transfer.response.count"));
        }
        for (offset, (item, input)) in self.items.iter().zip(inputs).enumerate() {
            item.validate_for(input)?;
            if input.job != inputs[0].job
                || inputs[..offset]
                    .iter()
                    .any(|old| old.request_id == input.request_id)
            {
                return Err(ProtocolError::invalid("transfer.response.duplicate"));
            }
        }
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES)
    }
}

fn validate_count(count: usize) -> Result<(), ProtocolError> {
    if count == 0 || count > MAX_BATCH_ITEMS {
        return Err(ProtocolError::limit("transfer.items"));
    }
    Ok(())
}
