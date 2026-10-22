use super::{TransferInput, TransferView, MAX_BATCH_ITEMS};
use crate::limits::{encoded_json_bytes, REQUEST_JSON_MAX_BYTES};
use crate::{BackupJob, DecimalU64, Digest, Id, ProtocolError, Text, Version};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferPrepareRequest {
    pub version: Version,
    pub items: Vec<TransferInput>,
}

impl TransferPrepareRequest {
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
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferStatusRequest {
    pub version: Version,
    pub job: BackupJob,
    pub request_ids: Vec<Id>,
}

impl TransferStatusRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        validate_count(self.request_ids.len())?;
        for (offset, id) in self.request_ids.iter().enumerate() {
            if self.request_ids[..offset].contains(id) {
                return Err(ProtocolError::invalid("transfer.request_ids.duplicate"));
            }
        }
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

/// Resume and final verification select the same saved transfer, without worker revisions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferIdentityRequest {
    pub version: Version,
    pub job: BackupJob,
    pub request_id: Id,
    pub input_sha256: Digest,
}

impl TransferIdentityRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES).map(|_| ())
    }

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

pub type TransferResumeRequest = TransferIdentityRequest;
pub type TransferCompleteRequest = TransferIdentityRequest;

/// Temporary create-once PUT authority. Never save this URL in a durable record.
/// The client sends signed If-None-Match: * and the input's exact Content-Length.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadGrant {
    pub url: Text<8192>,
    pub expires_ms: DecimalU64,
}

impl UploadGrant {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.expires_ms.get() == 0 {
            return Err(ProtocolError::invalid("upload_grant.expires_ms"));
        }
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferResumeResponse {
    pub version: Version,
    pub job: BackupJob,
    pub request_id: Id,
    pub input_sha256: Digest,
    pub grant: UploadGrant,
}

impl TransferResumeResponse {
    pub fn validate_for(&self, input: &TransferInput) -> Result<(), ProtocolError> {
        input.validate()?;
        self.grant.validate()?;
        if self.job != input.job
            || self.request_id != input.request_id
            || self.input_sha256 != input.input_sha256()?
        {
            return Err(ProtocolError::invalid("transfer.resume.identity"));
        }
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

/// A resume race can return the current state instead of another temporary upload grant.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum TransferResumeResult {
    Grant {
        response: Box<TransferResumeResponse>,
    },
    State {
        transfer: TransferView,
    },
    Error {
        request_id: Id,
        error: ProtocolError,
    },
}

impl TransferResumeResult {
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
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum TransferItemResult {
    Ok {
        transfer: TransferView,
    },
    Error {
        request_id: Id,
        error: ProtocolError,
    },
}

impl TransferItemResult {
    pub fn validate_for(&self, input: &TransferInput) -> Result<(), ProtocolError> {
        input.validate()?;
        match self {
            Self::Ok { transfer } => transfer.validate_for(input)?,
            Self::Error { request_id, .. } if request_id != &input.request_id => {
                return Err(ProtocolError::invalid("transfer.error.request_id"));
            }
            Self::Error { .. } => {}
        }
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferBatchResponse {
    pub version: Version,
    pub items: Vec<TransferItemResult>,
}

impl TransferBatchResponse {
    pub fn validate_for(&self, inputs: &[TransferInput]) -> Result<(), ProtocolError> {
        validate_count(inputs.len())?;
        if self.items.len() != inputs.len() {
            return Err(ProtocolError::invalid("transfer.response.count"));
        }
        for (offset, (item, input)) in self.items.iter().zip(inputs).enumerate() {
            item.validate_for(input)?;
            if inputs[..offset]
                .iter()
                .any(|old| old.request_id == input.request_id)
            {
                return Err(ProtocolError::invalid("transfer.response.duplicate"));
            }
        }
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

fn validate_count(count: usize) -> Result<(), ProtocolError> {
    if count == 0 || count > MAX_BATCH_ITEMS {
        return Err(ProtocolError::limit("transfer.items"));
    }
    Ok(())
}
