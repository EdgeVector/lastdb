use super::{
    validate_parts, PartReceipt, ProviderBinding, TransferInput, TransferPhase, TransferRecord,
    MAX_BATCH_ITEMS, MAX_REQUEST_BYTES, PART_BYTES,
};
use crate::{BackupStream, DecimalU64, Id, ProtocolError, Text, Version};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferPrepareRequest {
    pub version: Version,
    pub items: Vec<TransferInput>,
}

impl TransferPrepareRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_batch_count(self.items.len())?;
        for (offset, item) in self.items.iter().enumerate() {
            item.validate()?;
            if self.items[..offset]
                .iter()
                .any(|old| old.request_id == item.request_id)
            {
                return Err(ProtocolError::invalid("transfer.items.duplicate"));
            }
        }
        crate::limits::encoded_json_bytes(self, MAX_REQUEST_BYTES).map(|_| ())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferStatusRequest {
    pub version: Version,
    pub stream: BackupStream,
    pub request_ids: Vec<Id>,
}

impl TransferStatusRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.stream.validate()?;
        validate_batch_count(self.request_ids.len())?;
        for (offset, id) in self.request_ids.iter().enumerate() {
            if self.request_ids[..offset].contains(id) {
                return Err(ProtocolError::invalid("transfer.request_ids.duplicate"));
            }
        }
        crate::limits::encoded_json_bytes(self, MAX_REQUEST_BYTES).map(|_| ())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferResumeRequest {
    pub version: Version,
    pub stream: BackupStream,
    pub request_id: Id,
    pub expected_revision: DecimalU64,
}

impl TransferResumeRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.stream.validate()?;
        crate::limits::encoded_json_bytes(self, MAX_REQUEST_BYTES).map(|_| ())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartUpload {
    pub index: u16,
    pub bytes: DecimalU64,
    pub url: Text<8192>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferResumeResponse {
    pub version: Version,
    pub request_id: Id,
    pub revision: DecimalU64,
    pub binding: ProviderBinding,
    pub expires_ms: DecimalU64,
    pub parts: Vec<PartUpload>,
}

impl TransferResumeResponse {
    pub fn validate_for(&self, record: &TransferRecord) -> Result<(), ProtocolError> {
        record.validate()?;
        if self.request_id != record.input.request_id
            || self.revision != record.revision
            || Some(&self.binding) != record.provider_binding.as_ref()
            || self.binding.upload_id.is_none()
            || !matches!(
                record.phase,
                TransferPhase::Ready | TransferPhase::Uploading
            )
            || self.parts.len() != record.input.part_count()
        {
            return Err(ProtocolError::invalid("transfer.resume.identity"));
        }
        for (offset, part) in self.parts.iter().enumerate() {
            let expected =
                (record.input.object.bytes.get() - offset as u64 * PART_BYTES).min(PART_BYTES);
            if usize::from(part.index) != offset + 1 || part.bytes.get() != expected {
                return Err(ProtocolError::invalid("transfer.resume.parts"));
            }
        }
        crate::limits::encoded_json_bytes(self, MAX_REQUEST_BYTES).map(|_| ())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferCompleteRequest {
    pub version: Version,
    pub stream: BackupStream,
    pub request_id: Id,
    pub expected_revision: DecimalU64,
    pub parts: Vec<PartReceipt>,
}

impl TransferCompleteRequest {
    pub fn validate_for(&self, input: &TransferInput) -> Result<(), ProtocolError> {
        if self.stream != input.stream || self.request_id != input.request_id {
            return Err(ProtocolError::invalid("transfer.complete.identity"));
        }
        validate_parts(input, &self.parts)?;
        crate::limits::encoded_json_bytes(self, MAX_REQUEST_BYTES).map(|_| ())
    }
}

/// Status carries no upload URL. Each error belongs to one exact request identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum TransferItemResult {
    Ok {
        request_id: Id,
        record: Box<TransferRecord>,
    },
    Error {
        request_id: Id,
        error: ProtocolError,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferBatchResponse {
    pub version: Version,
    pub items: Vec<TransferItemResult>,
}

impl TransferBatchResponse {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_batch_count(self.items.len())?;
        let mut ids = std::collections::BTreeSet::new();
        for item in &self.items {
            let request_id = match item {
                TransferItemResult::Ok { request_id, record } => {
                    record.validate()?;
                    if request_id != &record.input.request_id {
                        return Err(ProtocolError::invalid("transfer.response.request_id"));
                    }
                    request_id
                }
                TransferItemResult::Error { request_id, .. } => request_id,
            };
            if !ids.insert(request_id) {
                return Err(ProtocolError::invalid("transfer.response.duplicate"));
            }
        }
        crate::limits::encoded_json_bytes(self, MAX_REQUEST_BYTES).map(|_| ())
    }
}

fn validate_batch_count(count: usize) -> Result<(), ProtocolError> {
    if count == 0 || count > MAX_BATCH_ITEMS {
        return Err(ProtocolError::limit("transfer.items"));
    }
    Ok(())
}
