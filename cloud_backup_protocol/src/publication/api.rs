use super::{CommitInput, CommitReceipt, CommitRecord, HeadCandidate, HeadIdentity};
use crate::{BackupStream, Id, ProtocolError, Version};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitRequest {
    pub version: Version,
    pub input: CommitInput,
}

impl CommitRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.input.validate()?;
        crate::limits::encoded_json_bytes(self, crate::limits::REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitStatusRequest {
    pub version: Version,
    pub stream: BackupStream,
    pub commit_id: Id,
}

impl CommitStatusRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.stream.validate()?;
        crate::limits::encoded_json_bytes(self, crate::limits::REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum CommitResponse {
    Ok { record: Box<CommitRecord> },
    Error { commit_id: Id, error: ProtocolError },
}

impl CommitResponse {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if let Self::Ok { record } = self {
            record.validate()?;
        }
        crate::limits::encoded_json_bytes(self, crate::limits::REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeadReadRequest {
    pub version: Version,
    pub stream: BackupStream,
}

impl HeadReadRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.stream.validate()?;
        crate::limits::encoded_json_bytes(self, crate::limits::REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishedHead {
    pub identity: HeadIdentity,
    pub candidate: HeadCandidate,
    pub receipt: CommitReceipt,
}

impl PublishedHead {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.identity.validate()?;
        self.candidate.validate()?;
        if self.identity.commit_id != self.candidate.input.commit_id
            || self.identity.ordinal != self.candidate.ordinal
            || self.identity.head_sha256 != self.candidate.head_sha256()?
        {
            return Err(ProtocolError::invalid("head.identity"));
        }
        self.receipt.validate_for(&self.candidate, &self.identity)?;
        crate::limits::encoded_json_bytes(self, crate::limits::REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeadReadResponse {
    pub version: Version,
    pub stream: BackupStream,
    /// None means an authoritative absent head, never an uncertain provider outcome.
    pub head: Option<PublishedHead>,
}

impl HeadReadResponse {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.stream.validate()?;
        if let Some(head) = &self.head {
            head.validate()?;
            if head.candidate.input.stream != self.stream {
                return Err(ProtocolError::invalid("head.stream"));
            }
        }
        crate::limits::encoded_json_bytes(self, crate::limits::REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum HeadReadResult {
    Ok { response: Box<HeadReadResponse> },
    Error { error: ProtocolError },
}

impl HeadReadResult {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if let Self::Ok { response } = self {
            response.validate()?;
        }
        crate::limits::encoded_json_bytes(self, crate::limits::REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}
