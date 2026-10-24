use super::super::{limits, AcceptedBackup, Stream, Version};
use crate::{Id, ProtocolError};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeadReadRequest {
    pub version: Version,
    pub stream: Stream,
}
impl HeadReadRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.stream.validate()?;
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}

/// A new pending job never hides the previous accepted backup.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeadReadResponse {
    pub version: Version,
    pub stream: Stream,
    pub accepted: Option<AcceptedBackup>,
    pub active_commit_id: Option<Id>,
}
impl HeadReadResponse {
    pub fn validate_for(&self, request: &HeadReadRequest) -> Result<(), ProtocolError> {
        request.validate()?;
        self.stream.validate()?;
        if self.stream != request.stream {
            return Err(ProtocolError::invalid("v2.head_read.stream"));
        }
        if let Some(backup) = &self.accepted {
            backup.validate()?;
            if backup.job().stream != self.stream
                || self.active_commit_id.as_ref() == Some(&backup.job().commit_id)
            {
                return Err(ProtocolError::invalid("v2.head_read.accepted"));
            }
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum HeadReadResult {
    Ok { response: Box<HeadReadResponse> },
    Error { error: ProtocolError },
}
impl HeadReadResult {
    pub fn validate_for(&self, request: &HeadReadRequest) -> Result<(), ProtocolError> {
        request.validate()?;
        if let Self::Ok { response } = self {
            response.validate_for(request)?;
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}
