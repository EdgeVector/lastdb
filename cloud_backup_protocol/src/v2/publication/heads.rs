use super::super::{limits, AcceptedBackup, Stream, Version};
use crate::{Id, ProtocolError};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Read the accepted head and any active commit on one stream.
pub struct HeadReadRequest {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Account scope and stream id.
    pub stream: Stream,
}
impl HeadReadRequest {
    /// Validate the stream and the encoded size.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.stream.validate()?;
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}

/// A new pending job never hides the previous accepted backup.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeadReadResponse {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Account scope and stream id.
    pub stream: Stream,
    /// Latest accepted backup. Absent when the stream has none. It cannot share the active commit id.
    pub accepted: Option<AcceptedBackup>,
    /// Commit id of an in-progress job, if one is open.
    pub active_commit_id: Option<Id>,
}
impl HeadReadResponse {
    /// Require the same stream, a valid accepted backup on that stream, and an active id that differs from it.
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
/// Head read outcome.
pub enum HeadReadResult {
    /// The head was read.
    Ok {
        /// Head state for the request.
        response: Box<HeadReadResponse>,
    },
    /// The head was not read.
    Error {
        /// Why the head was not read.
        error: ProtocolError,
    },
}
impl HeadReadResult {
    /// Validate a success body against the request. An error arm only checks encoded size.
    pub fn validate_for(&self, request: &HeadReadRequest) -> Result<(), ProtocolError> {
        request.validate()?;
        if let Self::Ok { response } = self {
            response.validate_for(request)?;
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}
