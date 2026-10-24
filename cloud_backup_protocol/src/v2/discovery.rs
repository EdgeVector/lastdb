//! Account-authorized discovery. A directory entry is not acceptance evidence.
use super::{limits, AcceptedBackup, Stream, Version};
use crate::{CloudScope, Digest, Id, ProtocolError, Text};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const STREAMS_LIST_LIMIT_MAX: u16 = 100;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamsListRequest {
    pub version: Version,
    pub scope: CloudScope,
    pub limit: u16,
    pub cursor: Option<Text<1024>>,
}
impl StreamsListRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.scope.validate()?;
        if !(1..=STREAMS_LIST_LIMIT_MAX).contains(&self.limit) {
            return Err(ProtocolError::invalid("v2.streams_list.limit"));
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamsListResponse {
    pub version: Version,
    pub scope: CloudScope,
    pub streams: Vec<Stream>,
    pub next_cursor: Option<Text<1024>>,
}
impl StreamsListResponse {
    pub fn validate_for(&self, request: &StreamsListRequest) -> Result<(), ProtocolError> {
        request.validate()?;
        if self.scope != request.scope || self.streams.len() > usize::from(request.limit) {
            return Err(ProtocolError::invalid("v2.streams_list.scope_limit"));
        }
        if self.next_cursor.is_some() && self.next_cursor == request.cursor {
            return Err(ProtocolError::invalid("v2.streams_list.cursor_progress"));
        }
        let mut seen = BTreeSet::new();
        for stream in &self.streams {
            stream.validate()?;
            if stream.scope != self.scope || !seen.insert(&stream.stream_id) {
                return Err(ProtocolError::invalid("v2.streams_list.stream"));
            }
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}

/// Optional user receipt. Credentials plus discovery can recover this selection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryLocator {
    pub version: Version,
    pub stream: Stream,
    pub commit_id: Id,
    pub commit_sha256: Digest,
}
impl RecoveryLocator {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.stream.validate()?;
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }

    pub fn validate_for(&self, accepted: &AcceptedBackup) -> Result<(), ProtocolError> {
        self.validate()?;
        accepted.validate()?;
        if self.stream != accepted.job().stream
            || self.commit_id != accepted.job().commit_id
            || self.commit_sha256 != accepted.commit_sha256
        {
            return Err(ProtocolError::invalid("v2.recovery_locator.accepted"));
        }
        Ok(())
    }
}
