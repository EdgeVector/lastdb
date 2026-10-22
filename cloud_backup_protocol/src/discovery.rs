//! Account-scoped stream discovery. A directory entry is not publication proof.

use crate::{BackupStream, CloudScope, Digest, Id, ProtocolError, Text, Version};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const STREAMS_LIST_LIMIT_MAX: u16 = 100;

/// The service derives its keyed account directory from authenticated authority.
/// The cursor selects a bounded page within this exact scope.
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
            return Err(ProtocolError::invalid("streams_list.limit"));
        }
        crate::limits::encoded_json_bytes(self, crate::limits::REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

/// Read each selected stream's last accepted backup before restore.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamsListResponse {
    pub version: Version,
    pub scope: CloudScope,
    pub streams: Vec<BackupStream>,
    pub next_cursor: Option<Text<1024>>,
}

impl StreamsListResponse {
    pub fn validate_for(&self, request: &StreamsListRequest) -> Result<(), ProtocolError> {
        request.validate()?;
        self.scope.validate()?;
        if self.scope != request.scope || self.streams.len() > usize::from(request.limit) {
            return Err(ProtocolError::invalid("streams_list.response.scope_limit"));
        }
        let mut identities = BTreeSet::new();
        for stream in &self.streams {
            stream.validate()?;
            if stream.scope != self.scope || !identities.insert(&stream.stream_id) {
                return Err(ProtocolError::invalid("streams_list.response.stream"));
            }
        }
        crate::limits::encoded_json_bytes(self, crate::limits::REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

/// Optional durable user receipt. Account-authorized discovery also finds streams.
/// This locator names an accepted commit; it does not replace its authoritative record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryLocator {
    pub version: Version,
    pub stream: BackupStream,
    pub commit_id: Id,
    pub commit_sha256: Digest,
}

impl RecoveryLocator {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.stream.validate()?;
        crate::limits::encoded_json_bytes(self, crate::limits::RECEIPT_JSON_MAX_BYTES).map(|_| ())
    }
}
