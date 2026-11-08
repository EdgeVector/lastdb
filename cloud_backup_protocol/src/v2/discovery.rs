//! Account-authorized discovery. A directory entry is not acceptance evidence.
use super::{limits, Stream, Version};
use crate::{CloudScope, ProtocolError, Text};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Largest `limit` a stream-list request may ask for.
pub const STREAMS_LIST_LIMIT_MAX: u16 = 100;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// List streams in one account scope.
pub struct StreamsListRequest {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Account id and account generation.
    pub scope: CloudScope,
    /// Maximum streams to return. Values outside 1..=`STREAMS_LIST_LIMIT_MAX` are rejected.
    pub limit: u16,
    /// Continuation from a previous page. Absent starts at the beginning.
    pub cursor: Option<Text<1024>>,
}
impl StreamsListRequest {
    /// Reject a bad scope, a limit outside 1..=100, or oversized JSON.
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
/// One page of streams in the requested scope.
pub struct StreamsListResponse {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Account id and account generation.
    pub scope: CloudScope,
    /// Streams in this page. The count cannot exceed the request limit.
    pub streams: Vec<Stream>,
    /// Cursor for the next page. A repeat of the request cursor is rejected.
    pub next_cursor: Option<Text<1024>>,
}
impl StreamsListResponse {
    /// Require the same scope, no extra streams, distinct stream ids, and a cursor that moves.
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
