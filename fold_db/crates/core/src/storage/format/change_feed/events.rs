//! Pure event, sequence and retention encoder. No store or runtime handle.

use super::super::{CHANGE_FEED_EVENT_PREFIX, CHANGE_FEED_MAX_EVENTS};
use super::{EncodedFeed, PreparedEvents, APPEND_BYTES, APPEND_EVENTS, LAST_SEQ_KEY};
use crate::schema::SchemaError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChangeFeedEvent {
    pub seq: u64,
    pub mutation_id: String,
    pub schema: String,
    pub operation: String,
    pub hash: Option<String>,
    pub range: Option<String>,
    pub committed_at_ms: u64,
    pub background_tasks_drained: bool,
    pub convergence_pending: bool,
}

pub(super) fn event_key(seq: u64) -> String {
    format!("{CHANGE_FEED_EVENT_PREFIX}{seq:020}")
}

pub(super) struct Encoder {
    first: u64,
    end: u64,
    count: usize,
    ready: Vec<ChangeFeedEvent>,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
    bytes: usize,
}

impl Encoder {
    pub(super) fn new(last_seq: u64, count: usize) -> Result<Self, SchemaError> {
        let first = last_seq.checked_add(1).ok_or_else(exhausted)?;
        let end = last_seq.checked_add(count as u64).ok_or_else(exhausted)?;
        Ok(Self {
            first,
            end,
            count,
            ready: Vec::with_capacity(count),
            rows: Vec::with_capacity(count.min(APPEND_EVENTS) + 1),
            bytes: 0,
        })
    }

    pub(super) fn push(
        &mut self,
        mut event: ChangeFeedEvent,
    ) -> Result<Option<EncodedFeed>, SchemaError> {
        if self.ready.len() >= self.count {
            return Err(exhausted());
        }
        event.seq = self.first + self.ready.len() as u64;
        let key = event_key(event.seq).into_bytes();
        let body = serde_json::to_vec(&event).map_err(invalid)?;
        self.bytes = self
            .bytes
            .saturating_add(key.len())
            .saturating_add(body.len())
            .saturating_add(256);
        if self.bytes > APPEND_BYTES {
            return Err(SchemaError::InvalidData(
                "change-feed batch exceeds its byte limit".into(),
            ));
        }
        self.rows.push((key, body));
        self.ready.push(event);
        Ok((self.rows.len() == APPEND_EVENTS)
            .then(|| EncodedFeed::Put(std::mem::take(&mut self.rows))))
    }

    pub(super) fn finish(
        mut self,
    ) -> Result<(EncodedFeed, Option<EncodedFeed>, PreparedEvents), SchemaError> {
        if self.ready.len() != self.count {
            return Err(exhausted());
        }
        self.rows.push((
            LAST_SEQ_KEY.as_bytes().to_vec(),
            serde_json::to_vec(&self.end).map_err(invalid)?,
        ));
        let expired = (self.end > CHANGE_FEED_MAX_EVENTS).then(|| {
            EncodedFeed::Delete(
                (self.first.saturating_sub(CHANGE_FEED_MAX_EVENTS).max(1)
                    ..=self.end - CHANGE_FEED_MAX_EVENTS)
                    .map(|sequence| event_key(sequence).into_bytes())
                    .collect(),
            )
        });
        Ok((
            EncodedFeed::Put(self.rows),
            expired,
            PreparedEvents {
                end: self.end,
                events: self.ready,
            },
        ))
    }
}

fn exhausted() -> SchemaError {
    SchemaError::InvalidData("change-feed sequence exhausted".into())
}
fn invalid(error: impl std::fmt::Display) -> SchemaError {
    SchemaError::InvalidData(error.to_string())
}
