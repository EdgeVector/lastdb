use super::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Position {
    pub origin: String,
    pub sequence: u64,
    pub sha256: String,
}

/// Authored prefix metadata stored beside an old capture event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct EventMetadata {
    pub origin: String,
    pub sequence: u64,
    pub sha256: String,
    pub previous_sha256: String,
    pub mutation_ids: Vec<String>,
    pub outcome_count: usize,
    pub encoded_event_bytes: u64,
}

/// Version 1 device head in an old native snapshot.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Head {
    pub version: u8,
    pub position: Position,
    pub encoded_backlog_bytes: u64,
}

impl EventMetadata {
    pub(super) fn verify_prefix(
        &self,
        origin: &str,
        sequence: u64,
        sha256: &str,
    ) -> StorageResult<()> {
        verify::mutation_ids(&self.mutation_ids)?;
        unhex::<32>(&self.previous_sha256)?;
        if self.origin != origin
            || self.sequence != sequence
            || self.sha256 != sha256
            || self.outcome_count > MAX_OUTCOMES
            || self.encoded_event_bytes == 0
            || self.encoded_event_bytes > (MAX_EVENT_BYTES - 2) as u64
        {
            return Err(invalid("logical capture authored prefix differs"));
        }
        Ok(())
    }
}
