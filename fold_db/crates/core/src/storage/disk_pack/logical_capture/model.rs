use super::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Position {
    pub origin: String,
    pub sequence: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureEvent {
    pub version: u8,
    pub origin: String,
    pub sequence: u64,
    pub previous_sha256: String,
    pub mutation_ids: Vec<String>,
    pub outcomes: Vec<AcceptedFieldOutcome>,
    pub sha256: String,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventMetadata {
    pub origin: String,
    pub sequence: u64,
    pub sha256: String,
    pub previous_sha256: String,
    pub mutation_ids: Vec<String>,
    pub outcome_count: usize,
    pub encoded_event_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CaptureStatus {
    pub enabled: bool,
    pub position: Option<Position>,
    pub encoded_backlog_bytes: u64,
    pub backlog_cap_bytes: u64,
    pub recent: Vec<EventMetadata>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Head {
    pub version: u8,
    pub position: Position,
    pub encoded_backlog_bytes: u64,
}

impl Head {
    pub(super) fn initial(identity: &identity::Identity) -> StorageResult<Self> {
        Ok(Self {
            version: 1,
            position: Position {
                origin: identity.origin(),
                sequence: 0,
                sha256: identity.anchor()?,
            },
            encoded_backlog_bytes: 0,
        })
    }

    pub(super) fn validate(&self, identity: &identity::Identity) -> StorageResult<()> {
        if self.version != 1
            || self.position.origin != identity.origin()
            || self.encoded_backlog_bytes > BACKLOG_CAP_BYTES
        {
            return Err(invalid("logical capture head identity or size differs"));
        }
        unhex::<32>(&self.position.sha256)?;
        if self.position.sequence == 0
            && (self.position.sha256 != identity.anchor()? || self.encoded_backlog_bytes != 0)
        {
            return Err(invalid("logical capture initial anchor differs"));
        }
        Ok(())
    }
}

impl CaptureEvent {
    pub(super) fn authored(
        identity: &identity::Identity,
        head: &Head,
        mutation_ids: &[String],
        outcomes: Vec<AcceptedFieldOutcome>,
    ) -> StorageResult<Self> {
        if mutation_ids.is_empty()
            || mutation_ids.len() > MAX_MUTATIONS
            || mutation_ids
                .iter()
                .any(|id| id.is_empty() || id.len() > 128)
        {
            return Err(StorageError::MutationLimit {
                detail: "logical capture needs 1..=128 bounded mutation IDs".into(),
            });
        }
        let mut entry = Self {
            version: 1,
            origin: identity.origin(),
            sequence: head
                .position
                .sequence
                .checked_add(1)
                .ok_or_else(|| invalid("logical capture sequence exhausted"))?,
            previous_sha256: head.position.sha256.clone(),
            mutation_ids: mutation_ids.to_vec(),
            outcomes,
            sha256: String::new(),
            signature: String::new(),
        };
        let signing = entry.signing_bytes()?;
        entry.sha256 = hex(&Sha256::digest(&signing));
        entry.signature = identity.sign(&signing);
        // Leave two bytes for the JSON array delimiters around one event.
        bounded::serialized_size(&entry)?;
        Ok(entry)
    }

    fn signing_bytes(&self) -> StorageResult<Vec<u8>> {
        encode(&(
            "lastdb-logical-device-capture-v1",
            self.version,
            &self.origin,
            self.sequence,
            &self.previous_sha256,
            &self.mutation_ids,
            &self.outcomes,
        ))
    }

    pub(super) fn position(&self) -> Position {
        Position {
            origin: self.origin.clone(),
            sequence: self.sequence,
            sha256: self.sha256.clone(),
        }
    }

    pub(super) fn metadata(&self, encoded_event_bytes: usize) -> EventMetadata {
        EventMetadata {
            origin: self.origin.clone(),
            sequence: self.sequence,
            sha256: self.sha256.clone(),
            previous_sha256: self.previous_sha256.clone(),
            mutation_ids: self.mutation_ids.clone(),
            outcome_count: self.outcomes.len(),
            encoded_event_bytes: encoded_event_bytes as u64,
        }
    }
}
