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
    /// Version 1: the field outcomes of a format 4 to 6 home.
    pub outcomes: Vec<AcceptedFieldOutcome>,
    /// Version 2: the record outcomes of a format 7 home.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub records: Vec<AcceptedRecordOutcome>,
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

/// The authored effects of one request. A format 7 home writes records and
/// authors version 2; another home writes field tips and authors version 1.
pub(crate) enum Outcomes {
    Fields(Vec<AcceptedFieldOutcome>),
    Records(Vec<AcceptedRecordOutcome>),
}

impl CaptureEvent {
    pub(super) fn authored(
        identity: &identity::Identity,
        head: &Head,
        mutation_ids: &[String],
        authored: Outcomes,
    ) -> StorageResult<Self> {
        verify::mutation_ids(mutation_ids)?;
        let (version, outcomes, records) = match authored {
            Outcomes::Fields(outcomes) => (1, outcomes, Vec::new()),
            Outcomes::Records(records) => (2, Vec::new(), records),
        };
        verify::outcomes(&outcomes, mutation_ids)?;
        verify::records(&records, mutation_ids)?;
        let mut entry = Self {
            version,
            origin: identity.origin(),
            sequence: head
                .position
                .sequence
                .checked_add(1)
                .ok_or_else(|| invalid("logical capture sequence exhausted"))?,
            previous_sha256: head.position.sha256.clone(),
            mutation_ids: mutation_ids.to_vec(),
            outcomes,
            records,
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

    pub(super) fn signing_bytes(&self) -> StorageResult<Vec<u8>> {
        if self.version == 2 {
            return encode(&(
                "lastdb-logical-device-capture-v2",
                self.version,
                &self.origin,
                self.sequence,
                &self.previous_sha256,
                &self.mutation_ids,
                &self.records,
            ));
        }
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

    /// The field outcomes of version 1, or the record outcomes of version 2.
    pub(crate) fn outcome_count(&self) -> usize {
        self.outcomes.len() + self.records.len()
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
            outcome_count: self.outcome_count(),
            encoded_event_bytes: encoded_event_bytes as u64,
        }
    }
}
