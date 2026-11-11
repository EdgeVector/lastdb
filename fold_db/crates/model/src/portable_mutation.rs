//! Portable mutation entries, stamps, stream cursors, and canonical JSON bounds.

mod record;

pub use record::{
    RecordData, RecordKey, StoredRecord, MAX_MUTATION_ENTRY_BYTES, MAX_PORTABLE_RECORD_BYTES,
};

use schema_types::record_keys::RecordKeys;
use schema_types::{PortableSchemaDefinition, PortableSchemaId};
use serde::{Deserialize, Serialize, Serializer};
use std::{collections::BTreeSet, fmt, str::FromStr};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct StreamId(String);

impl StreamId {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for StreamId {
    type Err = PortableMutationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_from(value.to_owned())
    }
}

impl TryFrom<String> for StreamId {
    type Error = PortableMutationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let canonical = value.len() == 36
            && value.bytes().enumerate().all(|(index, byte)| {
                if matches!(index, 8 | 13 | 18 | 23) {
                    byte == b'-'
                } else {
                    matches!(byte, b'0'..=b'9' | b'a'..=b'f')
                }
            });
        if canonical && value != "00000000-0000-0000-0000-000000000000" {
            Ok(Self(value))
        } else {
            Err(PortableMutationError::Invalid(
                "stream ID must be a canonical nonnil UUID",
            ))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Position {
    pub source_stream: StreamId,
    pub sequence: u64,
}

impl Position {
    pub fn validate(&self) -> Result<(), PortableMutationError> {
        if self.sequence == 0 {
            return Err(PortableMutationError::Invalid(
                "mutation sequence starts at one",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Stamp {
    pub authored_at_ms: u64,
    pub position: Position,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileMarker {
    pub version: Position,
    pub name: Option<String>,
    pub media_type: Option<String>,
    pub size_bytes: u64,
    pub sha256: String,
}

impl FileMarker {
    fn validate(&self, record_position: &Position) -> Result<(), PortableMutationError> {
        self.version.validate()?;
        if self.sha256.len() != 64
            || !self
                .sha256
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        {
            return Err(PortableMutationError::Invalid(
                "file marker requires a lowercase SHA-256",
            ));
        }
        if self.version.source_stream == record_position.source_stream
            && self.version.sequence > record_position.sequence
        {
            return Err(PortableMutationError::Invalid(
                "file version cannot follow its record in the same stream",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(
    tag = "scope",
    content = "id",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum SchemaIdentity {
    Shared(String),
    Local(String),
}

impl SchemaIdentity {
    pub fn validate(&self) -> Result<(), PortableMutationError> {
        let (Self::Shared(id) | Self::Local(id)) = self;
        if id.is_empty() || id.len() > 1024 || id.chars().any(char::is_control) {
            return Err(PortableMutationError::Invalid(
                "schema identity must contain 1 to 1024 bytes without control characters",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum MutationOperation {
    DefineSchema {
        #[serde(serialize_with = "ordered")]
        definition: Box<PortableSchemaDefinition>,
    },
    ReplaceRecord {
        primary_key: RecordKey,
        data: RecordData,
    },
    DeleteRecord {
        primary_key: RecordKey,
    },
}

impl MutationOperation {
    #[must_use]
    pub fn primary_key(&self) -> Option<&RecordKey> {
        match self {
            Self::ReplaceRecord { primary_key, .. } | Self::DeleteRecord { primary_key } => {
                Some(primary_key)
            }
            Self::DefineSchema { .. } => None,
        }
    }

    #[must_use]
    pub fn file_version(&self) -> Option<&Position> {
        match self {
            Self::ReplaceRecord { data, .. } => data.file.as_ref().map(|file| &file.version),
            Self::DeleteRecord { .. } | Self::DefineSchema { .. } => None,
        }
    }
}

fn ordered<T: Serialize, S: Serializer>(value: &T, serializer: S) -> Result<S::Ok, S::Error> {
    serde_json::to_value(value)
        .map_err(serde::ser::Error::custom)?
        .serialize(serializer)
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationEntry {
    pub stamp: Stamp,
    pub schema: SchemaIdentity,
    pub operation: MutationOperation,
}

impl MutationEntry {
    pub fn validate(&self) -> Result<(), PortableMutationError> {
        self.stamp.position.validate()?;
        self.schema.validate()?;
        match &self.operation {
            MutationOperation::DefineSchema { definition } => {
                let SchemaIdentity::Shared(id) = &self.schema else {
                    return Err(PortableMutationError::Invalid(
                        "a schema definition requires a shared schema ID",
                    ));
                };
                let id: PortableSchemaId = id.parse().map_err(|_| {
                    PortableMutationError::Invalid("schema definition ID must be a SHA-256")
                })?;
                definition.verify(&id).map_err(|_| {
                    PortableMutationError::Invalid("schema definition differs from its ID")
                })?;
            }
            MutationOperation::ReplaceRecord { primary_key, data } => {
                primary_key.validate()?;
                data.validate(&self.stamp.position)?;
            }
            MutationOperation::DeleteRecord { primary_key } => primary_key.validate()?,
        }
        Ok(())
    }

    #[must_use]
    pub fn definition(&self) -> Option<&PortableSchemaDefinition> {
        match &self.operation {
            MutationOperation::DefineSchema { definition } => Some(&**definition),
            MutationOperation::ReplaceRecord { .. } | MutationOperation::DeleteRecord { .. } => {
                None
            }
        }
    }

    pub fn validate_keys(&self, keys: &RecordKeys) -> Result<(), PortableMutationError> {
        keys.validate()
            .map_err(|_| PortableMutationError::Invalid("invalid record key definitions"))?;
        self.operation
            .primary_key()
            .ok_or(PortableMutationError::Invalid(
                "a schema definition has no record keys",
            ))?
            .validate_against(&keys.primary)?;
        if let MutationOperation::ReplaceRecord { data, .. } = &self.operation {
            data.validate_alternates(keys)?;
        }
        Ok(())
    }

    #[must_use]
    pub fn max_encoded_bytes(&self) -> usize {
        record::entry_max_encoded_bytes(self)
    }

    pub fn encode(&self) -> Result<Vec<u8>, PortableMutationError> {
        record::encode_entry(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, PortableMutationError> {
        record::decode_entry(bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppliedDelete {
    pub authored_at_ms: u64,
    pub delete_position: Position,
}

impl From<Stamp> for AppliedDelete {
    fn from(stamp: Stamp) -> Self {
        Self {
            authored_at_ms: stamp.authored_at_ms,
            delete_position: stamp.position,
        }
    }
}

impl From<AppliedDelete> for Stamp {
    fn from(delete: AppliedDelete) -> Self {
        Self {
            authored_at_ms: delete.authored_at_ms,
            position: delete.delete_position,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamProgress {
    pub applied_through: u64,
    pub applied_through_ms: Option<u64>,
    pub uploaded_through: Option<u64>,
}

impl StreamProgress {
    pub fn validate(&self) -> Result<(), PortableMutationError> {
        if (self.applied_through != 0) != self.applied_through_ms.is_some() {
            return Err(PortableMutationError::Invalid(
                "an applied position and its timestamp must be present together",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HomeReplayState {
    pub local_stream_id: StreamId,
    pub participating_streams: BTreeSet<StreamId>,
    pub delete_reclaimed_through_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PortableMutationError {
    Invalid(&'static str),
    TooLarge { max_bytes: usize },
}

impl fmt::Display for PortableMutationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => formatter.write_str(message),
            Self::TooLarge { max_bytes } => write!(
                formatter,
                "encoded portable value exceeds the {max_bytes}-byte limit"
            ),
        }
    }
}

impl std::error::Error for PortableMutationError {}
