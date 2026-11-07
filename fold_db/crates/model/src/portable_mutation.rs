//! Data that the disk writer and replay planner share. Positions identify
//! immutable operations. A record's only version is the stamp of its last
//! winning operation.

mod codec;
mod record;

pub use record::{FileMarker, RecordData, RecordKey, StoredRecord};

use schema_types::record_keys::RecordKeys;
use schema_types::{PortableSchemaDefinition, PortableSchemaId};
use serde::{Deserialize, Serialize, Serializer};
use std::{collections::BTreeSet, fmt, str::FromStr};

/// A safe path component with the same spelling and ordering as a cloud ID.
/// Fixed-width lowercase hexadecimal text orders like the underlying UUID.
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

/// Field order is the replay order, with the newest stamp greatest.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Stamp {
    pub authored_at_ms: u64,
    pub position: Position,
}

/// Shared mutations use locally computed definition IDs. A local schema with
/// identical ID text still has a distinct identity.
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
    /// The typed definition of the entry's schema ID. A writer appends one
    /// before its stream's first record operation with that ID, so each
    /// stream carries every definition it uses. A reader verifies the ID.
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
    /// A record operation's primary key. A definition has none.
    #[must_use]
    pub fn primary_key(&self) -> Option<&RecordKey> {
        match self {
            Self::ReplaceRecord { primary_key, .. } | Self::DeleteRecord { primary_key } => {
                Some(primary_key)
            }
            Self::DefineSchema { .. } => None,
        }
    }

    /// The exact file version a replacement names.
    #[must_use]
    pub fn file_version(&self) -> Option<&Position> {
        match self {
            Self::ReplaceRecord { data, .. } => data.file.as_ref().map(|file| &file.version),
            Self::DeleteRecord { .. } | Self::DefineSchema { .. } => None,
        }
    }
}

/// Serialize through a JSON value, whose maps order their keys. A schema
/// definition holds hash maps, and a log entry has one canonical encoding.
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
                // Recompute the hash: only a definition that matches its ID installs.
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

    /// The definition of a `define_schema` entry.
    #[must_use]
    pub fn definition(&self) -> Option<&PortableSchemaDefinition> {
        match &self.operation {
            MutationOperation::DefineSchema { definition } => Some(&**definition),
            MutationOperation::ReplaceRecord { .. } | MutationOperation::DeleteRecord { .. } => {
                None
            }
        }
    }

    /// The caller supplies the exact schema revision for this identity and
    /// validates field values against it separately.
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
    /// A local stream can upload beyond its applied prefix during a freeze.
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

pub use codec::MAX_MUTATION_ENTRY_BYTES;
