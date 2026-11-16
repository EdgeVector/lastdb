//! Local author stamps, applied-delete markers, and record replacement intents.

use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PortableMutationError {
    Invalid(&'static str),
}

impl fmt::Display for PortableMutationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for PortableMutationError {}

pub mod record_replacement {
    use crate::keys::KeyValue;
    use crate::record_value::FileMarker;
    use schema_types::record_keys::RecordKeys;
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use std::collections::BTreeMap;

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct RecordTarget {
        pub schema: String,
        pub key: KeyValue,
        pub alternates: BTreeMap<String, Option<KeyValue>>,
    }

    impl RecordTarget {
        pub fn validate(&self, def: &RecordKeys) -> Result<(), String> {
            def.validate()?;
            if self.schema.is_empty() {
                return Err("a record target requires a schema".into());
            }
            def.primary
                .validate_coordinates(self.key.hash.as_deref(), self.key.range.as_deref())?;
            if def.alternates.keys().ne(self.alternates.keys()) {
                return Err("supply exactly every declared alternate key".into());
            }
            for (name, key) in &self.alternates {
                let declared = &def.alternates[name];
                match key {
                    Some(k) => {
                        declared.validate_coordinates(k.hash.as_deref(), k.range.as_deref())?;
                    }
                    None if declared.allow_absent => {}
                    None => return Err(format!("alternate {name} requires an address")),
                }
            }
            Ok(())
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
    pub enum RecordExpectation {
        Absent,
        Revision { revision: String },
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct RecordMetadata {
        pub source_file_name: Option<String>,
        pub metadata: BTreeMap<String, String>,
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct RecordReplacement {
        pub target: RecordTarget,
        pub expected: RecordExpectation,
        pub fields: BTreeMap<String, Value>,
        pub metadata: Option<RecordMetadata>,
        pub file: Option<FileMarker>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub inline_file: Option<FileMarker>,
    }

    impl RecordReplacement {
        pub fn validate(&self, def: &RecordKeys) -> Result<(), String> {
            self.target.validate(def)?;
            if matches!(&self.expected, RecordExpectation::Revision { revision } if revision.is_empty())
            {
                return Err("a replacement revision must be nonempty".into());
            }
            if self.file.as_ref().is_some_and(|f| !f.has_valid_hash()) {
                return Err("a record file marker requires a lowercase SHA-256".into());
            }
            if self.inline_file.as_ref().is_some_and(|f| !f.fits_inline()) {
                return Err("an inline file marker has an invalid digest, label or size".into());
            }
            Ok(())
        }
    }
}
