//! Local author stamps and the applied-delete marker they leave behind.

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
