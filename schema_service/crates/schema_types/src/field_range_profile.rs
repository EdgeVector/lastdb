//! The explicit range contract of one field key in the typed logical format.
//!
//! The physical run format and the encrypted key bundle have separate versions.
//! This profile belongs in one field key metadata row, never in each key.

use crate::{range_storage_profile::RangeStorageProfile, FieldValueType};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FieldRangeProfile {
    /// The empty structural slot of a field key with no range field.
    EmptyV1,
    /// Exactly one reversible scalar payload, protected by the V2 block.
    TypedV1 { scalar: RangeStorageProfile },
}

impl FieldRangeProfile {
    pub fn for_field_type(range: Option<&FieldValueType>) -> Result<Self, String> {
        match range {
            None => Ok(Self::EmptyV1),
            Some(field_type) => Ok(Self::TypedV1 {
                scalar: RangeStorageProfile::typed(field_type)?,
            }),
        }
    }

    pub fn encode_value(self, api: &str) -> Result<String, String> {
        match self {
            Self::EmptyV1 => empty_slot(api),
            Self::TypedV1 { scalar } => scalar.encode_value(api),
        }
    }

    pub fn decode_value(self, stored: &str) -> Result<String, String> {
        match self {
            Self::EmptyV1 => empty_slot(stored),
            Self::TypedV1 { scalar } => scalar.decode_value(stored),
        }
    }

    pub fn require_text_prefix(self) -> Result<(), String> {
        match self {
            Self::EmptyV1 => Err("a field key without a range field has no text prefix".into()),
            Self::TypedV1 { scalar } => scalar.require_text_prefix(),
        }
    }
}

fn empty_slot(value: &str) -> Result<String, String> {
    if value.is_empty() {
        Ok(String::new())
    } else {
        Err("a field key without a range field requires an empty range slot".into())
    }
}
