//! The explicit range contract of one molecule in the typed logical format.
//!
//! The physical run format and the encrypted key bundle have separate versions.
//! This profile belongs in one molecule metadata row, never in each key.

use crate::{range_storage_profile::RangeStorageProfile, FieldValueType};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MoleculeRangeProfile {
    /// The empty structural slot of a molecule with no range field.
    EmptyV1,
    /// Exactly one reversible scalar payload, protected by the V2 block.
    TypedV1 { scalar: RangeStorageProfile },
}

impl MoleculeRangeProfile {
    pub fn for_field_type(range: Option<&FieldValueType>) -> Result<Self, String> {
        match range {
            None => Ok(Self::EmptyV1),
            Some(field_type) => Ok(Self::TypedV1 {
                scalar: RangeStorageProfile::typed(field_type)?,
            }),
        }
    }

    /// Direct field reads use this domain without a second schema lookup.
    #[must_use]
    pub fn comparison_type(self) -> FieldValueType {
        match self {
            Self::EmptyV1 => FieldValueType::String,
            Self::TypedV1 { scalar } => match scalar {
                RangeStorageProfile::StringUtf8V1 => FieldValueType::String,
                RangeStorageProfile::IntegerI64V1 => FieldValueType::Integer,
                RangeStorageProfile::DateDaysV1 => FieldValueType::Date,
                RangeStorageProfile::DateTimeNanosV1 => FieldValueType::DateTime,
                RangeStorageProfile::BooleanV1 => FieldValueType::Boolean,
            },
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
            Self::EmptyV1 => Err("a molecule without a range field has no text prefix".into()),
            Self::TypedV1 { scalar } => scalar.require_text_prefix(),
        }
    }

    #[must_use]
    pub fn format_id(self) -> &'static str {
        match self {
            Self::EmptyV1 => "empty-v1",
            Self::TypedV1 { scalar } => match scalar {
                RangeStorageProfile::StringUtf8V1 => "string-utf8-v1",
                RangeStorageProfile::IntegerI64V1 => "integer-i64-v1",
                RangeStorageProfile::DateDaysV1 => "date-days-v1",
                RangeStorageProfile::DateTimeNanosV1 => "date-time-nanos-v1",
                RangeStorageProfile::BooleanV1 => "boolean-v1",
            },
        }
    }
}

fn empty_slot(value: &str) -> Result<String, String> {
    if value.is_empty() {
        Ok(String::new())
    } else {
        Err("a molecule without a range field requires an empty range slot".into())
    }
}
