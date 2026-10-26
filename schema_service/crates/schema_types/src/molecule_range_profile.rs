//! The explicit range contract of one molecule in the typed logical format.
//!
//! The physical run format and the encrypted key bundle have separate versions.
//! This profile belongs in one molecule metadata row, never in each key. A
//! missing profile is a legacy format, not permission to infer a field type.

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

    /// The legacy payload cannot enter a typed-format metadata record.
    pub fn validate(self) -> Result<(), String> {
        match self {
            Self::TypedV1 {
                scalar: RangeStorageProfile::LegacyUtf8V1,
            } => Err("a typed molecule requires an explicit scalar range profile".into()),
            _ => Ok(()),
        }
    }

    pub fn require_field_type(self, range: Option<&FieldValueType>) -> Result<(), String> {
        self.validate()?;
        if self != Self::for_field_type(range)? {
            return Err("the schema range type differs from the stored molecule profile".into());
        }
        Ok(())
    }

    /// Direct field reads use this domain without a second schema lookup.
    pub fn comparison_type(self) -> Result<FieldValueType, String> {
        self.validate()?;
        match self {
            Self::EmptyV1 => Ok(FieldValueType::String),
            Self::TypedV1 { scalar } => match scalar {
                RangeStorageProfile::StringUtf8V1 => Ok(FieldValueType::String),
                RangeStorageProfile::IntegerI64V1 => Ok(FieldValueType::Integer),
                RangeStorageProfile::DateDaysV1 => Ok(FieldValueType::Date),
                RangeStorageProfile::DateTimeNanosV1 => Ok(FieldValueType::DateTime),
                RangeStorageProfile::BooleanV1 => Ok(FieldValueType::Boolean),
                RangeStorageProfile::LegacyUtf8V1 => {
                    Err("a typed profile cannot use legacy order".into())
                }
            },
        }
    }

    pub fn encode_value(self, api: &str) -> Result<String, String> {
        self.validate()?;
        match self {
            Self::EmptyV1 => empty_slot(api),
            Self::TypedV1 { scalar } => scalar.encode_value(api),
        }
    }

    pub fn decode_value(self, stored: &str) -> Result<String, String> {
        self.validate()?;
        match self {
            Self::EmptyV1 => empty_slot(stored),
            Self::TypedV1 { scalar } => scalar.decode_value(stored),
        }
    }

    pub fn require_text_prefix(self) -> Result<(), String> {
        self.validate()?;
        match self {
            Self::EmptyV1 => Err("a molecule without a range field has no text prefix".into()),
            Self::TypedV1 { scalar } => scalar.require_text_prefix(),
        }
    }

    pub fn format_id(self) -> Result<&'static str, String> {
        self.validate()?;
        Ok(match self {
            Self::EmptyV1 => "empty-v1",
            Self::TypedV1 { scalar } => match scalar {
                RangeStorageProfile::LegacyUtf8V1 => "legacy-utf8-v1",
                RangeStorageProfile::StringUtf8V1 => "string-utf8-v1",
                RangeStorageProfile::IntegerI64V1 => "integer-i64-v1",
                RangeStorageProfile::DateDaysV1 => "date-days-v1",
                RangeStorageProfile::DateTimeNanosV1 => "date-time-nanos-v1",
                RangeStorageProfile::BooleanV1 => "boolean-v1",
            },
        })
    }
}

fn empty_slot(value: &str) -> Result<String, String> {
    if value.is_empty() {
        Ok(String::new())
    } else {
        Err("a molecule without a range field requires an empty range slot".into())
    }
}
