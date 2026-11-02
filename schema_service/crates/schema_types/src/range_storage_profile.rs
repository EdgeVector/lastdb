//! Versioned, reversible range order, independent of encryption.
//!
//! A profile describes one field key, not each key. The field key metadata row
//! stores it when schema publication establishes the field key.

use crate::{key_comparison::ordered_type, FieldValueType};
use serde::{Deserialize, Serialize};

mod ordered_scalar;

/// Stable wire identifiers. Unknown versions fail deserialization.
/// These are order encodings, not cryptographic constructions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeStorageProfile {
    StringUtf8V1,
    IntegerI64V1,
    DateDaysV1,
    DateTimeNanosV1,
    BooleanV1,
}

impl RangeStorageProfile {
    /// The profile of a declared range type. A nullable declaration still
    /// requires a non-null key.
    pub fn typed(field_type: &FieldValueType) -> Result<Self, String> {
        match ordered_type(field_type)? {
            FieldValueType::String => Ok(Self::StringUtf8V1),
            FieldValueType::Integer => Ok(Self::IntegerI64V1),
            FieldValueType::Date => Ok(Self::DateDaysV1),
            FieldValueType::DateTime => Ok(Self::DateTimeNanosV1),
            FieldValueType::Boolean => Ok(Self::BooleanV1),
            _ => Err("this field type has no typed storage profile".into()),
        }
    }

    /// Preserve the full UTF-8 string or produce fixed-width lowercase hex.
    /// Byte order of valid outputs is the declared scalar order.
    pub fn encode_value(self, api: &str) -> Result<String, String> {
        ordered_scalar::encode(self, api)
    }

    /// Decode only the selected format. Never guess a profile from key bytes.
    /// Numeric/date outputs use canonical API text; record values stay unchanged.
    pub fn decode_value(self, stored: &str) -> Result<String, String> {
        ordered_scalar::decode(self, stored)
    }

    pub fn require_text_prefix(self) -> Result<(), String> {
        match self {
            Self::StringUtf8V1 => Ok(()),
            _ => Err("a text prefix requires a String range storage profile".into()),
        }
    }
}
