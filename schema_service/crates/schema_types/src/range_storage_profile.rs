//! Versioned, reversible scalar range order and field range profiles.

use crate::field_value_type::{date_day, datetime_nanos};
use crate::{key_comparison::ordered_type, FieldValueType};
use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

const FIRST_DAY: i32 = -365;
const LAST_DAY: i32 = 3_652_059;
const FIRST_NANOS: i128 = -62_167_219_200_000_000_000;
const END_NANOS: i128 = 253_402_300_800_000_000_000;
const NANOS_PER_SECOND: i128 = 1_000_000_000;

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

    pub fn encode_value(self, api: &str) -> Result<String, String> {
        match self {
            Self::StringUtf8V1 => Ok(api.to_owned()),
            Self::IntegerI64V1 => api
                .parse::<i64>()
                .map(|v| format!("{:016x}", (v as u64) ^ (1_u64 << 63)))
                .map_err(|_| "expected a signed 64-bit range key".into()),
            Self::DateDaysV1 => Ok(format!("{:06x}", date_day(api)? - FIRST_DAY)),
            Self::DateTimeNanosV1 => Ok(format!("{:018x}", datetime_nanos(api)? - FIRST_NANOS)),
            Self::BooleanV1 => match api {
                "false" => Ok("0".into()),
                "true" => Ok("1".into()),
                _ => Err("expected a Boolean range key".into()),
            },
        }
    }

    pub fn decode_value(self, stored: &str) -> Result<String, String> {
        match self {
            Self::StringUtf8V1 => Ok(stored.to_owned()),
            Self::IntegerI64V1 => {
                let v = parse_hex(stored, 16)? as u64;
                Ok(((v ^ (1_u64 << 63)) as i64).to_string())
            }
            Self::DateDaysV1 => {
                let biased = parse_hex(stored, 6)?;
                if biased > (LAST_DAY - FIRST_DAY) as u128 {
                    return Err("stored Date range is outside years 0000..9999".into());
                }
                NaiveDate::from_num_days_from_ce_opt(FIRST_DAY + biased as i32)
                    .map(|date| date.format("%Y-%m-%d").to_string())
                    .ok_or_else(|| "stored Date range is invalid".into())
            }
            Self::DateTimeNanosV1 => {
                let biased = parse_hex(stored, 18)?;
                if biased >= (END_NANOS - FIRST_NANOS) as u128 {
                    return Err("stored DateTime range is outside UTC years 0000..9999".into());
                }
                let nanos = FIRST_NANOS + biased as i128;
                DateTime::<Utc>::from_timestamp(
                    nanos.div_euclid(NANOS_PER_SECOND) as i64,
                    nanos.rem_euclid(NANOS_PER_SECOND) as u32,
                )
                .map(|instant| instant.to_rfc3339_opts(SecondsFormat::Nanos, true))
                .ok_or_else(|| "stored DateTime range is invalid".into())
            }
            Self::BooleanV1 => match stored {
                "0" => Ok("false".into()),
                "1" => Ok("true".into()),
                _ => Err("stored Boolean range is invalid".into()),
            },
        }
    }

    pub fn require_text_prefix(self) -> Result<(), String> {
        (self == Self::StringUtf8V1)
            .then_some(())
            .ok_or_else(|| "a text prefix requires a String range storage profile".into())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FieldRangeProfile {
    EmptyV1,
    TypedV1 { scalar: RangeStorageProfile },
}

impl FieldRangeProfile {
    pub fn for_field_type(range: Option<&FieldValueType>) -> Result<Self, String> {
        match range {
            None => Ok(Self::EmptyV1),
            Some(ty) => RangeStorageProfile::typed(ty).map(|scalar| Self::TypedV1 { scalar }),
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
    value
        .is_empty()
        .then(String::new)
        .ok_or_else(|| "a field key without a range field requires an empty range slot".into())
}

fn parse_hex(text: &str, width: usize) -> Result<u128, String> {
    if text.len() != width
        || !text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err("stored typed range has a noncanonical width or alphabet".into());
    }
    u128::from_str_radix(text, 16).map_err(|_| "stored typed range is invalid".into())
}
