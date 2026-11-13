//! Pure wire/domain schema types, field scalar profiles, and portable schema IDs.

use chrono::{DateTime, Datelike, NaiveDate, Timelike};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, fmt};

mod alternate_keys;
mod declarative_schemas;
mod range_profile;

pub use alternate_keys::{record_keys, AlternateKey, MissingAlternateKey};
pub use declarative_schemas::DeclarativeSchemaDefinition;
pub use range_profile::{
    compute_field_hash, field_range_profile, key_comparison, PortableSchemaId,
};

pub const IDENTITY_HASH_ALGO_VERSION: u32 = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedDeclaration {
    pub namespace: String,
    pub local_schema_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityRecompute {
    Recomputed,
    RefusedDowngrade { stored: u32, binary: u32 },
}

#[must_use]
pub fn refuses_identity_downgrade(
    stored_hash: Option<&str>,
    stored_version: Option<u32>,
) -> Option<u32> {
    stored_version.filter(|stored| {
        stored_hash.is_some_and(|h| !h.is_empty()) && *stored > IDENTITY_HASH_ALGO_VERSION
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum FieldValueType {
    String,
    Integer,
    Float,
    Number,
    Boolean,
    Null,
    Array(Box<Self>),
    Map(Box<Self>),
    Object(BTreeMap<String, Self>),
    OneOf(Vec<Self>),
    Any,
    Date,
    DateTime,
}

impl FieldValueType {
    pub fn validate(&self, value: &Value) -> Result<(), String> {
        let check = |ok: bool, exp: &str| ok.then_some(()).ok_or_else(|| type_err(exp, value));
        match self {
            Self::Any => Ok(()),
            Self::Date => value
                .as_str()
                .ok_or_else(|| type_err("Date", value))
                .and_then(|s| date_day(s).map(|_| ())),
            Self::DateTime => value
                .as_str()
                .ok_or_else(|| type_err("DateTime", value))
                .and_then(|s| datetime_nanos(s).map(|_| ())),
            Self::Null => check(value.is_null(), "Null"),
            Self::String => check(value.is_string(), "String"),
            Self::Boolean => check(value.is_boolean(), "Boolean"),
            Self::Integer => check(value.as_i64().is_some(), "Integer"),
            Self::Float => check(value.as_f64().is_some(), "Float"),
            Self::Number => check(value.is_number(), "Number"),
            Self::Array(elem_ty) => value
                .as_array()
                .ok_or_else(|| type_err("Array", value))?
                .iter()
                .enumerate()
                .try_for_each(|(i, v)| elem_ty.validate(v).map_err(|e| format!("Array[{i}]: {e}"))),
            Self::Map(val_ty) => value
                .as_object()
                .ok_or_else(|| type_err("Map", value))?
                .iter()
                .try_for_each(|(k, v)| {
                    val_ty.validate(v).map_err(|e| format!("Map[\"{k}\"]: {e}"))
                }),
            Self::Object(fields) => {
                let obj = value.as_object().ok_or_else(|| type_err("Object", value))?;
                fields.iter().try_for_each(|(name, ty)| {
                    ty.validate(obj.get(name).unwrap_or(&Value::Null))
                        .map_err(|e| format!(".{name}: {e}"))
                })
            }
            Self::OneOf(variants) => variants
                .iter()
                .any(|v| v.validate(value).is_ok())
                .then_some(())
                .ok_or_else(|| {
                    format!(
                        "value does not match any variant of OneOf({self}), got {}",
                        json_type_name(value)
                    )
                }),
        }
    }
}

fn type_err(expected: &str, got: &Value) -> String {
    format!("expected {expected}, got {}", json_type_name(got))
}

fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "Null",
        Value::Bool(_) => "Boolean",
        Value::Number(_) => "Number",
        Value::String(_) => "String",
        Value::Array(_) => "Array",
        Value::Object(_) => "Object",
    }
}

impl fmt::Display for FieldValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String => f.write_str("String"),
            Self::Date => f.write_str("Date"),
            Self::DateTime => f.write_str("DateTime"),
            Self::Integer => f.write_str("Integer"),
            Self::Float => f.write_str("Float"),
            Self::Number => f.write_str("Number"),
            Self::Boolean => f.write_str("Boolean"),
            Self::Null => f.write_str("Null"),
            Self::Any => f.write_str("Any"),
            Self::Array(t) => write!(f, "Array<{t}>"),
            Self::Map(v) => write!(f, "Map<String, {v}>"),
            Self::Object(fields) => {
                let parts: Vec<_> = fields.iter().map(|(k, v)| format!("{k}: {v}")).collect();
                write!(f, "{{{}}}", parts.join(", "))
            }
            Self::OneOf(variants) => {
                let parts: Vec<_> = variants.iter().map(ToString::to_string).collect();
                f.write_str(&parts.join(" | "))
            }
        }
    }
}

pub(crate) fn date_day(text: &str) -> Result<i32, String> {
    let err = || "expected Date in YYYY-MM-DD form with a valid calendar date".to_owned();
    if !date_shape(text.as_bytes()) {
        return Err(err());
    }
    NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .map(|d| d.num_days_from_ce())
        .map_err(|_| err())
}

pub(crate) fn datetime_nanos(text: &str) -> Result<i128, String> {
    let err = || {
        "expected DateTime in RFC 3339 form with an offset, years 0000..9999, at most nine fractional digits, and no leap second".to_owned()
    };
    if !datetime_shape(text.as_bytes()) {
        return Err(err());
    }
    let instant = DateTime::parse_from_rfc3339(text).map_err(|_| err())?;
    if instant.nanosecond() >= 1_000_000_000 || !(0..=9999).contains(&instant.to_utc().year()) {
        return Err(err());
    }
    Ok(i128::from(instant.timestamp()) * 1_000_000_000 + i128::from(instant.nanosecond()))
}

fn date_shape(b: &[u8]) -> bool {
    b.len() == 10
        && b.iter().enumerate().all(|(i, c)| {
            if matches!(i, 4 | 7) {
                *c == b'-'
            } else {
                c.is_ascii_digit()
            }
        })
}

fn datetime_shape(b: &[u8]) -> bool {
    if !(20..=35).contains(&b.len())
        || !date_shape(&b[..10])
        || !matches!(b[10], b'T' | b't')
        || b[13] != b':'
        || b[16] != b':'
        || ![11, 12, 14, 15, 17, 18]
            .iter()
            .all(|&i| b[i].is_ascii_digit())
    {
        return false;
    }
    let mut off = &b[19..];
    if off.first() == Some(&b'.') {
        let digits = off[1..]
            .iter()
            .copied()
            .take_while(u8::is_ascii_digit)
            .count();
        if !(1..=9).contains(&digits) {
            return false;
        }
        off = &off[1 + digits..];
    }
    matches!(off, [b'Z' | b'z'])
        || (off.len() == 6
            && matches!(off[0], b'+' | b'-')
            && off[3] == b':'
            && [1, 2, 4, 5].iter().all(|&i| off[i].is_ascii_digit()))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DeclarativeSchemaType {
    Single,
    Hash,
    Range,
    HashRange,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct KeyConfig {
    pub hash_field: Option<String>,
    pub range_field: Option<String>,
}

#[derive(Debug)]
pub enum FoldDbError {
    Config(String),
}

impl fmt::Display for FoldDbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(msg) => write!(f, "Configuration error: {msg}"),
        }
    }
}

impl std::error::Error for FoldDbError {}

pub type FoldDbResult<T> = Result<T, FoldDbError>;

impl DeclarativeSchemaDefinition {
    pub fn validate_field_name(name: &str) -> Result<(), String> {
        if matches!(name, "\u{1f}record" | "__record") {
            return Err(format!(
                "field name {name:?} is reserved and is not supported"
            ));
        }
        if name.is_empty()
            || name
                .chars()
                .any(|c| c.is_whitespace() || c.is_control() || matches!(c, ',' | ':'))
        {
            return Err(format!(
                "field name {name:?} must be nonempty and cannot contain whitespace, control characters, commas, or colons"
            ));
        }
        Ok(())
    }
}
