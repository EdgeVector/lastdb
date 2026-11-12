//! Strongly typed value types and temporal validation for schema fields.

use chrono::{DateTime, Datelike, NaiveDate, Timelike};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

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

impl std::fmt::Display for FieldValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
    let err = || "expected Date in YYYY-MM-DD form with a valid calendar date".to_string();
    date_shape(text.as_bytes())
        .then_some(())
        .ok_or_else(err)
        .and_then(|()| {
            NaiveDate::parse_from_str(text, "%Y-%m-%d")
                .map(|date| date.num_days_from_ce())
                .map_err(|_| err())
        })
}

pub(crate) fn datetime_nanos(text: &str) -> Result<i128, String> {
    let err = || {
        "expected DateTime in RFC 3339 form with an offset, years 0000..9999, \
         at most nine fractional digits, and no leap second"
            .to_string()
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

fn date_shape(bytes: &[u8]) -> bool {
    bytes.len() == 10
        && bytes.iter().enumerate().all(|(i, b)| {
            if matches!(i, 4 | 7) {
                *b == b'-'
            } else {
                b.is_ascii_digit()
            }
        })
}

fn datetime_shape(bytes: &[u8]) -> bool {
    if !(20..=35).contains(&bytes.len())
        || !date_shape(&bytes[..10])
        || !matches!(bytes[10], b'T' | b't')
        || bytes[13] != b':'
        || bytes[16] != b':'
        || ![11, 12, 14, 15, 17, 18]
            .iter()
            .all(|&i| bytes[i].is_ascii_digit())
    {
        return false;
    }
    let mut offset = &bytes[19..];
    if offset.first() == Some(&b'.') {
        let digits = offset[1..]
            .iter()
            .take_while(|b| b.is_ascii_digit())
            .count();
        if !(1..=9).contains(&digits) {
            return false;
        }
        offset = &offset[1 + digits..];
    }
    matches!(offset, [b'Z' | b'z'])
        || (offset.len() == 6
            && matches!(offset[0], b'+' | b'-')
            && offset[3] == b':'
            && [1, 2, 4, 5].iter().all(|&i| offset[i].is_ascii_digit()))
}
