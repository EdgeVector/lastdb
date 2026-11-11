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
            Self::Null => value
                .is_null()
                .then_some(())
                .ok_or_else(|| type_err("Null", value)),
            Self::String => value
                .is_string()
                .then_some(())
                .ok_or_else(|| type_err("String", value)),
            Self::Boolean => value
                .is_boolean()
                .then_some(())
                .ok_or_else(|| type_err("Boolean", value)),
            Self::Integer => (value.is_number() && value.as_i64().is_some())
                .then_some(())
                .ok_or_else(|| type_err("Integer", value)),
            Self::Float => (value.is_number() && value.as_f64().is_some())
                .then_some(())
                .ok_or_else(|| type_err("Float", value)),
            Self::Number => value
                .is_number()
                .then_some(())
                .ok_or_else(|| type_err("Number", value)),
            Self::Array(element_type) => {
                let arr = value
                    .as_array()
                    .ok_or_else(|| format!("expected Array, got {}", json_type_name(value)))?;
                for (i, elem) in arr.iter().enumerate() {
                    element_type
                        .validate(elem)
                        .map_err(|e| format!("Array[{i}]: {e}"))?;
                }
                Ok(())
            }
            Self::Map(value_type) => {
                let obj = value
                    .as_object()
                    .ok_or_else(|| format!("expected Map, got {}", json_type_name(value)))?;
                for (k, v) in obj {
                    value_type
                        .validate(v)
                        .map_err(|e| format!("Map[\"{k}\"]: {e}"))?;
                }
                Ok(())
            }
            Self::Object(field_types) => {
                let obj = value
                    .as_object()
                    .ok_or_else(|| format!("expected Object, got {}", json_type_name(value)))?;
                for (field_name, field_type) in field_types {
                    let field_value = obj.get(field_name).unwrap_or(&Value::Null);
                    field_type
                        .validate(field_value)
                        .map_err(|e| format!(".{field_name}: {e}"))?;
                }
                Ok(())
            }
            Self::OneOf(variants) => {
                for variant in variants {
                    if variant.validate(value).is_ok() {
                        return Ok(());
                    }
                }
                Err(format!(
                    "value does not match any variant of OneOf({self}), got {}",
                    json_type_name(value)
                ))
            }
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
            Self::String => write!(f, "String"),
            Self::Date => write!(f, "Date"),
            Self::DateTime => write!(f, "DateTime"),
            Self::Integer => write!(f, "Integer"),
            Self::Float => write!(f, "Float"),
            Self::Number => write!(f, "Number"),
            Self::Boolean => write!(f, "Boolean"),
            Self::Null => write!(f, "Null"),
            Self::Array(t) => write!(f, "Array<{t}>"),
            Self::Map(v) => write!(f, "Map<String, {v}>"),
            Self::Object(fields) => {
                write!(f, "{{")?;
                for (i, (k, v)) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{k}: {v}")?;
                }
                write!(f, "}}")
            }
            Self::OneOf(variants) => {
                for (i, v) in variants.iter().enumerate() {
                    if i > 0 {
                        write!(f, " | ")?;
                    }
                    write!(f, "{v}")?;
                }
                Ok(())
            }
            Self::Any => write!(f, "Any"),
        }
    }
}

pub(crate) fn date_day(text: &str) -> Result<i32, String> {
    if !date_shape(text.as_bytes()) {
        return Err("expected Date in YYYY-MM-DD form with a valid calendar date".into());
    }
    NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .map(|date| date.num_days_from_ce())
        .map_err(|_| "expected Date in YYYY-MM-DD form with a valid calendar date".into())
}

pub(crate) fn datetime_nanos(text: &str) -> Result<i128, String> {
    let invalid = || {
        "expected DateTime in RFC 3339 form with an offset, years 0000..9999, \
         at most nine fractional digits, and no leap second"
            .to_string()
    };
    if !datetime_shape(text.as_bytes()) {
        return Err(invalid());
    }
    let instant = DateTime::parse_from_rfc3339(text).map_err(|_| invalid())?;
    if instant.nanosecond() >= 1_000_000_000 || !(0..=9999).contains(&instant.to_utc().year()) {
        return Err(invalid());
    }
    Ok(i128::from(instant.timestamp()) * 1_000_000_000 + i128::from(instant.nanosecond()))
}

fn date_shape(bytes: &[u8]) -> bool {
    bytes.len() == 10
        && bytes.iter().enumerate().all(|(i, byte)| match i {
            4 | 7 => *byte == b'-',
            _ => byte.is_ascii_digit(),
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
            .all(|i| bytes[*i].is_ascii_digit())
    {
        return false;
    }
    let mut offset = &bytes[19..];
    if offset.first() == Some(&b'.') {
        let digits = offset[1..]
            .iter()
            .take_while(|byte| byte.is_ascii_digit())
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
            && [1, 2, 4, 5].iter().all(|i| offset[*i].is_ascii_digit()))
}
