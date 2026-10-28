//! Field semantics and the typed range order of a node home.
//!
//! Every node home stores typed ranges, so API coordinates compare in the
//! scalar order of the declared range type.

use crate::{field_value_type::temporal, FieldValueType};
use serde::Serialize;
use std::cmp::Ordering;

/// Captured once per schema read, never stored on each molecule key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RangeKeyComparison {
    pub field_type: FieldValueType,
}

impl RangeKeyComparison {
    pub fn typed(field_type: FieldValueType) -> Result<Self, String> {
        ordered_type(&field_type)?;
        Ok(Self { field_type })
    }

    /// Compare API coordinates in the declared scalar order.
    /// Invalid coordinates and ambiguous domains are refused, never coerced.
    pub fn compare(&self, left: &str, right: &str) -> Result<Ordering, String> {
        self.canonical_value(left)?
            .compare(self.canonical_value(right)?)
    }

    pub fn validate_value(&self, text: &str) -> Result<(), String> {
        self.canonical_value(text).map(|_| ())
    }

    pub fn canonical_value<'a>(&self, text: &'a str) -> Result<CanonicalRange<'a>, String> {
        parse(ordered_type(&self.field_type)?, text)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CanonicalRange<'a> {
    Text(&'a str),
    Integer(i64),
    Date(i32),
    DateTime(i128),
    Boolean(bool),
}

impl CanonicalRange<'_> {
    pub fn compare(self, other: Self) -> Result<Ordering, String> {
        match (self, other) {
            (Self::Text(a), Self::Text(b)) => Ok(a.cmp(b)),
            (Self::Integer(a), Self::Integer(b)) => Ok(a.cmp(&b)),
            (Self::Date(a), Self::Date(b)) => Ok(a.cmp(&b)),
            (Self::DateTime(a), Self::DateTime(b)) => Ok(a.cmp(&b)),
            (Self::Boolean(a), Self::Boolean(b)) => Ok(a.cmp(&b)),
            _ => Err("a typed range cannot compare different scalar domains".into()),
        }
    }
}

pub(crate) fn ordered_type(field_type: &FieldValueType) -> Result<&FieldValueType, String> {
    if let FieldValueType::OneOf(variants) = field_type {
        let mut non_null = variants.iter().filter(|v| **v != FieldValueType::Null);
        let first = non_null.next().ok_or("a required key cannot be null")?;
        let first = ordered_type(first)?;
        for variant in non_null {
            if ordered_type(variant)? != first {
                return Err("a typed range requires one unambiguous scalar domain".into());
            }
        }
        return Ok(first);
    }
    match field_type {
        FieldValueType::String
        | FieldValueType::Integer
        | FieldValueType::Date
        | FieldValueType::DateTime
        | FieldValueType::Boolean => Ok(field_type),
        _ => Err("this field type has no approved typed range comparison".into()),
    }
}

fn parse<'a>(field_type: &FieldValueType, text: &'a str) -> Result<CanonicalRange<'a>, String> {
    match field_type {
        FieldValueType::String => Ok(CanonicalRange::Text(text)),
        FieldValueType::Integer => text
            .parse::<i64>()
            .map(CanonicalRange::Integer)
            .map_err(|_| "expected a signed 64-bit range key".into()),
        FieldValueType::Date => temporal::date_day(text).map(CanonicalRange::Date),
        FieldValueType::DateTime => temporal::datetime_nanos(text).map(CanonicalRange::DateTime),
        FieldValueType::Boolean => text
            .parse::<bool>()
            .map(CanonicalRange::Boolean)
            .map_err(|_| "expected a Boolean range key".into()),
        _ => Err("this field type has no approved typed range comparison".into()),
    }
}
