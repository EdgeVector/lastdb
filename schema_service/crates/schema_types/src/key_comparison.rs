//! Field semantics and physical key order are separate, explicit contracts.
//!
//! Existing plain and OPE-v1 files both use LegacyUtf8V1. No typed storage
//! profile is enabled here; a different order requires explicit conversion.

use crate::{field_value_type::temporal, FieldValueType};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyOrderVersion {
    LegacyUtf8V1,
}

/// Captured once per schema read, never stored on each molecule key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RangeKeyComparison {
    pub field_type: FieldValueType,
    pub order: KeyOrderVersion,
}

impl RangeKeyComparison {
    #[must_use]
    pub fn legacy(field_type: FieldValueType) -> Self {
        Self {
            field_type,
            order: KeyOrderVersion::LegacyUtf8V1,
        }
    }

    /// Compare API coordinates using the order promised by their disk format.
    /// A declared Integer alone must not reinterpret existing UTF-8 files.
    #[must_use]
    pub fn compare(&self, left: &str, right: &str) -> Ordering {
        match self.order {
            KeyOrderVersion::LegacyUtf8V1 => left.cmp(right),
        }
    }

    /// Semantic comparison for a future reviewed typed storage profile.
    /// Invalid coordinates and ambiguous domains are refused, never coerced.
    pub fn compare_typed(&self, left: &str, right: &str) -> Result<Ordering, String> {
        self.canonical_value(left)?
            .compare(self.canonical_value(right)?)
    }

    pub fn canonical_value<'a>(&self, text: &'a str) -> Result<CanonicalRange<'a>, String> {
        parse(ordered_type(&self.field_type)?, text)
    }

    pub fn require_typed_prefix(&self) -> Result<(), String> {
        if *ordered_type(&self.field_type)? != FieldValueType::String {
            return Err("a text prefix requires a String range field".into());
        }
        Ok(())
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
