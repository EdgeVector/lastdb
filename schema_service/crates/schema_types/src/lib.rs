//! Pure wire/domain schema types, record key declarations, and field scalar validation.

use chrono::{DateTime, Datelike, NaiveDate, Timelike};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, fmt};

mod declarative_schemas;
mod manifest_schema;
mod range_profile;

pub use declarative_schemas::DeclarativeSchemaDefinition;
pub use manifest_schema::expand_manifest_schema;
pub use range_profile::{
    compute_field_hash, field_range_profile, key_comparison, PortableSchemaId,
};

pub const IDENTITY_HASH_ALGO_VERSION: u32 = 4;

pub(crate) fn is_false(value: &bool) -> bool {
    !*value
}

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
    stored_version
        .filter(|&v| stored_hash.is_some_and(|h| !h.is_empty()) && v > IDENTITY_HASH_ALGO_VERSION)
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
        let chk = |ok: bool, exp: &str| ok.then_some(()).ok_or_else(|| type_err(exp, value));
        match self {
            Self::Any => Ok(()),
            Self::Date => {
                date_day(value.as_str().ok_or_else(|| type_err("Date", value))?).map(|_| ())
            }
            Self::DateTime => {
                datetime_nanos(value.as_str().ok_or_else(|| type_err("DateTime", value))?)
                    .map(|_| ())
            }
            Self::Null => chk(value.is_null(), "Null"),
            Self::String => chk(value.is_string(), "String"),
            Self::Boolean => chk(value.is_boolean(), "Boolean"),
            Self::Integer => chk(value.as_i64().is_some(), "Integer"),
            Self::Float => chk(value.as_f64().is_some(), "Float"),
            Self::Number => chk(value.is_number(), "Number"),
            Self::Array(elem) => {
                for (i, v) in value
                    .as_array()
                    .ok_or_else(|| type_err("Array", value))?
                    .iter()
                    .enumerate()
                {
                    elem.validate(v).map_err(|e| format!("Array[{i}]: {e}"))?;
                }
                Ok(())
            }
            Self::Map(val) => {
                for (k, v) in value.as_object().ok_or_else(|| type_err("Map", value))? {
                    val.validate(v).map_err(|e| format!("Map[\"{k}\"]: {e}"))?;
                }
                Ok(())
            }
            Self::Object(fields) => {
                let obj = value.as_object().ok_or_else(|| type_err("Object", value))?;
                for (name, ty) in fields {
                    ty.validate(obj.get(name).unwrap_or(&Value::Null))
                        .map_err(|e| format!(".{name}: {e}"))?;
                }
                Ok(())
            }
            Self::OneOf(variants) => {
                if variants.iter().any(|v| v.validate(value).is_ok()) {
                    Ok(())
                } else {
                    Err(format!(
                        "value does not match any variant of OneOf({self}), got {}",
                        json_type_name(value)
                    ))
                }
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
        && b.iter().enumerate().all(|(i, &c)| {
            (matches!(i, 4 | 7) && c == b'-') || (!matches!(i, 4 | 7) && c.is_ascii_digit())
        })
}

fn datetime_shape(b: &[u8]) -> bool {
    if !(20..=35).contains(&b.len())
        || !date_shape(&b[..10])
        || !matches!(b[10], b'T' | b't')
        || b[13] != b':'
        || b[16] != b':'
        || !b[11..19]
            .iter()
            .enumerate()
            .all(|(i, c)| matches!(i, 2 | 5) || c.is_ascii_digit())
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

pub mod record_keys {
    use crate::field_range_profile::FieldRangeProfile;
    use crate::{is_false, DeclarativeSchemaDefinition, DeclarativeSchemaType, FieldValueType};
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeMap;

    const MAX_ALTERNATE_KEYS: usize = 16;

    /// The refusal for a schema update that changes or removes a declared key.
    pub const KEYS_IMMUTABLE: &str = "explicit primary/alternate key declarations are immutable; declare a new schema and copy explicitly";

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum RecordKeyKind {
        Hash,
        HashRange,
    }

    impl RecordKeyKind {
        pub(crate) const fn schema_type(self) -> DeclarativeSchemaType {
            match self {
                Self::Hash => DeclarativeSchemaType::Hash,
                Self::HashRange => DeclarativeSchemaType::HashRange,
            }
        }
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct RecordKeyDefinition {
        pub kind: RecordKeyKind,
        #[serde(default, skip_serializing_if = "is_false")]
        pub allow_absent: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub range_type: Option<FieldValueType>,
    }

    impl RecordKeyDefinition {
        pub fn range_profile(&self) -> Result<FieldRangeProfile, String> {
            if matches!(
                (self.kind, &self.range_type),
                (RecordKeyKind::Hash, None) | (RecordKeyKind::HashRange, Some(_))
            ) {
                FieldRangeProfile::for_field_type(self.range_type.as_ref())
            } else {
                Err("hash keys have no range type; hash_range keys require one".into())
            }
        }

        pub fn validate_coordinates(
            &self,
            hash: Option<&str>,
            range: Option<&str>,
        ) -> Result<(), String> {
            if hash.is_none_or(str::is_empty) {
                return Err("a record address requires a nonempty hash".into());
            }
            if !matches!(
                (self.kind, range),
                (RecordKeyKind::Hash, None) | (RecordKeyKind::HashRange, Some(_))
            ) || range.is_some_and(str::is_empty)
            {
                return Err("record address differs from the declared key shape".into());
            }
            self.range_profile()?
                .encode_value(range.unwrap_or_default())
                .map(|_| ())
        }
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct RecordKeys {
        pub primary: RecordKeyDefinition,
        pub alternates: BTreeMap<String, RecordKeyDefinition>,
    }

    impl RecordKeys {
        pub fn validate(&self) -> Result<(), String> {
            if self.primary.allow_absent {
                return Err("a primary key cannot allow an absent address".into());
            }
            self.primary.range_profile()?;
            if self.alternates.len() > MAX_ALTERNATE_KEYS {
                return Err(format!(
                    "a schema permits at most {MAX_ALTERNATE_KEYS} alternate keys"
                ));
            }
            for (name, key) in &self.alternates {
                DeclarativeSchemaDefinition::validate_field_name(name)?;
                if name == "primary" {
                    return Err("primary is reserved as an alternate key name".into());
                }
                key.range_profile()?;
            }
            Ok(())
        }

        /// Check that `next` may replace these declarations on an installed
        /// schema. The primary key and every existing alternate key stay
        /// identical. A schema without alternate keys may add them. The
        /// address of an alternate entry hashes the whole declaration (see
        /// `alternate_key_revision`), so an addition beside existing alternate
        /// keys would move their stored entries; that stays refused.
        pub fn check_update(&self, next: &Self) -> Result<(), &'static str> {
            if self.primary != next.primary
                || self
                    .alternates
                    .iter()
                    .any(|(name, key)| next.alternates.get(name) != Some(key))
            {
                return Err(KEYS_IMMUTABLE);
            }
            if !self.alternates.is_empty() && next.alternates.len() != self.alternates.len() {
                return Err("only a schema without alternate keys may add alternate keys; existing alternate entries are addressed by the whole key declaration; declare a new schema and copy explicitly");
            }
            Ok(())
        }
    }
}
