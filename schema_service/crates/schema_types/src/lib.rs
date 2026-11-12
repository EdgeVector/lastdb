//! Pure wire/domain schema types shared across Schema Service and LastDB.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;

pub mod alternate_keys;
pub mod declarative_schemas;
pub mod field_value_type;
pub mod range_storage_profile;

pub mod field_range_profile {
    pub use crate::range_storage_profile::FieldRangeProfile;
}

/// Explicit record addresses independent of payload field definitions.
pub mod record_keys {
    use crate::alternate_keys::MAX_ALTERNATE_KEYS;
    use crate::field_range_profile::FieldRangeProfile;
    use crate::{DeclarativeSchemaDefinition, FieldValueType};
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeMap;

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum RecordKeyKind {
        Hash,
        HashRange,
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct RecordKeyDefinition {
        pub kind: RecordKeyKind,
        #[serde(default, skip_serializing_if = "crate::alternate_keys::is_false")]
        pub allow_absent: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub range_type: Option<FieldValueType>,
    }

    impl RecordKeyDefinition {
        pub fn range_profile(&self) -> Result<FieldRangeProfile, String> {
            match (self.kind, &self.range_type) {
                (RecordKeyKind::Hash, None) | (RecordKeyKind::HashRange, Some(_)) => {
                    FieldRangeProfile::for_field_type(self.range_type.as_ref())
                }
                _ => Err("hash keys have no range type; hash_range keys require one".into()),
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
            match (self.kind, range) {
                (RecordKeyKind::Hash, None) => (),
                (RecordKeyKind::HashRange, Some(v)) if !v.is_empty() => (),
                _ => return Err("record address differs from the declared key shape".into()),
            }
            self.range_profile()?
                .encode_value(range.unwrap_or_default())?;
            Ok(())
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
    }
}

/// Field semantics and typed scalar range comparison.
pub mod key_comparison {
    use crate::field_value_type::{date_day, datetime_nanos};
    use crate::FieldValueType;
    use serde::Serialize;
    use std::cmp::Ordering;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub struct RangeKeyComparison {
        pub field_type: FieldValueType,
    }

    impl RangeKeyComparison {
        pub fn typed(field_type: FieldValueType) -> Result<Self, String> {
            ordered_type(&field_type)?;
            Ok(Self { field_type })
        }

        pub fn compare(&self, left: &str, right: &str) -> Result<Ordering, String> {
            self.canonical_value(left)?
                .compare(self.canonical_value(right)?)
        }

        pub fn validate_value(&self, text: &str) -> Result<(), String> {
            self.canonical_value(text).map(|_| ())
        }

        pub fn canonical_value<'a>(&self, text: &'a str) -> Result<CanonicalRange<'a>, String> {
            match ordered_type(&self.field_type)? {
                FieldValueType::String => Ok(CanonicalRange::Text(text)),
                FieldValueType::Integer => text
                    .parse::<i64>()
                    .map(CanonicalRange::Integer)
                    .map_err(|_| "expected a signed 64-bit range key".into()),
                FieldValueType::Date => date_day(text).map(CanonicalRange::Date),
                FieldValueType::DateTime => datetime_nanos(text).map(CanonicalRange::DateTime),
                FieldValueType::Boolean => text
                    .parse::<bool>()
                    .map(CanonicalRange::Boolean)
                    .map_err(|_| "expected a Boolean range key".into()),
                _ => Err("this field type has no approved typed range comparison".into()),
            }
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
            let first = ordered_type(non_null.next().ok_or("a required key cannot be null")?)?;
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
}

pub use alternate_keys::{AlternateKey, MissingAlternateKey};
pub use declarative_schemas::{
    refuses_identity_downgrade, DeclarativeSchemaDefinition, IdentityRecompute, OwnedDeclaration,
    PortableSchemaId, IDENTITY_HASH_ALGO_VERSION,
};
pub use field_value_type::FieldValueType;

pub type Schema = DeclarativeSchemaDefinition;
pub type SchemaType = DeclarativeSchemaType;

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

#[must_use]
pub fn compute_field_hash(
    name: &str,
    description: &str,
    field_type: &FieldValueType,
    version: u32,
) -> String {
    let type_json = serde_json::to_string(field_type).unwrap_or_else(|_| "\"Any\"".into());
    let quote = |s: &str| serde_json::to_string(s.trim()).unwrap_or_else(|_| "\"\"".into());
    let payload = format!(
        r#"{{"description":{},"name":{},"type":{type_json},"version":{version}}}"#,
        quote(description),
        quote(name),
    );
    app_identity_crypto::hex_lower(&Sha256::digest(payload.as_bytes()))
}
