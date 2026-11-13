//! Record key addresses, named alternate keys, and key-field validation.

use crate::field_range_profile::FieldRangeProfile;
use crate::{DeclarativeSchemaDefinition, DeclarativeSchemaType, FieldValueType};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const MAX_ALTERNATE_KEYS: usize = 16;

pub(crate) fn is_false(value: &bool) -> bool {
    !*value
}

pub mod record_keys {
    use super::{is_false, MAX_ALTERNATE_KEYS};
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
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RangePart {
    pub field: String,
    #[serde(default)]
    pub format: PartFormat,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum PartFormat {
    #[default]
    Text,
    PaddedText {
        width: u8,
    },
    ReverseTimeMillis,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlternateKey {
    #[serde(default)]
    pub hash_field: String,
    pub range_field: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash_literal: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub range_parts: Vec<RangePart>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_separator: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub omit_empty_hash: bool,
    #[serde(default)]
    pub missing: MissingAlternateKey,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissingAlternateKey {
    #[default]
    Reject,
    Omit,
}

impl AlternateKey {
    fn fields(&self) -> impl Iterator<Item = &str> {
        (!self.hash_field.is_empty())
            .then_some(self.hash_field.as_str())
            .into_iter()
            .chain(self.range_field.as_deref())
            .chain(self.range_parts.iter().map(|p| p.field.as_str()))
    }
}

fn validate_parts(key: &AlternateKey, schema: &DeclarativeSchemaDefinition) -> Result<(), String> {
    if key.hash_field.is_empty() == key.hash_literal.is_none() {
        return Err("an alternate key needs exactly one hash field or literal".into());
    }
    if key.omit_empty_hash
        && (key.missing != MissingAlternateKey::Omit
            || key.hash_literal.is_some()
            || schema.key_field_type(&key.hash_field) != FieldValueType::String)
    {
        return Err("omit_empty_hash requires an optional text hash field".into());
    }
    if key
        .hash_literal
        .as_ref()
        .is_some_and(|v| !(1..=1024).contains(&v.len()))
    {
        return Err("an alternate hash literal must contain 1 to 1024 bytes".into());
    }
    if key.range_parts.is_empty() {
        return key
            .range_separator
            .is_none()
            .then_some(())
            .ok_or_else(|| "a range separator requires compound range parts".into());
    }
    if key.range_field.is_some() || !(2..=8).contains(&key.range_parts.len()) {
        return Err("a compound range needs 2 to 8 parts and no range field".into());
    }
    match key.range_separator.as_deref() {
        Some(sep) if sep.len() == 1 && sep.as_bytes()[0].is_ascii_punctuation() => {}
        None => return Err("a compound range needs a separator".into()),
        Some(_) => return Err("a compound separator must be one ASCII punctuation byte".into()),
    }
    for part in &key.range_parts {
        let kind = schema.key_field_type(&part.field);
        let valid = match part.format {
            PartFormat::Text => kind == FieldValueType::String,
            PartFormat::PaddedText { width } => {
                (1..=64).contains(&width) && kind == FieldValueType::String
            }
            PartFormat::ReverseTimeMillis => {
                matches!(kind, FieldValueType::String | FieldValueType::DateTime)
            }
        };
        if !valid {
            return Err("compound range format does not match its field type".into());
        }
    }
    Ok(())
}

impl DeclarativeSchemaDefinition {
    pub fn validate_owned_schema(&self) -> Result<(), String> {
        if let Some(keys) = &self.record_keys {
            keys.validate()?;
            let kind = match keys.primary.kind {
                record_keys::RecordKeyKind::Hash => DeclarativeSchemaType::Hash,
                record_keys::RecordKeyKind::HashRange => DeclarativeSchemaType::HashRange,
            };
            return (self.schema_type == kind
                && self.key.is_none()
                && self.alternate_keys.is_empty())
            .then_some(())
            .ok_or_else(|| {
                "record_keys require one owned Hash/HashRange layout without field-derived keys"
                    .into()
            });
        }
        if self.alternate_keys.len() > MAX_ALTERNATE_KEYS {
            return Err("a schema permits at most 16 alternate keys".into());
        }
        if !matches!(
            self.schema_type,
            DeclarativeSchemaType::Hash | DeclarativeSchemaType::HashRange
        ) {
            return Err("an owned schema must be a Hash or HashRange schema".into());
        }
        let fields = self.fields.as_deref().unwrap_or_default();
        let primary = self.key.as_ref().and_then(|k| k.hash_field.as_deref());
        if primary.is_none()
            && self.alternate_keys.is_empty()
            && self.schema_type == DeclarativeSchemaType::Hash
            && self.key.as_ref().is_none_or(|k| k.range_field.is_none())
        {
            return (fields.len() == 1).then_some(()).ok_or_else(|| {
                "an owned Hash without a primary hash field must declare exactly one field".into()
            });
        }
        let primary = primary.ok_or("alternate keys require a declared primary hash field")?;
        if !fields.iter().any(|name| name == primary) {
            return Err("the primary key must name a declared field".into());
        }
        if self.schema_type == DeclarativeSchemaType::HashRange && !self.alternate_keys.is_empty() {
            return Err("alternate keys currently require a Hash primary key".into());
        }
        for (name, key) in &self.alternate_keys {
            Self::validate_field_name(name)?;
            if name == "primary" {
                return Err("primary is reserved as an alternate key name".into());
            }
            validate_parts(key, self)?;
            let exact_primary = key.hash_field == primary
                || key.range_field.as_deref() == Some(primary)
                || key
                    .range_parts
                    .iter()
                    .any(|p| p.field == primary && p.format == PartFormat::Text);
            if !exact_primary {
                return Err(
                    "each alternate key must retain the exact primary key field without formatting"
                        .into(),
                );
            }
            for field in key.fields() {
                if !fields.iter().any(|f| f == field) {
                    return Err("an alternate key names an undeclared field".into());
                }
                FieldRangeProfile::for_field_type(Some(&self.key_field_type(field)))?;
            }
        }
        Ok(())
    }

    pub fn alternate_key_revision(&self) -> String {
        let bytes = match &self.record_keys {
            Some(keys) => serde_json::to_vec(keys).expect("record key declarations serialize"),
            None => serde_json::to_vec(&(true, &self.alternate_keys))
                .expect("alternate key declarations serialize"),
        };
        format!("{:x}", Sha256::digest(bytes))
    }

    #[must_use]
    pub(crate) fn key_field_type(&self, path: &str) -> FieldValueType {
        self.field_types
            .get(path)
            .cloned()
            .or_else(|| {
                let (root, child) = path.split_once('.')?;
                nested_type(self.field_types.get(root)?, child)
            })
            .unwrap_or(FieldValueType::Any)
    }

    pub fn validate_key_field_types(&self) -> Result<(), String> {
        for path in self.key.iter().filter_map(|k| k.hash_field.as_deref()) {
            let temporal = if let Some(ty) = self.field_types.get(path) {
                temporal_at_path(ty, None)
            } else if let Some((root, child)) = path.split_once('.') {
                self.field_types
                    .get(root)
                    .is_some_and(|ty| temporal_at_path(ty, Some(child)))
            } else {
                false
            };
            if temporal {
                return Err(format!(
                    "key field '{path}' uses Date or DateTime outside a typed range contract"
                ));
            }
        }
        Ok(())
    }
}

fn nested_type(field_type: &FieldValueType, path: &str) -> Option<FieldValueType> {
    match field_type {
        FieldValueType::Object(fields) => Some(
            fields
                .get(path)
                .cloned()
                .or_else(|| {
                    let (root, child) = path.split_once('.')?;
                    nested_type(fields.get(root)?, child)
                })
                .unwrap_or(FieldValueType::Any),
        ),
        FieldValueType::Map(v) => Some((**v).clone()),
        FieldValueType::Any => Some(FieldValueType::Any),
        FieldValueType::OneOf(variants) => {
            let mut types: Vec<_> = variants
                .iter()
                .filter_map(|ty| nested_type(ty, path))
                .collect();
            match types.len() {
                0 => None,
                1 => types.pop(),
                _ => Some(FieldValueType::OneOf(types)),
            }
        }
        _ => None,
    }
}

fn temporal_at_path(field_type: &FieldValueType, child: Option<&str>) -> bool {
    match (field_type, child) {
        (FieldValueType::Date | FieldValueType::DateTime, None) => true,
        (FieldValueType::OneOf(variants), _) => variants.iter().any(|v| temporal_at_path(v, child)),
        (FieldValueType::Object(fields), Some(path)) => match fields.get(path) {
            Some(ty) => temporal_at_path(ty, None),
            None => path.split_once('.').is_some_and(|(r, sub)| {
                fields
                    .get(r)
                    .is_some_and(|ty| temporal_at_path(ty, Some(sub)))
            }),
        },
        (FieldValueType::Map(v), Some(path)) => {
            temporal_at_path(v, None)
                || path
                    .split_once('.')
                    .is_some_and(|(_, sub)| temporal_at_path(v, Some(sub)))
        }
        _ => false,
    }
}
