//! Record key addresses, named alternate keys, and key-field validation.

use crate::{DeclarativeSchemaDefinition, DeclarativeSchemaType, FieldValueType};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

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

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlternateKey {
    #[serde(flatten)]
    pub fields: BTreeMap<String, serde_json::Value>,
}

impl DeclarativeSchemaDefinition {
    pub fn validate_owned_schema(&self) -> Result<(), String> {
        let keys = self
            .record_keys
            .as_ref()
            .ok_or("owned schemas require explicit record_keys")?;
        keys.validate()?;
        let kind = match keys.primary.kind {
            record_keys::RecordKeyKind::Hash => DeclarativeSchemaType::Hash,
            record_keys::RecordKeyKind::HashRange => DeclarativeSchemaType::HashRange,
        };
        (self.schema_type == kind && self.key.is_none() && self.alternate_keys.is_empty())
            .then_some(())
            .ok_or_else(|| {
                "record_keys require one owned Hash/HashRange layout without field-derived keys"
                    .into()
            })
    }

    #[must_use]
    pub fn alternate_key_revision(&self) -> String {
        let bytes = match &self.record_keys {
            Some(keys) => serde_json::to_vec(keys).expect("record key declarations serialize"),
            None => serde_json::to_vec(&(true, &self.alternate_keys))
                .expect("alternate key declarations serialize"),
        };
        format!("{:x}", Sha256::digest(bytes))
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
