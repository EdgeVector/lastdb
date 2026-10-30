//! Named alternate keys belong to one schema; field identities remain independent.

use crate::declarative_schemas::DeclarativeSchemaDefinition;
use crate::molecule_range_profile::MoleculeRangeProfile;
use crate::schema_type::DeclarativeSchemaType;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const MAX_ALTERNATE_KEYS: usize = 16;

pub(crate) fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AlternateKey {
    #[serde(default)]
    pub hash_field: String,
    pub range_field: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash_literal: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub range_parts: Vec<crate::alternate_parts::RangePart>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_separator: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub omit_empty_hash: bool,
    #[serde(default)]
    pub missing: MissingAlternateKey,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum MissingAlternateKey {
    #[default]
    Reject,
    Omit,
}

impl AlternateKey {
    pub fn fields(&self) -> impl Iterator<Item = &str> {
        (!self.hash_field.is_empty())
            .then_some(self.hash_field.as_str())
            .into_iter()
            .chain(self.range_field.as_deref())
            .chain(self.range_parts.iter().map(|part| part.field.as_str()))
    }

    pub fn range_type(
        &self,
        schema: &DeclarativeSchemaDefinition,
    ) -> Option<crate::FieldValueType> {
        if self.range_parts.is_empty() {
            self.range_field
                .as_deref()
                .map(|field| schema.key_field_type(field))
        } else {
            Some(crate::FieldValueType::String)
        }
    }
}

impl DeclarativeSchemaDefinition {
    pub fn uses_independent_fields(&self) -> bool {
        self.independent_fields || !self.alternate_keys.is_empty()
    }

    /// A declaration names local fields, never another schema or its records.
    pub fn validate_alternate_keys(&self) -> Result<(), String> {
        if !self.uses_independent_fields() {
            return Ok(());
        }
        if self.alternate_keys.len() > MAX_ALTERNATE_KEYS {
            return Err("a schema permits at most 16 alternate keys".into());
        }
        if !matches!(
            self.schema_type,
            DeclarativeSchemaType::Hash | DeclarativeSchemaType::HashRange
        ) || self.field_mappers.as_ref().is_some_and(|m| !m.is_empty())
            || self
                .transform_fields
                .as_ref()
                .is_some_and(|m| !m.is_empty())
        {
            return Err(
                "independent values require a Hash or HashRange schema without field mappers"
                    .into(),
            );
        }
        let primary = self
            .key
            .as_ref()
            .and_then(|k| k.hash_field.as_deref())
            .ok_or("alternate keys require a declared primary hash field")?;
        let fields = self.fields.as_deref().unwrap_or_default();
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
            crate::alternate_parts::validate(key, self)?;
            if !key.fields().any(|field| field == primary) {
                return Err("each alternate key must include the primary key field".into());
            }
            for field in key.fields() {
                if !fields.iter().any(|name| name == field) {
                    return Err("an alternate key names an undeclared field".into());
                }
                MoleculeRangeProfile::for_field_type(Some(&self.key_field_type(field)))?;
            }
        }
        Ok(())
    }

    /// Separate from schema identity: a layout change cannot remint record identity.
    pub fn alternate_key_revision(&self) -> String {
        let bytes = serde_json::to_vec(&(self.uses_independent_fields(), &self.alternate_keys))
            .expect("alternate key declarations serialize");
        format!("{:x}", Sha256::digest(bytes))
    }
}
