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
    /// The definition check: named alternate keys require the owned-schema
    /// rules.
    pub fn validate_alternate_keys(&self) -> Result<(), String> {
        if self.alternate_keys.is_empty() {
            return Ok(());
        }
        self.validate_owned_schema()
    }

    /// The rules for a schema that owns its record values. A node applies them
    /// to every schema it declares, loads, or admits at startup. A declaration
    /// names local fields, never another schema or its records.
    pub fn validate_owned_schema(&self) -> Result<(), String> {
        if self.alternate_keys.len() > MAX_ALTERNATE_KEYS {
            return Err("a schema permits at most 16 alternate keys".into());
        }
        if !matches!(
            self.schema_type,
            DeclarativeSchemaType::Hash | DeclarativeSchemaType::HashRange
        ) || self
            .transform_fields
            .as_ref()
            .is_some_and(|m| !m.is_empty())
        {
            return Err(
                "an owned schema must be a Hash or HashRange schema without transform fields"
                    .into(),
            );
        }
        let primary = self.key.as_ref().and_then(|key| key.hash_field.as_deref());
        if primary.is_none()
            && self.alternate_keys.is_empty()
            && self.schema_type == DeclarativeSchemaType::Hash
            && self
                .key
                .as_ref()
                .is_none_or(|key| key.range_field.is_none())
        {
            // A plain owned Hash can use the request's external coordinate as
            // its only key; its one field holds the value. More fields or named
            // alternate keys still need the primary value in a declared field.
            if self.fields.as_deref().unwrap_or_default().len() != 1 {
                return Err(
                    "an owned Hash without a primary hash field must declare exactly one field"
                        .into(),
                );
            }
            return Ok(());
        }
        let primary = primary.ok_or("alternate keys require a declared primary hash field")?;
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
            let exact_primary = key.hash_field == primary
                || key.range_field.as_deref() == Some(primary)
                || key.range_parts.iter().any(|part| {
                    part.field == primary && part.format == crate::alternate_parts::PartFormat::Text
                });
            if !exact_primary {
                return Err(
                    "each alternate key must retain the exact primary key field without formatting"
                        .into(),
                );
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
    ///
    /// Only a node schema has a revision, and every node schema owns its
    /// values. The leading `true` keeps the revision bytes that stored capture
    /// outcomes and alternate-key change jobs already hold.
    pub fn alternate_key_revision(&self) -> String {
        let bytes = serde_json::to_vec(&(true, &self.alternate_keys))
            .expect("alternate key declarations serialize");
        format!("{:x}", Sha256::digest(bytes))
    }
}
