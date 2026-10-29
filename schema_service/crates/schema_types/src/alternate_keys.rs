//! Named alternate keys belong to one schema; field identities remain independent.

use crate::declarative_schemas::DeclarativeSchemaDefinition;
use crate::molecule_range_profile::MoleculeRangeProfile;
use crate::schema_type::DeclarativeSchemaType;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const MAX_ALTERNATE_KEYS: usize = 16;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AlternateKey {
    pub hash_field: String,
    pub range_field: Option<String>,
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
        std::iter::once(self.hash_field.as_str()).chain(self.range_field.as_deref())
    }
}

impl DeclarativeSchemaDefinition {
    /// A declaration names local fields, never another schema or its records.
    pub fn validate_alternate_keys(&self) -> Result<(), String> {
        if self.alternate_keys.is_empty() {
            return Ok(());
        }
        if self.alternate_keys.len() > MAX_ALTERNATE_KEYS {
            return Err("a schema permits at most 16 alternate keys".into());
        }
        if self.schema_type != DeclarativeSchemaType::Hash
            || self.field_mappers.as_ref().is_some_and(|m| !m.is_empty())
            || self
                .transform_fields
                .as_ref()
                .is_some_and(|m| !m.is_empty())
        {
            return Err(
                "alternate keys require an ordinary Hash schema without field mappers".into(),
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
        for (name, key) in &self.alternate_keys {
            Self::validate_field_name(name)?;
            if name == "primary" {
                return Err("primary is reserved as an alternate key name".into());
            }
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
        let bytes =
            serde_json::to_vec(&self.alternate_keys).expect("alternate key declarations serialize");
        format!("{:x}", Sha256::digest(bytes))
    }
}
