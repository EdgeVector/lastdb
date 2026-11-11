//! Content-addressed typed schema definition IDs for record and contribution binding.

use super::DeclarativeSchemaDefinition;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fmt, str::FromStr};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct PortableSchemaId(String);

impl PortableSchemaId {
    pub fn from_catalog(
        catalog_id: &str,
        mut definition: DeclarativeSchemaDefinition,
    ) -> Result<Self, String> {
        if !is_digest(catalog_id) {
            return Err("schema catalog ID must be a lowercase SHA-256".into());
        }
        if definition.identity_hash.as_deref() != Some(catalog_id) {
            return Err("definition lacks the exact supported catalog identity".into());
        }
        if definition.record_keys.is_none() {
            return Err("portable schemas require explicit record keys".into());
        }
        definition.validate_definition()?;
        definition.require_typed_fields()?;
        let identity_version = definition.identity_hash_algo_version;
        definition.compute_identity_hash();
        if definition.identity_hash.as_deref() != Some(catalog_id)
            || definition.identity_hash_algo_version != identity_version
        {
            return Err("schema catalog identity does not match its declaration".into());
        }
        definition.normalize_wire();
        definition.ensure_field_hashes();
        definition_id(&definition)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PortableSchemaId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for PortableSchemaId {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_from(value.to_owned())
    }
}

impl TryFrom<String> for PortableSchemaId {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if is_digest(&value) {
            Ok(Self(value))
        } else {
            Err("portable schema ID must be a lowercase SHA-256".into())
        }
    }
}

fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn definition_id(definition: &DeclarativeSchemaDefinition) -> Result<PortableSchemaId, String> {
    let value = serde_json::to_value(definition)
        .map_err(|_| "cannot encode portable schema definition".to_string())?;
    let bytes = app_identity_crypto::canonicalize(&value)
        .map_err(|_| "cannot canonicalize portable schema definition".to_string())?;
    let mut digest = Sha256::new();
    digest.update(b"lastdb:portable-schema-definition:v1\0");
    digest.update(bytes);
    Ok(PortableSchemaId(app_identity_crypto::hex_lower(
        &digest.finalize(),
    )))
}
