//! Content-addressed typed definitions for record replay.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fmt, str::FromStr};

use super::DeclarativeSchemaDefinition;

/// Immutable service identity of a complete portable definition.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct PortableSchemaId(String);

impl PortableSchemaId {
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
        if !is_digest(&value) {
            return Err("portable schema ID must be a lowercase SHA-256".into());
        }
        Ok(Self(value))
    }
}

/// A published catalog definition pinned under an immutable ID. The catalog
/// ID names the existing schema slot. The portable ID also binds field types,
/// field declarations, and the rest of the canonical wire definition.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableSchemaDefinition {
    id: PortableSchemaId,
    catalog_id: String,
    definition: DeclarativeSchemaDefinition,
}

impl PortableSchemaDefinition {
    /// Pin an already published schema under its immutable replay identity.
    /// The caller supplies the exact catalog key it used to load the schema.
    pub fn from_catalog(
        catalog_id: &str,
        mut definition: DeclarativeSchemaDefinition,
    ) -> Result<Self, String> {
        Self::validate_catalog_id(catalog_id)?;
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
        Ok(Self {
            id: definition_id(&definition)?,
            catalog_id: catalog_id.to_owned(),
            definition,
        })
    }

    pub fn validate_catalog_id(id: &str) -> Result<(), String> {
        if !is_digest(id) {
            return Err("schema catalog ID must be a lowercase SHA-256".into());
        }
        Ok(())
    }

    #[must_use]
    pub fn id(&self) -> &PortableSchemaId {
        &self.id
    }

    #[must_use]
    pub fn catalog_id(&self) -> &str {
        &self.catalog_id
    }

    /// Authenticate the typed definition against a position in the replay
    /// log or a staged checkpoint. A mismatched copy cannot advance replay.
    pub fn verify(
        &self,
        expected: &PortableSchemaId,
    ) -> Result<&DeclarativeSchemaDefinition, String> {
        if &self.id != expected {
            return Err("portable schema definition has the wrong ID".into());
        }
        let verified = Self::from_catalog(&self.catalog_id, self.definition.clone())?;
        if verified.id != self.id || verified.definition != self.definition {
            return Err("portable schema ID does not match its typed definition".into());
        }
        Ok(&self.definition)
    }

    /// Bind the verified service revision to an installed schema by the
    /// entire definition. Only its local runtime name may differ.
    pub fn bind_local_alias(
        &self,
        expected: &PortableSchemaId,
        local: &DeclarativeSchemaDefinition,
    ) -> Result<(), String> {
        let definition = self.verify(expected)?;
        if local.name.is_empty() {
            return Err("portable schema requires an explicit local runtime name".into());
        }
        let mut candidate = local.clone();
        candidate.name.clone_from(&definition.name);
        let admitted = Self::from_catalog(&self.catalog_id, candidate)?;
        if admitted.id != self.id {
            return Err("local schema differs from the pinned typed definition".into());
        }
        Ok(())
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
    Ok(PortableSchemaId(crate::hex::hex_lower(&digest.finalize())))
}
