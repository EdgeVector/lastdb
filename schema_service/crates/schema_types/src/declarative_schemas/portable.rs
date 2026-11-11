//! Content-addressed typed definitions and version union rules for record replay.

use super::DeclarativeSchemaDefinition;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, fmt, str::FromStr};

const INCOMPATIBLE: &str = "a later shared schema definition changes the key, the alternate keys or an existing field; declare a new schema name";

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
        if is_digest(&value) {
            Ok(Self(value))
        } else {
            Err("portable schema ID must be a lowercase SHA-256".into())
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableSchemaDefinition {
    id: PortableSchemaId,
    catalog_id: String,
    definition: DeclarativeSchemaDefinition,
}

impl PortableSchemaDefinition {
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
        if is_digest(id) {
            Ok(())
        } else {
            Err("schema catalog ID must be a lowercase SHA-256".into())
        }
    }

    #[must_use]
    pub fn id(&self) -> &PortableSchemaId {
        &self.id
    }

    #[must_use]
    pub fn catalog_id(&self) -> &str {
        &self.catalog_id
    }

    #[must_use]
    pub fn canonical_name(&self) -> &str {
        &self.definition.name
    }

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

    pub fn current_of(accepted: &[Self]) -> Result<Self, String> {
        let (first, rest) = accepted
            .split_first()
            .ok_or("a shared schema name has no definition")?;
        let base = first.verify(&first.id)?;
        let mut union = base.clone();
        let mut names: BTreeSet<&str> = field_names(base).collect();
        for later in rest {
            let incoming = later.verify(&later.id)?;
            if incoming.name != base.name || !compatible(&union, incoming)? {
                return Err(INCOMPATIBLE.into());
            }
            for field in field_names(incoming) {
                if names.insert(field) {
                    copy_field(incoming, &mut union, field);
                }
            }
        }
        let mut complete = accepted.iter().filter(|definition| {
            field_names(&definition.definition).collect::<BTreeSet<_>>() == names
        });
        if let (Some(only), None) = (complete.next(), complete.next()) {
            return Ok(only.clone());
        }
        union.fields = Some(names.into_iter().map(str::to_owned).collect());
        union.identity_hash = None;
        union.identity_hash_algo_version = None;
        union.compute_identity_hash();
        let catalog_id = union
            .identity_hash
            .clone()
            .ok_or("the union of shared schema definitions has no identity")?;
        Self::from_catalog(&catalog_id, union)
    }

    pub fn bind_local_version(
        &self,
        expected: &PortableSchemaId,
        local: &DeclarativeSchemaDefinition,
    ) -> Result<&DeclarativeSchemaDefinition, String> {
        let definition = self.verify(expected)?;
        if local.name.is_empty() {
            return Err("portable schema requires an explicit local runtime name".into());
        }
        let mut candidate = local.clone();
        candidate.name.clone_from(&definition.name);
        candidate.identity_hash = None;
        candidate.identity_hash_algo_version = None;
        candidate.compute_identity_hash();
        let catalog_id = candidate
            .identity_hash
            .clone()
            .ok_or("the installed shared schema has no identity")?;
        let installed = Self::from_catalog(&catalog_id, candidate)?;
        if installed.definition.extends(definition)? {
            Ok(definition)
        } else {
            Err("the installed schema does not extend this shared schema definition".into())
        }
    }
}

impl DeclarativeSchemaDefinition {
    pub fn extends(&self, earlier: &Self) -> Result<bool, String> {
        let fields: BTreeSet<&str> = field_names(self).collect();
        Ok(self.name == earlier.name
            && field_names(earlier).all(|field| fields.contains(field))
            && compatible(earlier, self)?)
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

fn field_names(definition: &DeclarativeSchemaDefinition) -> impl Iterator<Item = &str> {
    definition.fields.iter().flatten().map(String::as_str)
}

fn compatible(
    a: &DeclarativeSchemaDefinition,
    b: &DeclarativeSchemaDefinition,
) -> Result<bool, String> {
    if shape(a)? != shape(b)? {
        return Ok(false);
    }
    let fields: BTreeSet<&str> = field_names(b).collect();
    for field in field_names(a).filter(|field| fields.contains(field)) {
        if field_entry(a, field)? != field_entry(b, field)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn shape(definition: &DeclarativeSchemaDefinition) -> Result<serde_json::Value, String> {
    let mut shape = definition.clone();
    shape.name.clear();
    shape.fields = None;
    shape.field_classifications.clear();
    shape.field_descriptions.clear();
    shape.field_interest_categories.clear();
    shape.field_types.clear();
    shape.field_hashes.clear();
    shape.field_versions.clear();
    shape.field_declarations.clear();
    shape.identity_hash = None;
    shape.identity_hash_algo_version = None;
    shape.hash = None;
    shape.superseded_by = None;
    serde_json::to_value(&shape).map_err(|_| "cannot encode a shared schema definition".into())
}

fn field_entry(
    definition: &DeclarativeSchemaDefinition,
    field: &str,
) -> Result<serde_json::Value, String> {
    serde_json::to_value((
        definition.field_types.get(field),
        definition.field_descriptions.get(field),
        definition.field_classifications.get(field),
        definition.field_interest_categories.get(field),
        definition.field_hashes.get(field),
        definition.field_versions.get(field),
        definition.field_declarations.get(field),
    ))
    .map_err(|_| "cannot encode a shared schema field".into())
}

fn copy_field(
    from: &DeclarativeSchemaDefinition,
    to: &mut DeclarativeSchemaDefinition,
    field: &str,
) {
    if let Some(value) = from.field_types.get(field) {
        to.field_types.insert(field.to_owned(), value.clone());
    }
    if let Some(value) = from.field_descriptions.get(field) {
        to.field_descriptions
            .insert(field.to_owned(), value.clone());
    }
    if let Some(value) = from.field_classifications.get(field) {
        to.field_classifications
            .insert(field.to_owned(), value.clone());
    }
    if let Some(value) = from.field_interest_categories.get(field) {
        to.field_interest_categories
            .insert(field.to_owned(), value.clone());
    }
    if let Some(value) = from.field_hashes.get(field) {
        to.field_hashes.insert(field.to_owned(), value.clone());
    }
    if let Some(value) = from.field_versions.get(field) {
        to.field_versions.insert(field.to_owned(), *value);
    }
    if let Some(value) = from.field_declarations.get(field) {
        to.field_declarations
            .insert(field.to_owned(), value.clone());
    }
}
