use crate::{DeclarativeSchemaType, FieldValueType, KeyConfig};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fmt,
    str::FromStr,
};

mod owned;
pub use owned::OwnedDeclaration;

pub const IDENTITY_HASH_ALGO_VERSION: u32 = 4;
const RECORD_KEYS_IDENTITY_HASH_ALGO_VERSION: u32 = 3;
const FIELD_DERIVED_IDENTITY_HASH_ALGO_VERSION: u32 = 2;

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
    stored_version.filter(|stored| {
        stored_hash.is_some_and(|h| !h.is_empty()) && *stored > IDENTITY_HASH_ALGO_VERSION
    })
}

fn compute_identity_hash_parts<'a>(
    owner_app_id: Option<&str>,
    descriptive_name: Option<&str>,
    name: &str,
    fields: impl IntoIterator<Item = &'a str>,
    key: Option<&KeyConfig>,
) -> String {
    let field_names: Vec<&str> = fields
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let mut hasher = Sha256::new();
    if let Some(app_id) = owner_app_id.filter(|s| !s.is_empty()) {
        hasher.update(b"app:");
        hasher.update(app_id.as_bytes());
        hasher.update(b":");
    }
    hasher.update(
        descriptive_name
            .filter(|s| !s.is_empty())
            .unwrap_or(name)
            .as_bytes(),
    );
    hasher.update(b":");
    hasher.update(field_names.join(",").as_bytes());
    if let Some(key) = key {
        let hf = key.hash_field.as_deref().map_or("", str::trim);
        let rf = key.range_field.as_deref().map_or("", str::trim);
        if !hf.is_empty() || !rf.is_empty() {
            hasher.update(format!(":key:{hf}:{rf}").as_bytes());
        }
    }
    format!("{:x}", hasher.finalize())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(remote = "Self")]
pub struct DeclarativeSchemaDefinition {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub descriptive_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purpose_statement: Option<String>,
    pub schema_type: DeclarativeSchemaType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<KeyConfig>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub alternate_keys: BTreeMap<String, crate::AlternateKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_keys: Option<crate::record_keys::RecordKeys>,
    #[serde(default, skip_serializing_if = "crate::alternate_keys::is_false")]
    pub file_required: bool,
    #[serde(default)]
    pub fields: Option<Vec<String>>,
    #[serde(skip_deserializing, skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_classifications: HashMap<String, Vec<String>>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_descriptions: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_interest_categories: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_types: HashMap<String, FieldValueType>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_hashes: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_versions: HashMap<String, u32>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_declarations: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_hash_algo_version: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_app_id: Option<String>,
}

impl DeclarativeSchemaDefinition {
    #[must_use]
    pub fn get_field_type(&self, field_name: &str) -> &FieldValueType {
        self.field_types
            .get(field_name)
            .unwrap_or(&FieldValueType::Any)
    }

    pub fn ensure_field_hashes(&mut self) {
        let mut seen = HashSet::new();
        let names: Vec<String> = self
            .fields
            .iter()
            .flatten()
            .chain(self.field_types.keys())
            .chain(self.field_descriptions.keys())
            .filter(|n| !self.field_hashes.contains_key(*n) && seen.insert((*n).clone()))
            .cloned()
            .collect();
        for name in names {
            let desc = self
                .field_descriptions
                .get(&name)
                .map_or("", String::as_str);
            let version = *self.field_versions.get(&name).unwrap_or(&1);
            let hash = crate::compute_field_hash(&name, desc, self.get_field_type(&name), version);
            self.field_versions.entry(name.clone()).or_insert(version);
            self.field_hashes.insert(name, hash);
        }
    }

    pub fn validate_definition(&self) -> Result<(), String> {
        if self.file_required && self.record_keys.is_none() {
            return Err("file_required needs explicit record keys".into());
        }
        self.validate_field_names()?;
        self.validate_key_field_types()?;
        if !self.alternate_keys.is_empty() || self.record_keys.is_some() {
            self.validate_owned_schema()?;
        }
        Ok(())
    }

    pub fn validate_field_name(name: &str) -> Result<(), String> {
        if matches!(name, "\u{1f}record" | "__record") {
            return Err(format!(
                "field name {name:?} is reserved and is not supported"
            ));
        }
        if name.is_empty()
            || name
                .chars()
                .any(|c| c.is_whitespace() || c.is_control() || matches!(c, ',' | ':'))
        {
            return Err(format!(
                "field name {name:?} must be nonempty and cannot contain whitespace, control characters, commas, or colons"
            ));
        }
        Ok(())
    }

    pub fn validate_field_names(&self) -> Result<(), String> {
        let keys = [
            self.field_classifications.keys().collect::<Vec<_>>(),
            self.field_descriptions.keys().collect(),
            self.field_interest_categories.keys().collect(),
            self.field_types.keys().collect(),
            self.field_hashes.keys().collect(),
            self.field_declarations.keys().collect(),
            self.field_versions.keys().collect(),
        ];
        self.fields
            .iter()
            .flatten()
            .chain(keys.into_iter().flatten())
            .map(String::as_str)
            .chain(
                self.key
                    .iter()
                    .flat_map(|k| [k.hash_field.as_deref(), k.range_field.as_deref()])
                    .flatten(),
            )
            .try_for_each(Self::validate_field_name)
    }

    pub fn compute_identity_hash(&mut self) {
        let base = compute_identity_hash_parts(
            self.owner_app_id.as_deref(),
            self.descriptive_name.as_deref(),
            &self.name,
            self.fields.iter().flatten().map(String::as_str),
            self.key.as_ref(),
        );
        if let Some(keys) = &self.record_keys {
            let mut hash = Sha256::new();
            hash.update(b"lastdb:record-schema:v1\0");
            hash.update(base.as_bytes());
            hash.update(serde_json::to_vec(keys).expect("record key definitions serialize"));
            if self.file_required {
                hash.update(b"\0file-required");
            }
            self.identity_hash = Some(format!("{:x}", hash.finalize()));
            self.identity_hash_algo_version = Some(if self.file_required {
                IDENTITY_HASH_ALGO_VERSION
            } else {
                RECORD_KEYS_IDENTITY_HASH_ALGO_VERSION
            });
        } else {
            self.identity_hash = Some(base);
            self.identity_hash_algo_version = Some(FIELD_DERIVED_IDENTITY_HASH_ALGO_VERSION);
        }
    }

    #[must_use]
    pub fn identity_is_newer_than_binary(&self) -> bool {
        refuses_identity_downgrade(
            self.identity_hash.as_deref(),
            self.identity_hash_algo_version,
        )
        .is_some()
    }

    #[must_use = "a refused downgrade must be surfaced, not dropped"]
    pub fn recompute_identity_hash_unless_newer(&mut self) -> IdentityRecompute {
        if let Some(stored) = refuses_identity_downgrade(
            self.identity_hash.as_deref(),
            self.identity_hash_algo_version,
        ) {
            return IdentityRecompute::RefusedDowngrade {
                stored,
                binary: IDENTITY_HASH_ALGO_VERSION,
            };
        }
        self.compute_identity_hash();
        IdentityRecompute::Recomputed
    }

    #[must_use]
    pub fn get_identity_hash(&self) -> Option<&String> {
        self.identity_hash.as_ref()
    }
}

impl PartialEq for DeclarativeSchemaDefinition {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.descriptive_name == other.descriptive_name
            && self.purpose_statement == other.purpose_statement
            && self.schema_type == other.schema_type
            && self.key == other.key
            && self.alternate_keys == other.alternate_keys
            && self.record_keys == other.record_keys
            && self.file_required == other.file_required
            && self.fields == other.fields
            && self.hash == other.hash
            && self.field_classifications == other.field_classifications
            && self.field_descriptions == other.field_descriptions
            && self.field_interest_categories == other.field_interest_categories
            && self.field_types == other.field_types
            && self.field_hashes == other.field_hashes
            && self.field_declarations == other.field_declarations
            && self.field_versions == other.field_versions
            && self.identity_hash == other.identity_hash
            && self.superseded_by == other.superseded_by
            && self.owner_app_id == other.owner_app_id
    }
}

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
        let value = serde_json::to_value(&definition)
            .map_err(|_| "cannot encode portable schema definition".to_string())?;
        let bytes = app_identity_crypto::canonicalize(&value)
            .map_err(|_| "cannot canonicalize portable schema definition".to_string())?;
        let mut digest = Sha256::new();
        digest.update(b"lastdb:portable-schema-definition:v1\0");
        digest.update(bytes);
        Ok(Self(app_identity_crypto::hex_lower(&digest.finalize())))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PortableSchemaId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
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
        is_digest(&value)
            .then_some(Self(value))
            .ok_or_else(|| "portable schema ID must be a lowercase SHA-256".into())
    }
}

fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}
