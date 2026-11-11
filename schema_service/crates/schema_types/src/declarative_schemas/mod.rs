use crate::{DeclarativeSchemaType, FieldValueType, KeyConfig};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};

mod owned;
mod portable;

pub use owned::OwnedDeclaration;
pub use portable::PortableSchemaId;

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

#[must_use]
pub fn compute_identity_hash_parts<'a, I>(
    owner_app_id: Option<&str>,
    descriptive_name: Option<&str>,
    name: &str,
    fields: I,
    key: Option<&KeyConfig>,
) -> String
where
    I: IntoIterator<Item = &'a str>,
{
    let mut field_names: Vec<&str> = fields.into_iter().collect();
    field_names.sort_unstable();
    field_names.dedup();
    let combined = field_names.join(",");
    let mut hasher = Sha256::new();

    if let Some(app_id) = owner_app_id.filter(|s| !s.is_empty()) {
        hasher.update(b"app:");
        hasher.update(app_id.as_bytes());
        hasher.update(b":");
    }

    let readable_name = descriptive_name.filter(|s| !s.is_empty()).unwrap_or(name);
    hasher.update(readable_name.as_bytes());
    hasher.update(b":");
    hasher.update(combined.as_bytes());

    if let Some(key) = key {
        let hf = key
            .hash_field
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or_default();
        let rf = key
            .range_field
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or_default();
        if !hf.is_empty() || !rf.is_empty() {
            hasher.update(b":key:");
            hasher.update(hf.as_bytes());
            hasher.update(b":");
            hasher.update(rf.as_bytes());
        }
    }

    format!("{:x}", hasher.finalize())
}

#[derive(Debug, Clone, Serialize)]
pub struct DeclarativeSchemaDefinition {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub descriptive_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purpose_statement: Option<String>,
    pub schema_type: DeclarativeSchemaType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<KeyConfig>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub alternate_keys: BTreeMap<String, crate::AlternateKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_keys: Option<crate::record_keys::RecordKeys>,
    #[serde(default, skip_serializing_if = "crate::alternate_keys::is_false")]
    pub file_required: bool,
    pub fields: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub identity_hash_algo_version: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub superseded_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub owner_app_id: Option<String>,
}

impl DeclarativeSchemaDefinition {
    #[must_use]
    pub fn get_field_type(&self, field_name: &str) -> &FieldValueType {
        static ANY: FieldValueType = FieldValueType::Any;
        self.field_types.get(field_name).unwrap_or(&ANY)
    }

    pub fn ensure_field_hashes(&mut self) {
        let names: Vec<String> = self
            .fields
            .clone()
            .unwrap_or_default()
            .into_iter()
            .chain(self.field_types.keys().cloned())
            .chain(self.field_descriptions.keys().cloned())
            .collect();
        let mut seen = HashSet::new();
        for name in names {
            if !seen.insert(name.clone()) || self.field_hashes.contains_key(&name) {
                continue;
            }
            let description = self
                .field_descriptions
                .get(&name)
                .cloned()
                .unwrap_or_default();
            let field_type = self.get_field_type(&name).clone();
            let version = *self.field_versions.get(&name).unwrap_or(&1);
            let hash = crate::compute_field_hash(&name, &description, &field_type, version);
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
        self.validate_alternate_keys()
    }

    pub fn validate_field_name(name: &str) -> Result<(), String> {
        if matches!(name, "\u{1f}record" | "__record") {
            return Err(format!(
                "field name {name:?} is reserved and is not supported"
            ));
        }
        if name.is_empty()
            || name.chars().any(|character| {
                character.is_whitespace()
                    || character.is_control()
                    || matches!(character, ',' | ':')
            })
        {
            return Err(format!(
                "field name {name:?} must be nonempty and cannot contain whitespace, control characters, commas, or colons"
            ));
        }
        Ok(())
    }

    pub fn validate_field_names(&self) -> Result<(), String> {
        let mut names = self
            .fields
            .iter()
            .flatten()
            .map(String::as_str)
            .chain(self.field_classifications.keys().map(String::as_str))
            .chain(self.field_descriptions.keys().map(String::as_str))
            .chain(self.field_interest_categories.keys().map(String::as_str))
            .chain(self.field_types.keys().map(String::as_str))
            .chain(self.field_hashes.keys().map(String::as_str))
            .chain(self.field_declarations.keys().map(String::as_str))
            .chain(self.field_versions.keys().map(String::as_str))
            .chain(self.key.iter().filter_map(|key| key.hash_field.as_deref()))
            .chain(self.key.iter().filter_map(|key| key.range_field.as_deref()));
        names.try_for_each(Self::validate_field_name)
    }

    pub fn compute_identity_hash(&mut self) {
        let base = compute_identity_hash_parts(
            self.owner_app_id.as_deref(),
            self.descriptive_name.as_deref(),
            &self.name,
            self.fields
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(String::as_str),
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
