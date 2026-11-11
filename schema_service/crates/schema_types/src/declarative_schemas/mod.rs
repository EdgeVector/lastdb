use crate::{DeclarativeSchemaType, FieldValueType, KeyConfig};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};

mod deserialize;
mod identity;
mod owned;
mod portable;

pub use identity::{
    compute_identity_hash_parts, parse_canonical_name, refuses_identity_downgrade,
    IdentityRecompute, IDENTITY_HASH_ALGO_VERSION,
};
pub use owned::{schema_from_value, OwnedDeclaration};
pub use portable::{PortableSchemaDefinition, PortableSchemaId};

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
    pub fn new(
        name: String,
        schema_type: DeclarativeSchemaType,
        key: Option<KeyConfig>,
        fields: Option<Vec<String>>,
    ) -> Self {
        Self {
            name,
            descriptive_name: None,
            purpose_statement: None,
            schema_type,
            key,
            alternate_keys: BTreeMap::new(),
            record_keys: None,
            file_required: false,
            fields,
            hash: None,
            field_classifications: HashMap::new(),
            field_descriptions: HashMap::new(),
            field_interest_categories: HashMap::new(),
            field_types: HashMap::new(),
            field_hashes: HashMap::new(),
            field_declarations: HashMap::new(),
            field_versions: HashMap::new(),
            identity_hash: None,
            identity_hash_algo_version: None,
            superseded_by: None,
            owner_app_id: None,
        }
    }

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
