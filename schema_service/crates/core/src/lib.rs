//! Advisory Schema Service core: curated canonical vocabulary and deterministic
//! draft schema advice (`GET /v1/fields/vocabulary`, `POST /v1/advice/schema`).

pub mod advice;

use schema_types::FieldValueType;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};

/// Maximum allowed sensitivity level (`0` = Public through `4` = Highly Restricted).
const MAX_SENSITIVITY_LEVEL: u8 = 4;

/// Validation rule version; excluded from field identity hashes.
const NAME_POLICY_VERSION: u32 = 1;

const APP_ABBREVIATIONS: &[&str] = &[
    "bcl", "bes", "bfc", "bge", "bll", "bln", "blp", "btd", "bts", "clu", "ctd", "eph", "ie",
    "klh", "liv", "nh", "prk", "psi", "rle", "tmb",
];

const SHORT_WORDS: &[&str] = &[
    "at", "by", "in", "of", "on", "or", "to", "id", "ip", "cm", "kg", "km", "mm", "ms", "ns", "ph",
    "us",
];

/// Two-dimensional data classification label for a canonical vocabulary field.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DataClassification {
    /// Sensitivity level: `0` (Public) through `4` (Highly Restricted).
    pub sensitivity_level: u8,
    /// Data domain tag identifying the type of data (e.g. `"financial"`, `"identity"`).
    pub data_domain: String,
}

impl<'de> Deserialize<'de> for DataClassification {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper {
            sensitivity_level: u8,
            data_domain: String,
        }
        let helper = Helper::deserialize(deserializer)?;
        Self::new(helper.sensitivity_level, helper.data_domain).map_err(serde::de::Error::custom)
    }
}

impl DataClassification {
    /// Create a validated [`DataClassification`] with trimmed `data_domain`.
    pub fn new(sensitivity_level: u8, data_domain: impl Into<String>) -> Result<Self, String> {
        if sensitivity_level > MAX_SENSITIVITY_LEVEL {
            return Err(format!(
                "sensitivity_level {sensitivity_level} exceeds maximum {MAX_SENSITIVITY_LEVEL} (Highly Restricted)"
            ));
        }
        let data_domain = data_domain.into();
        let trimmed = data_domain.trim();
        if trimmed.is_empty() {
            return Err("data_domain must not be empty".to_string());
        }
        Ok(Self {
            sensitivity_level,
            data_domain: trimmed.to_string(),
        })
    }

    /// Human-readable name for this sensitivity level.
    #[must_use]
    pub const fn sensitivity_name(&self) -> &'static str {
        match self.sensitivity_level {
            0 => "Public",
            1 => "Internal",
            2 => "Confidential",
            3 => "Restricted",
            4 => "Highly Restricted",
            _ => "Unknown",
        }
    }
}

/// A canonical field entry in the standard field vocabulary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct CanonicalField {
    pub(crate) description: String,
    pub(crate) field_type: FieldValueType,
    /// Field identity version (`name+description+type+version` -> `field_hash`).
    #[serde(default = "default_field_version")]
    pub(crate) version: u32,
    /// Code validation policy version. Excluded from field identity hashes.
    #[serde(default)]
    pub(crate) name_policy_version: u32,
    /// Usual data classification label for this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) classification: Option<DataClassification>,
    /// Optional interest category for discovery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) interest_category: Option<String>,
}

const fn default_field_version() -> u32 {
    1
}

impl CanonicalField {
    /// Schema Service field identity hash for this entry under `name`.
    #[must_use]
    pub(crate) fn field_hash(&self, name: &str) -> String {
        schema_types::compute_field_hash(name, &self.description, &self.field_type, self.version)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SeedEntry {
    name: String,
    description: String,
    field_type: FieldValueType,
    sensitivity: u8,
    domain: String,
    interest_category: Option<String>,
}

fn builtin_definitions() -> Result<Vec<(String, CanonicalField)>, String> {
    let entries: Vec<SeedEntry> =
        serde_json::from_str(include_str!("builtin_canonical_fields.json"))
            .map_err(|error| format!("invalid canonical seed: {error}"))?;
    entries
        .into_iter()
        .map(|entry| {
            let classification = DataClassification::new(entry.sensitivity, &entry.domain)
                .map_err(|error| format!("invalid canonical seed classification: {error}"))?;
            Ok((
                entry.name,
                CanonicalField {
                    description: entry.description,
                    field_type: entry.field_type,
                    version: 1,
                    name_policy_version: NAME_POLICY_VERSION,
                    classification: Some(classification),
                    interest_category: entry.interest_category,
                },
            ))
        })
        .collect()
}

/// Immutable in-memory advisory state constructed from the curated canonical vocabulary.
#[derive(Debug, Clone)]
pub struct SchemaServiceState {
    pub(crate) canonical_fields: HashMap<String, CanonicalField>,
    vocabulary_json: Value,
}

impl SchemaServiceState {
    /// Load and validate the built-in canonical field vocabulary.
    ///
    /// # Errors
    /// Returns an error if the embedded canonical seed violates vocabulary or naming rules.
    pub fn from_builtin() -> Result<Self, String> {
        let definitions = builtin_definitions()?;
        let canonical_fields = canonical_fields_after_insert(&definitions)?;
        let sorted: BTreeMap<&String, &CanonicalField> = canonical_fields.iter().collect();
        let vocabulary_json = json!({
            "name_policy_version": NAME_POLICY_VERSION,
            "fields": sorted,
        });
        Ok(Self {
            canonical_fields,
            vocabulary_json,
        })
    }

    /// Public standard vocabulary document (`GET /v1/fields/vocabulary`).
    #[must_use]
    pub const fn canonical_vocabulary(&self) -> &Value {
        &self.vocabulary_json
    }
}

/// Syntax gate for canonical field names.
pub(crate) fn validate_new_field_name(name: &str) -> Result<(), String> {
    let words: Vec<_> = name.split('_').collect();
    let reason = if name.len() > 96 || name.is_empty() {
        Some("use 1 through 96 characters")
    } else if words.iter().any(|word| {
        word.is_empty()
            || !word.as_bytes()[0].is_ascii_lowercase()
            || !word
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
    }) {
        Some("use lowercase words separated by single underscores")
    } else if words
        .iter()
        .filter(|word| {
            !matches!(
                **word,
                "of" | "to" | "by" | "in" | "at" | "for" | "and" | "the"
            )
        })
        .count()
        < 2
    {
        Some("name both the subject and its property")
    } else if words.iter().any(|word| word.len() == 1) {
        Some("spell out each concept; a one-letter key role is not a concept")
    } else if words
        .iter()
        .any(|word| word.len() == 2 && !SHORT_WORDS.contains(word))
    {
        Some("spell out short names; use standard key terms, connectives or units")
    } else if words.iter().enumerate().any(|(index, word)| {
        APP_ABBREVIATIONS.contains(word)
            && !(*word == "psi"
                && index > 0
                && words[index - 1] == "pressure"
                && index + 1 == words.len())
    }) {
        Some("spell out app abbreviations")
    } else if words
        .iter()
        .any(|word| matches!(*word, "generic" | "entity" | "item"))
    {
        Some("replace the generic subject with the actual concept")
    } else {
        None
    };
    if let Some(reason) = reason {
        Err(format!(
            "invalid_canonical_name: '{name}': {reason}; for example 'file_size_bytes'"
        ))
    } else {
        Ok(())
    }
}

fn canonical_fields_after_insert(
    fields: &[(String, CanonicalField)],
) -> Result<HashMap<String, CanonicalField>, String> {
    let mut candidate = HashMap::with_capacity(fields.len());
    for (name, field) in fields {
        validate_insert(&candidate, name, field)?;
        candidate.insert(name.clone(), field.clone());
    }
    Ok(candidate)
}

fn validate_insert(
    existing: &HashMap<String, CanonicalField>,
    name: &str,
    field: &CanonicalField,
) -> Result<(), String> {
    validate_new_field_name(name)?;
    if field.name_policy_version != NAME_POLICY_VERSION {
        return Err(format!(
            "invalid_canonical_policy: '{name}' requires name policy {NAME_POLICY_VERSION}"
        ));
    }
    if field.description.trim().is_empty() || field.version == 0 {
        return Err(format!(
            "canonical_field_definition_conflict: '{name}' requires a description and a positive identity version"
        ));
    }
    if field.description.len() > 4096 || words(&field.description).len() < 3 {
        return Err(format!(
            "invalid_canonical_description: '{name}' requires a specific description of 3 or more words, at most 4096 bytes"
        ));
    }
    if existing.get(name).is_some_and(|stored| stored != field) {
        return Err(format!(
            "canonical_field_conflict: '{name}' already has another definition; reset it explicitly before replacement"
        ));
    }
    for (other_name, other) in existing {
        if other_name != name && same_description(&other.description, &field.description) {
            return Err(format!(
                "canonical_field_reuse_required: '{name}' duplicates '{other_name}'; reuse that concept or describe a distinct subject and property"
            ));
        }
    }
    Ok(())
}

fn words(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|part| !part.is_empty())
        .map(str::to_lowercase)
        .collect()
}

pub(crate) fn same_description(left: &str, right: &str) -> bool {
    words(left) == words(right)
}
