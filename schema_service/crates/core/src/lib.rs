//! Advisory Schema Service core: curated canonical vocabulary and deterministic
//! draft schema advice (`GET /v1/fields/vocabulary`, `POST /v1/advice/schema`).

pub mod advice;
pub mod field_name_policy;

use schema_types::{FieldValueType, FoldDbError, FoldDbResult};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};

/// Maximum allowed sensitivity level (`0` = Public through `4` = Highly Restricted).
pub const MAX_SENSITIVITY_LEVEL: u8 = 4;

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
pub struct CanonicalField {
    pub description: String,
    pub field_type: FieldValueType,
    /// Field identity version (`name+description+type+version` -> `field_hash`).
    #[serde(default = "default_field_version")]
    pub version: u32,
    /// Code validation policy version. Excluded from field identity hashes.
    #[serde(default)]
    pub name_policy_version: u32,
    /// Usual data classification label for this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classification: Option<DataClassification>,
    /// Optional interest category for discovery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interest_category: Option<String>,
}

const fn default_field_version() -> u32 {
    1
}

impl CanonicalField {
    /// Schema Service field identity hash for this entry under `name`.
    #[must_use]
    pub fn field_hash(&self, name: &str) -> String {
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

fn builtin_definitions() -> FoldDbResult<Vec<(String, CanonicalField)>> {
    let entries: Vec<SeedEntry> =
        serde_json::from_str(include_str!("builtin_canonical_fields.json"))
            .map_err(|error| FoldDbError::Config(format!("invalid canonical seed: {error}")))?;
    entries
        .into_iter()
        .map(|entry| {
            let classification = DataClassification::new(entry.sensitivity, &entry.domain)
                .map_err(|error| {
                    FoldDbError::Config(format!("invalid canonical seed classification: {error}"))
                })?;
            Ok((
                entry.name,
                CanonicalField {
                    description: entry.description,
                    field_type: entry.field_type,
                    version: 1,
                    name_policy_version: field_name_policy::NAME_POLICY_VERSION,
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
    pub fn from_builtin() -> FoldDbResult<Self> {
        let definitions = builtin_definitions()?;
        let canonical_fields =
            field_name_policy::canonical_fields_after_insert(&HashMap::new(), &definitions)?;
        let sorted: BTreeMap<&String, &CanonicalField> = canonical_fields.iter().collect();
        let vocabulary_json = json!({
            "name_policy_version": field_name_policy::NAME_POLICY_VERSION,
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

/// Standard single-field JSON error envelope.
#[must_use]
pub fn error_only_body(message: &str) -> Value {
    json!({ "error": message })
}
