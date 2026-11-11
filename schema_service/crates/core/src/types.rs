//! Canonical vocabulary field and data-classification types.

use schema_types::FieldValueType;
use serde::{Deserialize, Serialize};

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

impl PartialOrd for DataClassification {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DataClassification {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.sensitivity_level.cmp(&other.sensitivity_level)
    }
}

impl std::fmt::Display for DataClassification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.sensitivity_name(), self.data_domain)
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
