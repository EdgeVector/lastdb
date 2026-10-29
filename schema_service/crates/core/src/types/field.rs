use schema_types::DataClassification;
use schema_types::FieldValueType;
use serde::{Deserialize, Serialize};

/// A canonical field entry in the global field registry.
/// Carries description (for semantic matching), type (for enforcement),
/// optional data classification (for sensitivity labeling), and optional
/// interest category (for discovery/social features).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanonicalField {
    pub description: String,
    pub field_type: FieldValueType,
    /// Field identity version (name+description+type+version → field_hash).
    /// Defaults to 1 for legacy registry rows.
    #[serde(default = "default_field_version")]
    pub version: u32,
    /// Code validation policy version. Never part of field identity or caller authority.
    #[serde(default)]
    pub name_policy_version: u32,
    /// Data classification label for this field. `None` for legacy fields
    /// that were registered before classification was required.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classification: Option<DataClassification>,
    /// Interest category for discovery (e.g. "Photography", "Cooking", "Running").
    /// Supplied by the approved definition. `None` for concepts without
    /// a user-interest category.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interest_category: Option<String>,
}

fn default_field_version() -> u32 {
    1
}

impl CanonicalField {
    /// Schema Service field identity hash for this registry entry under `name`.
    #[must_use]
    pub fn field_hash(&self, name: &str) -> String {
        schema_types::compute_field_hash(name, &self.description, &self.field_type, self.version)
    }
}
