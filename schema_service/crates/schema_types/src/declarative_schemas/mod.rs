use crate::schema_type::DeclarativeSchemaType;
use crate::FieldValueType;
use crate::KeyConfig;
use serde::Serialize;
use std::collections::HashMap;

mod construct;
mod deserialize;
mod equality;
mod identity;
mod key_type_resolution;
mod key_types;
mod owned;
mod portable;
mod validation;

/// The schema identity algorithm lives here and nowhere else. Every schema
/// type in the monorepo — including `fold_db`'s, which is a distinct struct —
/// calls these, so the algorithm cannot be forked back apart.
pub use identity::{
    compute_identity_hash_parts, parse_canonical_name, refuses_identity_downgrade,
    IdentityRecompute, IDENTITY_HASH_ALGO_VERSION,
};
pub use owned::{schema_from_value, OwnedDeclaration};
pub use portable::{PortableSchemaDefinition, PortableSchemaId};

/// Declarative schema definition - pure wire/domain form (no runtime FieldVariant).
///
/// This is the unified schema type used by schema_service. fold_db wraps this
/// type and adds `runtime_fields`.
#[derive(Debug, Clone, Serialize)]
pub struct DeclarativeSchemaDefinition {
    /// Schema name
    pub name: String,
    /// Human-readable descriptive name for the schema (used in AI-generated proposals)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub descriptive_name: Option<String>,
    /// One-sentence declaration of what the schema is semantically for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purpose_statement: Option<String>,
    /// Schema type ("Single" | "Hash" | "Range" | "HashRange")
    pub schema_type: DeclarativeSchemaType,
    /// Key configuration (required when schema_type == "Hash", "Range", or "HashRange")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<KeyConfig>,
    /// Named lookups of these same records; independent of canonical field reuse.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub alternate_keys: std::collections::BTreeMap<String, crate::AlternateKey>,
    /// Complete external record addresses, independent of payload fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_keys: Option<crate::record_keys::RecordKeys>,
    /// Every live record must own a file.
    #[serde(default, skip_serializing_if = "crate::alternate_keys::is_false")]
    pub file_required: bool,
    /// Field names - plain data fields
    pub fields: Option<Vec<String>>,
    /// SHA256 hash of the schema content for integrity verification
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    /// Classification tags for each field (e.g. "word", "name:person", "date", "number")
    /// Maps field_name -> list of classification strings
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_classifications: HashMap<String, Vec<String>>,
    /// Natural language descriptions for each field.
    /// Maps field_name -> description string.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_descriptions: HashMap<String, String>,
    /// Interest categories for each field (e.g. "Photography", "Cooking", "Running").
    /// Maps field_name -> interest category string.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_interest_categories: HashMap<String, String>,
    /// Strongly typed field value types.
    /// Maps field_name -> FieldValueType. Fields not in this map default to Any.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_types: HashMap<String, FieldValueType>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_hashes: HashMap<String, String>,
    /// Field identity version used when minting `field_hashes` (default 1).
    /// Maps field_name -> version.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_versions: HashMap<String, u32>,
    /// Declared-field provenance: local field name -> `declaration_id`.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_declarations: HashMap<String, String>,
    /// SHA256 hash of sorted field names — unique fingerprint of schema structure
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity_hash: Option<String>,
    /// Version of the algorithm that minted `identity_hash`.
    /// Deliberately **not** part of `PartialEq`: it describes the provenance of
    /// the hash, not the schema's content.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub identity_hash_algo_version: Option<u32>,
    /// If set, this schema has been superseded by the named schema.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub superseded_by: Option<String>,
    /// Identifier of the app that owns this schema, e.g. `"fbrain"`.
    /// Participates in `compute_identity_hash`.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub owner_app_id: Option<String>,
}
