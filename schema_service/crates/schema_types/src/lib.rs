//! Pure wire/domain schema types shared across Schema Service and LastDB.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fmt, io};

pub mod alternate_keys;
pub mod declarative_schemas;
pub mod field_value_type;
pub mod key_comparison;
pub mod range_storage_profile;
pub mod record_keys;

pub mod field_range_profile {
    pub use crate::range_storage_profile::FieldRangeProfile;
}

pub use alternate_keys::{AlternateKey, MissingAlternateKey};
pub use declarative_schemas::{
    compute_identity_hash_parts, parse_canonical_name, refuses_identity_downgrade,
    schema_from_value, DeclarativeSchemaDefinition, IdentityRecompute, OwnedDeclaration,
    PortableSchemaDefinition, PortableSchemaId, IDENTITY_HASH_ALGO_VERSION,
};
pub use field_value_type::FieldValueType;

pub type Schema = DeclarativeSchemaDefinition;
pub type SchemaType = DeclarativeSchemaType;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DeclarativeSchemaType {
    Single,
    Hash,
    Range,
    HashRange,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct KeyConfig {
    pub hash_field: Option<String>,
    pub range_field: Option<String>,
}

impl KeyConfig {
    pub fn new(hash_field: Option<String>, range_field: Option<String>) -> Self {
        Self {
            hash_field,
            range_field,
        }
    }
}

#[derive(Debug)]
pub enum FoldDbError {
    Schema(String),
    Database(String),
    Config(String),
    Io(io::Error),
    Serialization(String),
    Other(String),
}

impl fmt::Display for FoldDbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Schema(msg) => write!(f, "Schema error: {msg}"),
            Self::Database(msg) => write!(f, "Database error: {msg}"),
            Self::Config(msg) => write!(f, "Configuration error: {msg}"),
            Self::Io(err) => write!(f, "IO error: {err}"),
            Self::Serialization(msg) => write!(f, "Serialization error: {msg}"),
            Self::Other(msg) => write!(f, "Error: {msg}"),
        }
    }
}

impl std::error::Error for FoldDbError {}

impl From<io::Error> for FoldDbError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for FoldDbError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialization(error.to_string())
    }
}

pub type FoldDbResult<T> = Result<T, FoldDbError>;

#[must_use]
pub fn compute_field_hash(
    name: &str,
    description: &str,
    field_type: &FieldValueType,
    version: u32,
) -> String {
    let type_json = serde_json::to_string(field_type).unwrap_or_else(|_| "\"Any\"".to_string());
    let payload = format!(
        r#"{{"description":{},"name":{},"type":{},"version":{}}}"#,
        serde_json::to_string(description.trim()).unwrap_or_else(|_| "\"\"".into()),
        serde_json::to_string(name.trim()).unwrap_or_else(|_| "\"\"".into()),
        type_json,
        version
    );
    app_identity_crypto::hex_lower(&Sha256::digest(payload.as_bytes()))
}
