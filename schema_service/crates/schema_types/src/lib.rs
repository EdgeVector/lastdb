//! Pure wire/domain schema types shared across Schema Service and LastDB.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;

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
    compute_identity_hash_parts, refuses_identity_downgrade, DeclarativeSchemaDefinition,
    IdentityRecompute, OwnedDeclaration, PortableSchemaDefinition, PortableSchemaId,
    IDENTITY_HASH_ALGO_VERSION,
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

#[derive(Debug)]
pub enum FoldDbError {
    Config(String),
}

impl fmt::Display for FoldDbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(msg) => write!(f, "Configuration error: {msg}"),
        }
    }
}

impl std::error::Error for FoldDbError {}

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
