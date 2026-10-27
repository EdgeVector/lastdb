//! Lightweight error type for schema wire/service code.
//!
//! Adapted from `fold_db::error::FoldDbError` but without the
//! `Schema(SchemaError)` / `StorageError` coupling — those keep living in
//! `fold_db` for the node stack. Schema service only needs Config /
//! Serialization / Database-style string variants.

use std::fmt;
use std::io;

/// Unified error type for schema_service (and other fold_db-free consumers).
#[derive(Debug)]
pub enum FoldDbError {
    /// Errors related to schema operations (string form; no SchemaError dep).
    Schema(String),

    /// Errors related to database / persistence operations
    Database(String),

    /// Errors related to permission checks
    Permission(String),

    /// Errors related to configuration
    Config(String),

    /// Errors related to IO operations
    Io(io::Error),

    /// Errors related to serialization/deserialization
    Serialization(String),

    /// Errors related to security operations
    SecurityError(String),

    /// App already exists with a different owner (typed for proper error mapping).
    /// Contains the current owner's dev public key.
    AppOwnerConflict { current_owner_dev_pubkey: String },

    /// Channel generation conflict (typed for proper error mapping).
    /// Contains the expected and supplied generation values.
    GenerationConflict { expected: u64, supplied: u64 },

    /// Compat row already exists (typed for proper 409 error mapping).
    /// Contains the release_id and contract_version that already exist.
    CompatRowExists {
        release_id: String,
        contract_version: String,
    },

    /// S3/storage operation error (string form).
    S3(String),

    /// Other errors that don't fit into the above categories
    Other(String),
}

impl fmt::Display for FoldDbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Schema(msg) => write!(f, "Schema error: {msg}"),
            Self::Database(msg) => write!(f, "Database error: {msg}"),
            Self::Permission(msg) => write!(f, "Permission error: {msg}"),
            Self::Config(msg) => write!(f, "Configuration error: {msg}"),
            Self::Io(err) => write!(f, "IO error: {err}"),
            Self::Serialization(msg) => write!(f, "Serialization error: {msg}"),
            Self::SecurityError(msg) => write!(f, "Security error: {msg}"),
            Self::AppOwnerConflict {
                current_owner_dev_pubkey,
            } => write!(
                f,
                "App already exists with different owner: {current_owner_dev_pubkey}"
            ),
            Self::GenerationConflict { expected, supplied } => write!(
                f,
                "Generation conflict: expected {expected}, supplied {supplied}"
            ),
            Self::CompatRowExists {
                release_id,
                contract_version,
            } => write!(
                f,
                "Compat row already exists: release_id={release_id}, contract_version={contract_version}"
            ),
            Self::S3(msg) => write!(f, "S3 error: {msg}"),
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

/// Result type alias for operations that can result in a FoldDbError
pub type FoldDbResult<T> = Result<T, FoldDbError>;
