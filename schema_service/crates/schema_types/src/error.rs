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
