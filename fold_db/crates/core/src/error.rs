//! Unified FoldDB error type and result alias.

use crate::schema::types::SchemaError;
use std::{fmt, io};

pub use crate::access;
pub use crate::limits::{content_limit, manifest_retention};

#[derive(Debug)]
pub enum FoldDbError {
    Schema(SchemaError),
    Database(String),
    Config(String),
    Io(io::Error),
    Serialization(String),
    SecurityError(String),
    InProgress(String),
}

impl fmt::Display for FoldDbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Schema(err) => write!(f, "Schema error: {err}"),
            Self::Database(msg) => write!(f, "Database error: {msg}"),
            Self::Config(msg) => write!(f, "Configuration error: {msg}"),
            Self::Io(err) => write!(f, "IO error: {err}"),
            Self::Serialization(msg) => write!(f, "Serialization error: {msg}"),
            Self::SecurityError(msg) => write!(f, "Security error: {msg}"),
            Self::InProgress(msg) => write!(f, "operation in progress: {msg}"),
        }
    }
}

impl std::error::Error for FoldDbError {}

impl From<SchemaError> for FoldDbError {
    fn from(e: SchemaError) -> Self {
        Self::Schema(e)
    }
}

impl From<io::Error> for FoldDbError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_json::Error> for FoldDbError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serialization(e.to_string())
    }
}

impl From<crate::storage::StorageError> for FoldDbError {
    fn from(error: crate::storage::StorageError) -> Self {
        match error {
            crate::storage::StorageError::MutationLimit { detail } => {
                Self::Schema(SchemaError::WriteResourceLimit { detail })
            }
            other => Self::Database(other.to_string()),
        }
    }
}

pub type FoldDbResult<T> = Result<T, FoldDbError>;
