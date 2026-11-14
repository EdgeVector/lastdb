//! Storage error types and database path configuration.

use crate::schema::types::SchemaError;
use std::io::ErrorKind;
use thiserror::Error;

pub mod config {
    #[derive(Clone, Debug)]
    pub struct DatabaseConfig {
        pub path: std::path::PathBuf,
    }

    impl DatabaseConfig {
        pub fn local(path: std::path::PathBuf) -> Self {
            Self { path }
        }
    }
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Serialization error: {0}")]
    SerializationError(String),
    #[error("Storage backend error: {0}")]
    BackendError(String),
    #[error("read resource limit exceeded: {detail}")]
    ReadResourceLimit { detail: String },
    #[error("write resource limit exceeded: {detail}")]
    WriteResourceLimit { detail: String },
    #[error("complete mutation limit exceeded: {detail}")]
    MutationLimit { detail: String },
    #[error("the final record is {size} bytes; the cap is {limit} bytes")]
    RecordTooLarge { size: usize, limit: usize },
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
    #[error("share publication supports inline files only")]
    PublicationFile,
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("no space left on device — the filesystem holding the database is full, so the write did not land. Free disk space and retry; nothing already stored was lost. Underlying error: {detail}")]
    StorageFull { detail: String },
    #[error("Encryption error: {0}")]
    EncryptionError(String),
    #[error("retired format: {0}")]
    RetiredFormat(&'static str),
}

impl StorageError {
    pub(in crate::storage) fn read_limit(detail: impl Into<String>) -> Self {
        Self::ReadResourceLimit {
            detail: detail.into(),
        }
    }

    pub(in crate::storage) fn write_limit(detail: impl Into<String>) -> Self {
        Self::WriteResourceLimit {
            detail: detail.into(),
        }
    }

    pub(in crate::storage) fn mutation_limit(detail: impl Into<String>) -> Self {
        Self::MutationLimit {
            detail: detail.into(),
        }
    }

    pub(in crate::storage) fn backend(detail: impl Into<String>) -> Self {
        Self::BackendError(detail.into())
    }

    pub(in crate::storage) fn invalid(detail: impl Into<String>) -> Self {
        Self::InvalidOperation(detail.into())
    }

    pub(in crate::storage) fn is_storage_full_io(error: &std::io::Error) -> bool {
        #[cfg(unix)]
        if error.raw_os_error() == Some(libc::ENOSPC) {
            return true;
        }
        error.kind() == ErrorKind::StorageFull
    }

    pub(in crate::storage) fn from_laststore(error: laststore::Error) -> Self {
        use laststore::Error as E;
        let detail = error.to_string();
        match error {
            E::Io(io) if Self::is_storage_full_io(&io) => Self::StorageFull { detail },
            E::Io(io) => Self::IoError(io),
            E::ReadBudgetExceeded => Self::read_limit(detail),
            E::WriteCapacity(detail) => Self::write_limit(detail),
            E::AeadAuthFail => Self::EncryptionError(detail),
            E::Corrupt(_) | E::Config(_) | E::HomeBusy(_) => Self::backend(detail),
        }
    }
}

impl From<tokio::task::JoinError> for StorageError {
    fn from(error: tokio::task::JoinError) -> Self {
        Self::backend(error.to_string())
    }
}

impl<T> From<std::sync::PoisonError<T>> for StorageError {
    fn from(error: std::sync::PoisonError<T>) -> Self {
        Self::backend(format!("Lock poisoned: {error}"))
    }
}

pub type StorageResult<T> = Result<T, StorageError>;

impl From<StorageError> for SchemaError {
    fn from(error: StorageError) -> Self {
        use StorageError::*;
        match error {
            RecordTooLarge { size, limit } => Self::RecordTooLarge { size, limit },
            ReadResourceLimit { detail } => Self::ReadResourceLimit { detail },
            WriteResourceLimit { detail } | MutationLimit { detail } => {
                Self::WriteResourceLimit { detail }
            }
            StorageFull { detail } => Self::StorageFull { detail },
            IoError(io) if StorageError::is_storage_full_io(&io) => Self::StorageFull {
                detail: io.to_string(),
            },
            other => Self::InvalidData(other.to_string()),
        }
    }
}
