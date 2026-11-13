//! Storage error types, native proof stages, and database path configuration.

use crate::schema::types::SchemaError;
use std::fmt;
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeProofStage {
    NativeReader,
    Validation,
    Restore,
    Profile,
    PhysicalRoot,
    Account,
    FinalBinding,
}

impl fmt::Display for NativeProofStage {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str(match self {
            Self::NativeReader => "native_reader",
            Self::Validation => "validation",
            Self::Restore => "restore",
            Self::Profile => "profile",
            Self::PhysicalRoot => "physical_root",
            Self::Account => "account",
            Self::FinalBinding => "final_binding",
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeProofCause {
    Static(&'static str),
    Io {
        kind: ErrorKind,
        os_code: Option<i32>,
    },
    Authentication,
    NativeFormat,
    Configuration,
}

impl fmt::Display for NativeProofCause {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { kind, os_code } => write!(out, "io kind={kind:?} os_code={os_code:?}"),
            Self::Static(code) => out.write_str(code),
            Self::Authentication => out.write_str("authentication"),
            Self::NativeFormat => out.write_str("native_format"),
            Self::Configuration => out.write_str("configuration"),
        }
    }
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Serialization error: {0}")]
    SerializationError(String),
    #[error("Storage backend error: {0}")]
    BackendError(String),
    #[error("native proof {stage}: {cause}")]
    NativeProof {
        stage: NativeProofStage,
        cause: NativeProofCause,
    },
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
    pub(in crate::storage) fn read_limit(d: impl Into<String>) -> Self {
        Self::ReadResourceLimit { detail: d.into() }
    }
    pub(in crate::storage) fn write_limit(d: impl Into<String>) -> Self {
        Self::WriteResourceLimit { detail: d.into() }
    }
    pub(in crate::storage) fn mutation_limit(d: impl Into<String>) -> Self {
        Self::MutationLimit { detail: d.into() }
    }
    pub(in crate::storage) fn backend(d: impl Into<String>) -> Self {
        Self::BackendError(d.into())
    }
    pub(in crate::storage) fn invalid(d: impl Into<String>) -> Self {
        Self::InvalidOperation(d.into())
    }
    pub(in crate::storage) fn is_storage_full_io(e: &std::io::Error) -> bool {
        e.kind() == ErrorKind::StorageFull || e.raw_os_error() == Some(28)
    }
    pub fn native_proof_context(
        &self,
        stage: NativeProofStage,
    ) -> (NativeProofStage, NativeProofCause) {
        use NativeProofCause::{Io, Static};
        use NativeProofStage::{NativeReader, Validation};
        use StorageError::*;
        if let NativeProof { stage: p, cause } = self {
            return (
                if matches!(p, Validation | NativeReader) {
                    stage
                } else {
                    *p
                },
                *cause,
            );
        }
        let cause = match self {
            IoError(io) => Io {
                kind: io.kind(),
                os_code: io.raw_os_error(),
            },
            ReadResourceLimit { .. } => Static("read_resource_limit"),
            WriteResourceLimit { .. } | MutationLimit { .. } => Static("write_resource_limit"),
            StorageFull { .. } => Static("storage_full"),
            BackendError(_) => Static("backend"),
            InvalidOperation(_) => Static("invalid_operation"),
            EncryptionError(_) => Static("encryption"),
            _ => Static("other"),
        };
        (stage, cause)
    }
    pub fn at_native_proof(self, stage: NativeProofStage) -> Self {
        let (stage, cause) = self.native_proof_context(stage);
        Self::NativeProof { stage, cause }
    }
}

impl From<tokio::task::JoinError> for StorageError {
    fn from(e: tokio::task::JoinError) -> Self {
        Self::backend(e.to_string())
    }
}
impl<T> From<std::sync::PoisonError<T>> for StorageError {
    fn from(e: std::sync::PoisonError<T>) -> Self {
        Self::backend(format!("Lock poisoned: {e}"))
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
