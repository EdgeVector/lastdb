use std::fmt;
use std::io::ErrorKind;

use thiserror::Error;

use crate::schema::types::SchemaError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeProofStage {
    NativeReader,
    Admission,
    Validation,
    Restore,
    Profile,
    PhysicalRoot,
    Account,
    Schemas,
    Directories,
    NativeCas,
    Records,
    FinalBinding,
}

impl fmt::Display for NativeProofStage {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str(match self {
            Self::NativeReader => "native_reader",
            Self::Admission => "admission",
            Self::Validation => "validation",
            Self::Restore => "restore",
            Self::Profile => "profile",
            Self::PhysicalRoot => "physical_root",
            Self::Account => "account",
            Self::Schemas => "schemas",
            Self::Directories => "directories",
            Self::NativeCas => "native_cas",
            Self::Records => "records",
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
    ReadResource,
    WriteResource,
    StorageFull,
    Authentication,
    NativeFormat,
    Configuration,
    InvalidOperation,
    Encryption,
    Backend,
    Other,
}

impl fmt::Display for NativeProofCause {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Static(code) => out.write_str(code),
            Self::Io { kind, os_code } => write!(out, "io kind={kind:?} os_code={os_code:?}"),
            other => out.write_str(match other {
                Self::ReadResource => "read_resource_limit",
                Self::WriteResource => "write_resource_limit",
                Self::StorageFull => "storage_full",
                Self::Authentication => "authentication",
                Self::NativeFormat => "native_format",
                Self::Configuration => "configuration",
                Self::InvalidOperation => "invalid_operation",
                Self::Encryption => "encryption",
                Self::Backend => "backend",
                _ => "other",
            }),
        }
    }
}

/// Comprehensive storage error type supporting multiple backends
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Storage backend error: {0}")]
    BackendError(String),

    /// Closed stage/category diagnostics or a fixed source literal, never raw data.
    #[error("native proof {stage}: {cause}")]
    NativeProof {
        stage: NativeProofStage,
        cause: NativeProofCause,
    },

    /// One bounded disk call exhausted its work limit. This is never a missing key.
    #[error("read resource limit exceeded: {detail}")]
    ReadResourceLimit { detail: String },
    /// Complete native admission failed before any append. Retry after capacity returns.
    #[error("write resource limit exceeded: {detail}")]
    WriteResourceLimit { detail: String },

    /// One complete mutation exceeds a fixed operation or encoded-byte cap.
    /// The caller must reduce the request; waiting for capacity cannot help.
    #[error("complete mutation limit exceeded: {detail}")]
    MutationLimit { detail: String },

    /// The final stored record exceeds the logical format's byte cap.
    #[error("the final record is {size} bytes; the cap is {limit} bytes")]
    RecordTooLarge { size: usize, limit: usize },

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("snapshot is waiting for native compaction")]
    SnapshotCompactionPending,

    #[error("share publication is waiting for an unfinished edit")]
    PublicationEditPending,

    #[error("share publication supports inline files only")]
    PublicationFile,

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// The filesystem holding the database is out of space, so the write did
    /// not land.
    #[error(
        "no space left on device — the filesystem holding the database is \
         full, so the write did not land. Free disk space and retry; nothing \
         already stored was lost. Underlying error: {detail}"
    )]
    StorageFull { detail: String },

    #[error("Configuration error: {0}")]
    ConfigurationError(String),

    #[error("Encryption error: {0}")]
    EncryptionError(String),

    /// A dedicated shared home or portable journal key from the retired format.
    #[error("retired format: {0}")]
    RetiredFormat(&'static str),
}

/// `ENOSPC`. Identical on Linux and macOS/BSD.
const ENOSPC: i32 = 28;

impl StorageError {
    /// True when `error` reports that the filesystem is out of space.
    #[must_use]
    pub fn is_storage_full_io(error: &std::io::Error) -> bool {
        error.kind() == ErrorKind::StorageFull || error.raw_os_error() == Some(ENOSPC)
    }

    /// Preserve the nearest concrete proof stage and any typed static leaf.
    pub fn native_proof_context(
        &self,
        stage: NativeProofStage,
    ) -> (NativeProofStage, NativeProofCause) {
        let cause = match self {
            Self::NativeProof {
                stage: prior,
                cause,
            } => {
                let stage = if matches!(
                    prior,
                    NativeProofStage::Validation | NativeProofStage::NativeReader
                ) {
                    stage
                } else {
                    *prior
                };
                return (stage, *cause);
            }
            Self::IoError(io) => NativeProofCause::Io {
                kind: io.kind(),
                os_code: io.raw_os_error(),
            },
            Self::ReadResourceLimit { .. } => NativeProofCause::ReadResource,
            Self::WriteResourceLimit { .. } | Self::MutationLimit { .. } => {
                NativeProofCause::WriteResource
            }
            Self::StorageFull { .. } => NativeProofCause::StorageFull,
            Self::BackendError(_) => NativeProofCause::Backend,
            Self::ConfigurationError(_) => NativeProofCause::Configuration,
            Self::InvalidOperation(_) => NativeProofCause::InvalidOperation,
            Self::EncryptionError(_) => NativeProofCause::Encryption,
            _ => NativeProofCause::Other,
        };
        (stage, cause)
    }

    pub fn at_native_proof(self, stage: NativeProofStage) -> Self {
        let (stage, cause) = self.native_proof_context(stage);
        drop(self);
        Self::NativeProof { stage, cause }
    }
}

impl From<tokio::task::JoinError> for StorageError {
    fn from(e: tokio::task::JoinError) -> Self {
        Self::BackendError(e.to_string())
    }
}

impl<T> From<std::sync::PoisonError<T>> for StorageError {
    fn from(e: std::sync::PoisonError<T>) -> Self {
        Self::BackendError(format!("Lock poisoned: {e}"))
    }
}

pub type StorageResult<T> = Result<T, StorageError>;

impl From<StorageError> for SchemaError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::RecordTooLarge { size, limit } => Self::RecordTooLarge { size, limit },
            StorageError::ReadResourceLimit { detail } => Self::ReadResourceLimit { detail },
            StorageError::WriteResourceLimit { detail }
            | StorageError::MutationLimit { detail } => Self::WriteResourceLimit { detail },
            StorageError::StorageFull { detail } => Self::StorageFull { detail },
            StorageError::IoError(io) if StorageError::is_storage_full_io(&io) => {
                Self::StorageFull {
                    detail: io.to_string(),
                }
            }
            other => Self::InvalidData(other.to_string()),
        }
    }
}
