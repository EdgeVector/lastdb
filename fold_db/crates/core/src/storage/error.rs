use crate::schema::types::SchemaError;
use std::fmt;
use std::io::ErrorKind;
use thiserror::Error;

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
            Self::Io { kind, os_code } => write!(out, "io kind={kind:?} os_code={os_code:?}"),
            Self::Static(code) => out.write_str(code),
            Self::ReadResource => out.write_str("read_resource_limit"),
            Self::WriteResource => out.write_str("write_resource_limit"),
            Self::StorageFull => out.write_str("storage_full"),
            Self::Authentication => out.write_str("authentication"),
            Self::NativeFormat => out.write_str("native_format"),
            Self::Configuration => out.write_str("configuration"),
            Self::InvalidOperation => out.write_str("invalid_operation"),
            Self::Encryption => out.write_str("encryption"),
            Self::Backend => out.write_str("backend"),
            Self::Other => out.write_str("other"),
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
    #[error("snapshot is waiting for native compaction")]
    SnapshotCompactionPending,
    #[error("share publication is waiting for an unfinished edit")]
    PublicationEditPending,
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
    #[must_use]
    pub fn is_storage_full_io(error: &std::io::Error) -> bool {
        error.kind() == ErrorKind::StorageFull || error.raw_os_error() == Some(28)
    }

    pub fn native_proof_context(
        &self,
        stage: NativeProofStage,
    ) -> (NativeProofStage, NativeProofCause) {
        if let Self::NativeProof { stage: p, cause } = self {
            let keep = !matches!(
                p,
                NativeProofStage::Validation | NativeProofStage::NativeReader
            );
            return (if keep { *p } else { stage }, *cause);
        }
        use NativeProofCause as C;
        let cause = match self {
            Self::IoError(io) => C::Io {
                kind: io.kind(),
                os_code: io.raw_os_error(),
            },
            Self::ReadResourceLimit { .. } => C::ReadResource,
            Self::WriteResourceLimit { .. } | Self::MutationLimit { .. } => C::WriteResource,
            Self::StorageFull { .. } => C::StorageFull,
            Self::BackendError(_) => C::Backend,
            Self::InvalidOperation(_) => C::InvalidOperation,
            Self::EncryptionError(_) => C::Encryption,
            _ => C::Other,
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
