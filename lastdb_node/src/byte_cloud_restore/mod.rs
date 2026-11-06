//! Offline restore of an exact accepted opaque-byte backup.
//! The cloud authorizes bytes; this client verifies all native semantics,
//! then installs and publishes the verified home. No V1 cloud selection,
//! read grant or downloader is admitted here.
mod closure;
mod context;
mod download;
mod file_proof;
mod inventory;
mod keys;
pub(crate) mod local;
mod logs;
mod native;
pub(crate) mod publish;
mod report;
mod runs;
mod session;
mod spool;

pub use keys::RecoveryKeys;
pub use report::RestoreReport;
pub use session::RestoreSession;

use cloud_backup_client::byte_backup::ByteBackupClient;
use context::{NativeFileProof, NativeInventory, VerifiedNativeContext};
pub(crate) use file_proof::check_capacity;
use file_proof::{bounded_bytes, verify_file};

/// Static errors never contain keys, record values, provider URLs, or local secrets.
#[derive(Debug)]
pub enum RestoreError {
    Invalid(&'static str),
    Limit(&'static str),
    Io(std::io::Error),
    Protocol(cloud_backup_protocol::ProtocolError),
    Source(&'static str),
    SharedStage {
        stage: &'static str,
        cause: Box<Self>,
    },
    SharedWorkspace {
        path: std::path::PathBuf,
        cause: Box<Self>,
    },
    NativeProof {
        stage: fold_db::storage::error::NativeProofStage,
        cause: fold_db::storage::error::NativeProofCause,
    },
}
pub type Result<T> = std::result::Result<T, RestoreError>;

impl From<std::io::Error> for RestoreError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}
impl From<cloud_backup_protocol::ProtocolError> for RestoreError {
    fn from(value: cloud_backup_protocol::ProtocolError) -> Self {
        Self::Protocol(value)
    }
}
impl RestoreError {
    pub(crate) fn shared_stage(self, stage: &'static str) -> Self {
        Self::SharedStage {
            stage,
            cause: Box::new(self),
        }
    }

    pub(crate) fn native_proof(error: fold_db::storage::StorageError) -> Self {
        let (stage, cause) =
            error.native_proof_context(fold_db::storage::error::NativeProofStage::Restore);
        drop(error);
        Self::NativeProof { stage, cause }
    }
}
impl std::fmt::Display for RestoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(field) => write!(f, "invalid restore {field}"),
            Self::Limit(field) => write!(f, "restore limit: {field}"),
            Self::Io(_) => f.write_str("restore disk operation failed"),
            Self::Protocol(_) => f.write_str("restore protocol validation failed"),
            Self::Source(field) => write!(f, "restore source unavailable: {field}"),
            Self::SharedStage { stage, cause } => write!(f, "shared restore {stage}: {cause}"),
            Self::SharedWorkspace { path, cause } => {
                write!(
                    f,
                    "shared restore retained workspace {}: {cause}",
                    path.display()
                )
            }
            Self::NativeProof { stage, cause } => {
                write!(f, "native restore proof {stage}: {cause}")
            }
        }
    }
}
impl std::error::Error for RestoreError {}

fn source_error(_: cloud_backup_client::byte_backup::BackupTransportError) -> RestoreError {
    RestoreError::Source("byte transport")
}
