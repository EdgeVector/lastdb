//! Explicit personal backup jobs. Legacy logical cloud sync remains separate.
//! The owner saves intent before a process-lifetime worker starts. A lost
//! response or process exit resumes the same native cut and transfer identities.
mod content;
mod control;
mod identity;
mod model;
mod publication;
mod setup;
mod source_diagnostic;
mod store;
mod terminal;
mod transfer;
mod usage;
mod worker;

pub use control::request;
use fold_db::sync::auth::native_backup::NativeBackupError;
pub use model::{NativeBackupRequest, NativeBackupStatus, Phase};

pub fn require_enabled() -> Result<()> {
    if env_flag::var_truthy("LASTDB_NATIVE_BACKUP_ENABLED") {
        Ok(())
    } else {
        Err(BackupError::Disabled)
    }
}

#[derive(Debug)]
pub enum BackupError {
    Disabled,
    Busy,
    Missing,
    Identity,
    Invalid,
    Disk,
    Source,
    UnsupportedFileScope,
    FileSourceUnavailable,
    UncertifiedFile,
    Limit,
    Waiting,
    Closing,
    Cloud(NativeBackupError),
}
pub type Result<T> = std::result::Result<T, BackupError>;
impl BackupError {
    fn label(&self) -> &'static str {
        match self {
            Self::Disabled => "native backup awaits release activation",
            Self::Busy => "a native backup job is active",
            Self::Missing => "no saved native backup job exists",
            Self::Identity => "native backup account or home differs",
            Self::Invalid => "native backup saved state differs",
            Self::Disk => "native backup disk operation failed",
            Self::Source => "native backup source needs attention",
            Self::UnsupportedFileScope => {
                "native backup needs a read-only grant for the original file"
            }
            Self::UncertifiedFile => "native backup needs complete file pointers and certificates",
            Self::FileSourceUnavailable => "native backup has no existing authorized file copy; the saved job retains its source",
            Self::Limit => "native backup resource limit reached",
            Self::Waiting => "native backup awaits cloud verification; resume this job",
            Self::Closing => "native backup closure is requested",
            Self::Cloud(error) => cloud_label(error),
        }
    }
}
fn cloud_label(error: &NativeBackupError) -> &'static str {
    use cloud_backup_protocol::ErrorCode;
    match error {
        NativeBackupError::Authentication => "native backup credentials were refused",
        NativeBackupError::Protocol(error) => match error.code {
            ErrorCode::QuotaExceeded => {
                "native backup cloud quota reached; the saved job retains its source"
            }
            ErrorCode::ResourceLimit => "native backup cloud resource limit reached",
            ErrorCode::Unauthorized
            | ErrorCode::GenerationConflict
            | ErrorCode::StreamAuthorityConflict
            | ErrorCode::PredecessorConflict => {
                "native backup cloud authority changed; close or review this job"
            }
            ErrorCode::IntegrityMismatch
            | ErrorCode::IdentityConflict
            | ErrorCode::RecoveryKeyMismatch => {
                "native backup cloud identity or byte proof differs"
            }
            ErrorCode::ProviderPending | ErrorCode::OutcomeUnknown | ErrorCode::RetryLater => {
                "native backup awaits cloud verification; resume this job"
            }
            ErrorCode::UnsupportedFormat
            | ErrorCode::UnsupportedKeyMode
            | ErrorCode::UnsupportedCapability => {
                "native backup cloud format or key mode is unsupported"
            }
            _ => "native backup cloud request was refused; the saved job retains its source",
        },
        NativeBackupError::Integrity(_) => "native backup upload bytes differ from the saved input",
        _ => "native backup cloud request failed; resume this job",
    }
}
impl std::fmt::Display for BackupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}
impl std::error::Error for BackupError {}
impl From<NativeBackupError> for BackupError {
    fn from(value: NativeBackupError) -> Self {
        Self::Cloud(value)
    }
}
impl From<std::io::Error> for BackupError {
    fn from(_: std::io::Error) -> Self {
        Self::Disk
    }
}
impl From<cloud_backup_protocol::ProtocolError> for BackupError {
    fn from(_: cloud_backup_protocol::ProtocolError) -> Self {
        Self::Invalid
    }
}
impl From<fold_db::storage::StorageError> for BackupError {
    fn from(error: fold_db::storage::StorageError) -> Self {
        use fold_db::storage::StorageError;
        source_diagnostic::report(&error);
        match error {
            StorageError::ReadResourceLimit { .. }
            | StorageError::WriteResourceLimit { .. }
            | StorageError::MutationLimit { .. } => Self::Limit,
            StorageError::StorageFull { .. } | StorageError::IoError(_) => Self::Disk,
            StorageError::InvalidOperation(detail) => match detail.as_str() {
                "native authorized file consumer copy is unavailable" => {
                    Self::FileSourceUnavailable
                }
                "native file source owner scope needs a read-only grant" => {
                    Self::UnsupportedFileScope
                }
                "legacy or incomplete file dependency is uncertified"
                | "metadata-only file dependency is uncertified"
                | "live file metadata lacks certified atom pointer coverage"
                | "native CAS per-file encryption suite is uncertified" => Self::UncertifiedFile,
                _ => Self::Source,
            },
            _ => Self::Source,
        }
    }
}
