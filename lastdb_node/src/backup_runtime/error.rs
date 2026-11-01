use super::source_diagnostic;
use cloud_backup_client::byte_backup::BackupTransportError;
use fold_db::storage::error::NativeProofCause;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Disposition {
    Retry,
    Attention,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PauseReason {
    Temporary,
    Disabled,
    Busy,
    Missing,
    Identity,
    Invalid,
    Disk,
    Source,
    Unsupported,
    ResourceLimit,
    Quota,
    Credentials,
    Authority,
    Refused,
    Closing,
    Shutdown,
    WorkerPanic,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PauseState {
    pub disposition: Disposition,
    pub reason: PauseReason,
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
    Shutdown,
    WorkerPanic,
    Authority,
    Cloud(BackupTransportError),
}
impl BackupError {
    pub(super) fn label(&self) -> &'static str {
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
            Self::Shutdown => "native backup paused for daemon shutdown",
            Self::WorkerPanic => "native backup worker needs attention",
            Self::Authority => "native backup cloud authority needs attention",
            Self::Cloud(error) => cloud_label(error),
        }
    }
}
fn cloud_label(error: &BackupTransportError) -> &'static str {
    use cloud_backup_protocol::ErrorCode;
    match error {
        BackupTransportError::Authentication => "native backup credentials were refused",
        BackupTransportError::Protocol(error) => match error.code {
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
        BackupTransportError::Integrity(_) => {
            "native backup upload bytes differ from the saved input"
        }
        _ => "native backup cloud request failed; resume this job",
    }
}
impl std::fmt::Display for BackupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}
impl std::error::Error for BackupError {}
impl From<BackupTransportError> for BackupError {
    fn from(value: BackupTransportError) -> Self {
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
impl From<cloud_backup_plan::PlanError> for BackupError {
    fn from(error: cloud_backup_plan::PlanError) -> Self {
        use cloud_backup_plan::PlanError;
        match error {
            PlanError::SourceLength => Self::Identity,
            PlanError::ExtentBound => Self::Limit,
            PlanError::Io(error) => Self::from(error),
            PlanError::Invalid(_) | PlanError::Protocol(_) => Self::Invalid,
        }
    }
}
impl From<fold_db::storage::StorageError> for BackupError {
    fn from(error: fold_db::storage::StorageError) -> Self {
        use fold_db::storage::StorageError;
        source_diagnostic::report(&error);
        match error {
            StorageError::NativeProof { cause, .. } => Self::from(cause),
            StorageError::ReadResourceLimit { .. }
            | StorageError::WriteResourceLimit { .. }
            | StorageError::MutationLimit { .. } => Self::Limit,
            StorageError::StorageFull { .. } | StorageError::IoError(_) => Self::Disk,
            StorageError::InvalidOperation(detail) => {
                Self::from(NativeProofCause::invalid_operation(&detail))
            }
            _ => Self::Source,
        }
    }
}

impl From<NativeProofCause> for BackupError {
    fn from(cause: NativeProofCause) -> Self {
        match cause {
            NativeProofCause::ReadResource | NativeProofCause::WriteResource => Self::Limit,
            NativeProofCause::Io { .. } | NativeProofCause::StorageFull => Self::Disk,
            NativeProofCause::FileSourceUnavailable => Self::FileSourceUnavailable,
            NativeProofCause::UnsupportedFileScope => Self::UnsupportedFileScope,
            NativeProofCause::UncertifiedFile => Self::UncertifiedFile,
            NativeProofCause::Static(code) => Self::from(NativeProofCause::invalid_operation(code)),
            _ => Self::Source,
        }
    }
}

impl BackupError {
    pub fn pause_state(&self) -> PauseState {
        use cloud_backup_protocol::ErrorCode;
        use PauseReason as P;
        let reason = match self {
            Self::Disabled => P::Disabled,
            Self::Busy => P::Busy,
            Self::Missing => P::Missing,
            Self::Identity => P::Identity,
            Self::Invalid => P::Invalid,
            Self::Disk => P::Disk,
            Self::Source | Self::FileSourceUnavailable | Self::UncertifiedFile => P::Source,
            Self::UnsupportedFileScope => P::Unsupported,
            Self::Limit => P::ResourceLimit,
            Self::Waiting => P::Temporary,
            Self::Closing => P::Closing,
            Self::Shutdown => P::Shutdown,
            Self::WorkerPanic => P::WorkerPanic,
            Self::Authority => P::Authority,
            Self::Cloud(error) => match error {
                BackupTransportError::Transport | BackupTransportError::Timeout => P::Temporary,
                BackupTransportError::Http(code)
                    if *code == 408 || *code == 429 || *code >= 500 =>
                {
                    P::Temporary
                }
                BackupTransportError::Authentication => P::Credentials,
                BackupTransportError::Protocol(error) => match error.code {
                    ErrorCode::ProviderPending
                    | ErrorCode::OutcomeUnknown
                    | ErrorCode::RetryLater => P::Temporary,
                    ErrorCode::QuotaExceeded => P::Quota,
                    ErrorCode::ResourceLimit => P::ResourceLimit,
                    ErrorCode::Unauthorized
                    | ErrorCode::GenerationConflict
                    | ErrorCode::StreamAuthorityConflict
                    | ErrorCode::PredecessorConflict => P::Authority,
                    ErrorCode::IntegrityMismatch
                    | ErrorCode::IdentityConflict
                    | ErrorCode::RecoveryKeyMismatch => P::Identity,
                    ErrorCode::UnsupportedFormat
                    | ErrorCode::UnsupportedKeyMode
                    | ErrorCode::UnsupportedCapability => P::Unsupported,
                    _ => P::Refused,
                },
                BackupTransportError::Source | BackupTransportError::Task => P::Source,
                BackupTransportError::UnsupportedFileScope => P::Unsupported,
                BackupTransportError::Integrity(_) | BackupTransportError::InvalidResponse(_) => {
                    P::Identity
                }
                BackupTransportError::Configuration(_) => P::Invalid,
                BackupTransportError::Http(_) => P::Refused,
            },
        };
        PauseState {
            disposition: if matches!(reason, P::Temporary | P::Busy | P::Shutdown) {
                Disposition::Retry
            } else {
                Disposition::Attention
            },
            reason,
        }
    }
}
