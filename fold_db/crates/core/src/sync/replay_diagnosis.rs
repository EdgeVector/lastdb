//! Closed replay diagnostics. Private error text never enters this wire type.

use crate::schema::types::SchemaError;
use crate::sync::log::LogOp;
use crate::sync::SyncError;
use serde::Serialize;

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayOperation {
    Put,
    Delete,
    BatchPut,
    BatchDelete,
    LogicalCommit,
    MutationIntent,
    Unknown,
}

impl From<&LogOp> for ReplayOperation {
    fn from(op: &LogOp) -> Self {
        match op {
            LogOp::Put { .. } => Self::Put,
            LogOp::Delete { .. } => Self::Delete,
            LogOp::BatchPut { .. } => Self::BatchPut,
            LogOp::BatchDelete { .. } => Self::BatchDelete,
            LogOp::LogicalCommit { .. } => Self::LogicalCommit,
            LogOp::MutationIntent { .. } => Self::MutationIntent,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayCause {
    SchemaNotFound,
    SchemaBlocked,
    InvalidField,
    InvalidPermission,
    InvalidTransform,
    InvalidData,
    DeleteBarrierChanged,
    PermissionDenied,
    CatalogMembershipDenied,
    TransportNotAttested,
    TransformGasExceeded,
    TransformCallDepthExceeded,
    CasConflict,
    AtomContentTooLarge,
    InvalidCursor,
    StorageFull,
    PersistQueueFull,
    CaptureQueueFull,
    MaterializationFailed,
    MissingMutationApplier,
    LegacyMoleculeDelete,
    RawDeleteBarrierPut,
    DeleteBarrierIdentityMismatch,
    Unknown,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct ReplayApplyDiagnosis {
    pub operation: ReplayOperation,
    pub cause: ReplayCause,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub atom_content_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub atom_limit_bytes: Option<usize>,
}

impl ReplayApplyDiagnosis {
    pub fn new(operation: ReplayOperation, cause: ReplayCause) -> Self {
        Self {
            operation,
            cause,
            atom_content_bytes: None,
            atom_limit_bytes: None,
        }
    }
}

/// The callback retains private text separately from the safe diagnosis.
#[derive(Debug)]
pub struct MutationIntentReplayError {
    pub reason: String,
    pub diagnosis: ReplayApplyDiagnosis,
}

impl MutationIntentReplayError {
    pub fn materialization(reason: String) -> Self {
        Self {
            reason,
            diagnosis: ReplayApplyDiagnosis::new(
                ReplayOperation::MutationIntent,
                ReplayCause::MaterializationFailed,
            ),
        }
    }
}

impl From<SchemaError> for MutationIntentReplayError {
    fn from(error: SchemaError) -> Self {
        let cause = match &error {
            SchemaError::NotFound(_) => ReplayCause::SchemaNotFound,
            SchemaError::Blocked(_) => ReplayCause::SchemaBlocked,
            SchemaError::InvalidField(_) => ReplayCause::InvalidField,
            SchemaError::InvalidPermission(_) => ReplayCause::InvalidPermission,
            SchemaError::InvalidTransform(_) => ReplayCause::InvalidTransform,
            SchemaError::InvalidData(_) => ReplayCause::InvalidData,
            SchemaError::ReplayDeleteBarrierChanged => ReplayCause::DeleteBarrierChanged,
            SchemaError::PermissionDenied(_) => ReplayCause::PermissionDenied,
            SchemaError::CatalogMembershipDenied { .. } => ReplayCause::CatalogMembershipDenied,
            SchemaError::TransportNotAttested { .. } => ReplayCause::TransportNotAttested,
            SchemaError::TransformGasExceeded { .. } => ReplayCause::TransformGasExceeded,
            SchemaError::TransformCallDepthExceeded { .. } => {
                ReplayCause::TransformCallDepthExceeded
            }
            SchemaError::CasConflict { .. } => ReplayCause::CasConflict,
            SchemaError::AtomContentTooLarge { .. } => ReplayCause::AtomContentTooLarge,
            SchemaError::InvalidCursor(_) => ReplayCause::InvalidCursor,
            SchemaError::StorageFull { .. } => ReplayCause::StorageFull,
            SchemaError::PersistQueueFull { .. } => ReplayCause::PersistQueueFull,
            SchemaError::CaptureQueueFull { .. } => ReplayCause::CaptureQueueFull,
        };
        let mut diagnosis = ReplayApplyDiagnosis::new(ReplayOperation::MutationIntent, cause);
        if let SchemaError::AtomContentTooLarge { size, limit } = &error {
            diagnosis.atom_content_bytes = Some(*size);
            diagnosis.atom_limit_bytes = Some(*limit);
        }
        Self {
            reason: error.to_string(),
            diagnosis,
        }
    }
}

impl SyncError {
    pub(crate) fn replay_refused(
        target: impl Into<String>,
        seq: u64,
        operation: ReplayOperation,
        cause: ReplayCause,
    ) -> Self {
        let reason = match cause {
            ReplayCause::LegacyMoleculeDelete => {
                "legacy physical molecule Delete needs a safe recovery path"
            }
            ReplayCause::RawDeleteBarrierPut => "raw Delete barrier Put needs a safe recovery path",
            ReplayCause::MissingMutationApplier => "no MutationIntent applier registered",
            ReplayCause::DeleteBarrierIdentityMismatch => {
                "durable Delete barrier key identity differs from the molecule key"
            }
            _ => "replay policy refused the operation",
        };
        Self::ReplayApplyFailed {
            target: target.into(),
            seq,
            reason: reason.into(),
            diagnosis: ReplayApplyDiagnosis::new(operation, cause),
        }
    }

    pub(crate) fn with_replay_operation(self, replay_seq: u64, operation: ReplayOperation) -> Self {
        match self {
            Self::ReplayApplyFailed {
                target,
                seq,
                reason,
                mut diagnosis,
            } => {
                diagnosis.operation = operation;
                Self::ReplayApplyFailed {
                    target,
                    seq: if seq == 0 { replay_seq } else { seq },
                    reason,
                    diagnosis,
                }
            }
            other => other,
        }
    }
}
