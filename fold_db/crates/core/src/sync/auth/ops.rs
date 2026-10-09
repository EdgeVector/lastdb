//! AuthClient high-level operations: list, presign, metering confirm, locks.

use crate::sync::error::SyncError;

mod backup_latest;
pub use backup_latest::{
    BackupLatestCasResponse, BackupLatestGetResponse, BackupLatestPointer,
    BackupRecoveryDescriptorPut, BACKUP_LATEST_FORMAT_VERSION_V1,
};
mod confirm;
mod guaranteed_write;
pub use guaranteed_write::{
    GuaranteedWriteCasResponse, GuaranteedWriteGetResponse, GuaranteedWritePointer,
    GuaranteedWriteSetCasResponse, GuaranteedWriteSetGetResponse, GuaranteedWriteSetGrant,
    GuaranteedWriteSetHead, GuaranteedWriteSetMember, GuaranteedWriteSetSlot,
    GuaranteedWriteSetSlotState,
};
mod list;
pub use list::LogListStats;
mod lock;
mod photograph_latest;
pub mod presign;
pub mod register_db;
mod rescue_s0;
pub use rescue_s0::{
    RescueS0CommitOutcome, RescueS0CommitResponse, RescueS0Identity, RescueS0Pointer,
    RescueS0PrepareResponse, RescueS0WaitReason,
};

fn op_failed(action: &str, detail: Option<String>) -> SyncError {
    SyncError::Storage(format!(
        "{action}: {}",
        detail.unwrap_or_else(|| "operation failed".to_string())
    ))
}
