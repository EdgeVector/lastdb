//! Explicit personal backup jobs. Legacy logical cloud sync remains separate.
//! The owner saves intent before a process-lifetime worker starts. A lost
//! response or process exit resumes the same native cut and transfer identities.
mod automatic;
mod content;
mod control;
mod error;
mod identity;
mod invocation;
mod model;
mod publication;
mod retry;
mod runtime;
mod setup;
mod source_diagnostic;
mod status;
mod store;
mod terminal;
mod transfer;
mod usage;
mod v2_cleanup;
mod worker;
pub(crate) use v2_cleanup::require_complete as require_v1_complete;

pub use automatic::{request as request_auto, spawn as spawn_automatic, AutoRequest, AutoStatus};
pub use control::request;
pub use error::{BackupError, Disposition, PauseReason, PauseState};
use fold_db::sync::auth::native_backup::NativeBackupError;
pub use model::{NativeBackupRequest, NativeBackupStatus, Phase};
pub use runtime::{
    BackupRuntime, Completion, DrainError, Invocation, InvocationOutcome, RunOrigin,
    RuntimeSnapshot,
};

pub fn require_enabled() -> Result<()> {
    if env_flag::var_truthy("LASTDB_NATIVE_BACKUP_ENABLED") {
        Ok(())
    } else {
        Err(BackupError::Disabled)
    }
}

pub type Result<T> = std::result::Result<T, BackupError>;
