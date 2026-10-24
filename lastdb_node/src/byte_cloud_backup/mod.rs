//! Explicit V2 byte backup. The native format remains a client responsibility.
mod control;
mod credentials;
mod invocation;
mod model;
mod publication;
mod retry;
mod source;
mod store;
mod transfer;
mod wait;
mod worker;

pub use crate::native_cloud_backup::{BackupError, BackupRuntime};
pub use control::request;
pub use credentials::client_from_home;
pub use model::{ByteBackupRequest, ByteBackupStatus, Phase};
pub type Result<T> = std::result::Result<T, BackupError>;

pub fn require_enabled() -> Result<()> {
    if env_flag::var_truthy("LASTDB_BYTE_BACKUP_V2_ENABLED") {
        Ok(())
    } else {
        Err(BackupError::Disabled)
    }
}
