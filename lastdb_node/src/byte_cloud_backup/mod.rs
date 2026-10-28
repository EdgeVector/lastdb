//! Explicit V2 byte backup. The native format remains a client responsibility.
mod automatic;
mod capture;
mod connection;
mod control;
mod credentials;
mod invocation;
mod log_capture;
mod model;
mod publication;
mod recovery;
mod retry;
mod source;
mod stopped;
mod store;
mod transfer;
mod wait;
mod work;
mod worker;

pub use crate::backup_runtime::{BackupError, BackupRuntime};
pub use automatic::{spawn_automatic, AutomaticBackupStatus};
pub use connection::{connect_existing, ManualConnectionStatus, SensitiveConnectionRequest};
pub use control::request;
pub(crate) use control::status;
pub use credentials::client_from_home;
pub use model::{ByteBackupRequest, ByteBackupStatus, Phase};
pub use stopped::{backup_stopped, StoppedBackupReport, StoppedBackupRequest};
pub type Result<T> = std::result::Result<T, BackupError>;

pub fn require_enabled() -> Result<()> {
    if env_flag::var_truthy("LASTDB_BYTE_BACKUP_V2_ENABLED") {
        Ok(())
    } else {
        Err(BackupError::Disabled)
    }
}
