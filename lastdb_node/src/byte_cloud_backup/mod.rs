//! Explicit V2 byte backup. The native format remains a client responsibility.
mod automatic;
pub(crate) mod automatic_publication;
mod blob_source;
mod blobs;
mod capture;
pub(crate) mod connection;
mod control;
mod credentials;
mod invocation;
mod log_capture;
mod model;
mod publication;
mod recovery;
mod retry;
pub mod share_write_gate;
mod source;
mod store;
mod transfer;
mod wait;
mod work;
mod worker;

pub use crate::backup_runtime::{BackupError, BackupRuntime};
pub use automatic::{spawn_automatic, AutomaticBackupStatus};
pub use blob_source::register_blob_source;
pub use connection::{connect_existing, ManualConnectionStatus, SensitiveConnectionRequest};
pub use control::request;
pub(crate) use control::status;
pub use credentials::client_from_home;
pub use model::{ByteBackupRequest, ByteBackupStatus, Phase};
pub(crate) use source::PublicationSource;
pub type Result<T> = std::result::Result<T, BackupError>;

pub const PUBLICATION_MARKER: &str = "share-publication.json";

pub(crate) fn is_publication_home(home: &std::path::Path) -> std::io::Result<bool> {
    match std::fs::symlink_metadata(home.join(PUBLICATION_MARKER)) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

pub fn require_enabled() -> Result<()> {
    if fold_db::env_flag::var_truthy("LASTDB_BYTE_BACKUP_V2_ENABLED") {
        Ok(())
    } else {
        Err(BackupError::Disabled)
    }
}
