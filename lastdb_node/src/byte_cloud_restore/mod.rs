//! Offline restore of an exact accepted opaque-byte backup.
//! The cloud authorizes bytes; this client verifies all native semantics.
mod closure;
mod download;
mod inventory;
mod local;
mod logs;
mod native;
mod runs;
mod session;
mod spool;

pub use crate::native_cloud_restore::{RecoveryKeys, RestoreError, RestoreReport};
pub use session::RestoreSession;
pub type Result<T> = std::result::Result<T, RestoreError>;

use crate::native_cloud_restore::{NativeFileProof, NativeInventory, VerifiedNativeContext};
use cloud_backup_client::byte_backup::ByteBackupClient;

fn source_error(_: cloud_backup_client::byte_backup::BackupTransportError) -> RestoreError {
    RestoreError::Source("byte transport")
}
