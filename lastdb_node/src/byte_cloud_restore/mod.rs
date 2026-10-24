//! Offline restore of an exact accepted opaque-byte backup.
//! The cloud authorizes bytes; this client verifies all native semantics.
mod closure;
mod dependencies;
mod download;
mod inventory;
mod local;
mod native;
mod session;
mod spool;

pub use crate::native_cloud_restore::{RecoveryKeys, RestoreError, RestoreReport};
pub use session::RestoreSession;
pub type Result<T> = std::result::Result<T, RestoreError>;

use crate::native_cloud_restore::{NativeFileProof, NativeInventory, VerifiedNativeContext};
use fold_db::sync::auth::byte_backup::ByteBackupClient;

fn source_error(_: fold_db::sync::auth::native_backup::NativeBackupError) -> RestoreError {
    RestoreError::Source("byte transport")
}
