//! Scoped guest permissions and share environment bindings.
mod guest_permission;

pub use super::BackupTransportError;
pub use guest_permission::{GuestReadPermission, ShareEnvironment};
pub type Result<T> = std::result::Result<T, BackupTransportError>;
