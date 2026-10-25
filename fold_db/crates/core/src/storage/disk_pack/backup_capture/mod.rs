//! Frozen retired capture metadata. No capture, manifest assembly or provider source exists.
mod state;
use super::{StorageError, StorageResult};
pub use state::{BackupCapturePhase, ExportRecord as LegacyCaptureRecord, PageDescriptor};
const PAGE_MAX: u64 = 2 * 1024 * 1024;
fn invalid(detail: &str) -> StorageError {
    StorageError::InvalidOperation(format!("disk-pack v1: {detail}"))
}
fn protocol(error: cloud_backup_protocol::ProtocolError) -> StorageError {
    invalid(&error.to_string())
}
