//! Durable bounded assembly of the existing native cloud manifest protocol.
//! No network call occurs here. Authenticated service results remain caller inputs.
mod content;
mod index;
mod pages;
mod receipts;
mod references;
mod state;
mod store;
mod upload;

use super::backup_capture::{AssemblySelection, BackupCapturePage, NativeBackupCapture};
use super::{StorageError, StorageResult};
use cloud_backup_protocol::jobs::JobResult;
use cloud_backup_protocol::{bytes_sha256, ProtocolError};
use state::{Phase, State};
use store::Store;
pub use upload::FrozenUpload;

pub struct ManifestAssembly<'a> {
    capture: &'a NativeBackupCapture,
    key: zeroize::Zeroizing<[u8; 32]>,
    state: State,
    store: Store,
    cache: Option<BackupCapturePage>,
}

impl<'a> ManifestAssembly<'a> {
    pub fn open(capture: &'a NativeBackupCapture, content_key: &[u8; 32]) -> StorageResult<Self> {
        let selection = capture.assembly_selection()?;
        let mut store = Store::open(capture.assembly_directory()?)?;
        capture.track_assembly_metadata(store.bytes())?;
        let key_sha256 = bytes_sha256(content_key);
        let state = if store.exists("state.json")? {
            let state: State = store.read("state.json", store::CONTROL_MAX)?;
            state.validate()?;
            if state.selection != selection || state.key_sha256 != key_sha256 {
                return Err(invalid(
                    "manifest assembly selection or encryption key changed",
                ));
            }
            state
        } else {
            State::new(selection, key_sha256)
        };
        store.write(capture, "state.json", &state, store::CONTROL_MAX)?;
        capture.admit_assembly_metadata(store.bytes())?;
        Ok(Self {
            capture,
            key: zeroize::Zeroizing::new(*content_key),
            state,
            store,
            cache: None,
        })
    }

    pub fn manifest_page_count(&self) -> usize {
        self.state.manifest_pages as usize
    }
    /// Public reference pages precede their index at the final ordinal.
    pub fn reference_count(&self) -> usize {
        if self.state.phase >= Phase::References {
            self.state.reference_pages as usize + 1
        } else {
            0
        }
    }
    pub fn spool_bytes(&self) -> u64 {
        self.store.bytes()
    }

    fn save(&mut self) -> StorageResult<()> {
        self.state.validate()?;
        self.store
            .write(self.capture, "state.json", &self.state, store::CONTROL_MAX)
    }
}

/// The owner first saves an authoritative terminal result. This cleanup never
/// touches native files, the capture hold, or another job's assembly directory.
pub fn cleanup_after_terminal(
    capture: &NativeBackupCapture,
    result: &JobResult,
) -> StorageResult<()> {
    result
        .validate_for_begin(&capture.frozen_begin()?)
        .map_err(protocol)?;
    if !matches!(
        result.status,
        cloud_backup_protocol::jobs::JobStatus::Published { .. }
            | cloud_backup_protocol::jobs::JobStatus::Closed { .. }
    ) {
        return Err(invalid("manifest cleanup requires a terminal job"));
    }
    store::cleanup(&capture.assembly_directory()?)?;
    capture.track_assembly_metadata(0)
}

fn invalid(message: &str) -> StorageError {
    StorageError::InvalidOperation(message.into())
}
fn protocol(error: ProtocolError) -> StorageError {
    let ProtocolError { code, field } = error;
    invalid(&format!("manifest protocol {code:?}: {field}"))
}
