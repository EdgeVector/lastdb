//! The Mini format and retained source of a byte backup of a disk format v2
//! home: its run files, unchanged. Wire identities and saved capture records
//! are independent of the cloud protocol's own version names.
mod cancel;
mod capture;
mod codec;
mod counters;
mod model;
mod planner;
mod reader;
mod run_capture;
mod run_codec;
mod run_source;
mod store;
mod terminal;

use super::{PackRuntime, StorageError, StorageResult};
use cloud_backup_protocol::v2::{
    transfer::{ObjectReceipt, TransferInput},
    Begin,
};
use cloud_backup_protocol::{Digest, ProtocolError};
pub use codec::*;
pub use reader::ByteBackupReader;
pub use run_codec::*;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub struct ByteBackupObject {
    pub input: TransferInput,
    pub receipt: Option<ObjectReceipt>,
}
#[derive(Clone, Debug)]
pub struct ByteBackupCaptureStatus {
    pub ready: bool,
    pub source_available: bool,
    pub native_files: u64,
    pub native_bytes: u64,
    pub objects: u64,
    pub verified_objects: u64,
    pub retained_source_bytes: u64,
    pub metadata_spool_bytes: u64,
    pub metadata_pack_objects: u64,
    pub multi_file_pack_objects: u64,
}
pub struct ByteBackupCapture {
    inner: Arc<Inner>,
}
struct Inner {
    runtime: Arc<PackRuntime>,
    store: store::Store,
    state: Mutex<model::State>,
    /// The held run files; none after release or local cancel.
    run: Option<run_source::RunSource>,
    readers: std::sync::atomic::AtomicUsize,
    _admission: super::NativeCloudWorkGuard,
}
impl ByteBackupCapture {
    pub fn mutation_log_anchor(&self) -> StorageResult<Option<super::mutation_log::LogAnchor>> {
        Ok(self.lock()?.journal_anchor.clone())
    }
    /// A run file read holds no lookup buffer, so every stream of the central
    /// backup allowance is available.
    pub fn recommended_parallelism(&self) -> StorageResult<usize> {
        Ok(lastdb_storage_policy::BACKUP_RESOURCES.concurrent_streams)
    }
    pub fn begin(&self) -> StorageResult<Begin> {
        Ok(self.lock()?.begin.clone())
    }
    pub fn object_count(&self) -> StorageResult<u64> {
        let state = self.lock()?;
        self.require_export(&state)?;
        Ok(state.objects)
    }
    pub fn status(&self) -> StorageResult<ByteBackupCaptureStatus> {
        let state = self.lock()?;
        self.require_export(&state)?;
        Ok(ByteBackupCaptureStatus {
            ready: state.ready
                && !state.released
                && !state.local_cancelled
                && state.terminal.is_none(),
            source_available: self.inner.available() && !state.released,
            native_files: state.native_files,
            native_bytes: state.native_bytes,
            objects: state.objects,
            verified_objects: state.verified,
            retained_source_bytes: if state.released {
                0
            } else {
                state.native_bytes
            },
            metadata_spool_bytes: self.inner.store.bytes(),
            metadata_pack_objects: state.metadata_pack_objects,
            multi_file_pack_objects: state.multi_file_pack_objects,
        })
    }
    pub fn object(&self, index: u64) -> StorageResult<ByteBackupObject> {
        let state = self.lock()?;
        self.require_export(&state)?;
        let object = self.inner.object(&state, index)?;
        Ok(ByteBackupObject {
            input: object.input,
            receipt: object.receipt,
        })
    }
    pub fn confirm(&self, index: u64, receipt: &ObjectReceipt) -> StorageResult<()> {
        let mut state = self.lock()?;
        self.inner.require_idle()?;
        if !state.ready || state.terminal.is_some() || state.local_cancelled {
            return Err(invalid("byte source confirmation phase"));
        }
        let mut object = self.inner.object(&state, index)?;
        model::check_receipt(&object.input, receipt)?;
        if object
            .receipt
            .as_ref()
            .is_some_and(|prior| prior != receipt)
        {
            return Err(invalid("byte source receipt changed"));
        }
        object.receipt = Some(receipt.clone());
        self.inner
            .store
            .write(&model::object_name(index), &object)?;
        self.inner.advance(&mut state)
    }
    pub fn root_receipt_id(&self) -> StorageResult<Digest> {
        let state = self.lock()?;
        if let Some(cloud_backup_protocol::v2::jobs::JobResult {
            status: cloud_backup_protocol::v2::jobs::JobStatus::Published { backup },
            ..
        }) = &state.terminal
        {
            return Ok(backup.root_receipt_id.clone());
        }
        if !state.ready {
            return Err(invalid("byte source is not ready"));
        }
        planner::expected_receipt(&self.inner.object(&state, state.objects - 1)?.input)
            .map(|r| r.receipt_id)
    }
    pub fn set_coordinator_spool_bytes(&self, bytes: u64) -> StorageResult<()> {
        self.inner.store.coordinator(bytes)
    }
    fn lock(&self) -> StorageResult<std::sync::MutexGuard<'_, model::State>> {
        self.inner
            .state
            .lock()
            .map_err(|_| invalid("byte source state lock"))
    }
    fn require_export(&self, state: &model::State) -> StorageResult<()> {
        if !state.released && !state.local_cancelled && state.terminal.is_none() {
            counters::require_validated(&self.inner)?;
        }
        Ok(())
    }
}
impl Inner {
    fn require_idle(&self) -> StorageResult<()> {
        if self.readers.load(std::sync::atomic::Ordering::Acquire) != 0 {
            return Err(invalid("byte source readers are active"));
        }
        Ok(())
    }
    fn object(&self, state: &model::State, index: u64) -> StorageResult<model::ObjectRecord> {
        if index >= state.objects {
            return Err(invalid("byte source object index"));
        }
        let object: model::ObjectRecord = self.store.read(&model::object_name(index))?;
        object.validate(&state.begin)?;
        if object
            .ciphertext
            .as_ref()
            .is_some_and(|name| *name != format!("payload-{index:08}.enc"))
        {
            return Err(invalid("byte source ciphertext coordinate"));
        }
        Ok(object)
    }
    fn advance(&self, state: &mut model::State) -> StorageResult<()> {
        if state.released {
            return Ok(());
        }
        while state.verified < state.objects
            && self.object(state, state.verified)?.receipt.is_some()
        {
            state.verified += 1;
        }
        state.validate()?;
        self.store.write("job.json", state)
    }
}
fn invalid(reason: &str) -> StorageError {
    StorageError::InvalidOperation(reason.into())
}
fn protocol(_error: ProtocolError) -> StorageError {
    invalid("byte protocol validation")
}
