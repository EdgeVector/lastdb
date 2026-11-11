//! The Mini format and retained source of a byte backup of a disk format v2
//! home: its run files, unchanged.

mod capture;
mod codec;
mod hold;
mod planner;
mod run_capture;
mod run_source;
mod store;

use crate::storage::cloud_backup_plan::ClientNativeFile;
use crate::storage::disk_pack::PackRuntime;
use crate::storage::error::{StorageError, StorageResult};
use cloud_backup_protocol::v2::{
    jobs::{JobResult, JobStatus},
    transfer::{ObjectReceipt, TransferInput},
    Begin,
};
use cloud_backup_protocol::{Digest, NativeRoot, ProtocolError, RecoveryDescriptor, RelativePath};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

/// The backup capture source inside [`laststore::disk_run::home::BACKUP_DIR`].
pub const SOURCE_DIR: &str = "source";
/// The run holds inside [`laststore::disk_run::home::BACKUP_DIR`], one directory per paired capture.
pub const HOLDS_DIR: &str = "holds";
/// The saved capture intent inside [`SOURCE_DIR`].
pub const RUN_INTENT_FILE: &str = "run-intent.json";
/// The hard link to the node marker inside one run hold.
pub const HELD_NODE_FILE: &str = "node";

pub use codec::*;
pub use store::ByteBackupReader;

pub(in crate::storage::disk_backup::byte_backup) const STATE_FORMAT: u16 = 5;
pub(in crate::storage::disk_backup::byte_backup) const MAX_OBJECTS: u64 =
    (cloud_backup_protocol::v2::limits::MAX_PAGE_ENTRIES
        * cloud_backup_protocol::v2::limits::MAX_PAGES) as u64;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::storage::disk_backup::byte_backup) struct State {
    pub format: u16,
    pub begin: Begin,
    pub reference_id: uuid::Uuid,
    pub native_root: NativeRoot,
    pub key_sha256: Digest,
    pub ready: bool,
    pub objects: u64,
    pub verified: u64,
    pub native_files: u64,
    pub native_bytes: u64,
    pub metadata_pack_objects: u64,
    pub multi_file_pack_objects: u64,
    pub terminal: Option<JobResult>,
    pub local_cancelled: bool,
    pub released: bool,
    pub recovery: RecoveryDescriptor,
    pub run: RunBackupProfile,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal_anchor: Option<super::mutation_log::LogAnchor>,
}

impl State {
    pub(in crate::storage::disk_backup::byte_backup) fn validate(&self) -> StorageResult<()> {
        self.begin.validate().map_err(protocol)?;
        let run = &self.run;
        run.validate_root(&self.native_root)?;
        if let Some(anchor) = &self.journal_anchor {
            anchor.position()?;
            if anchor.main_home != run.main.home_id
                || anchor.frontiers.main != run.main.frontier.get()
                || run.cas.is_some()
            {
                return Err(invalid("snapshot mutation log anchor differs"));
            }
        }
        if self.format != STATE_FORMAT
            || self.reference_id.is_nil()
            || self.objects > MAX_OBJECTS
            || self.verified > self.objects
            || self.multi_file_pack_objects > self.metadata_pack_objects
            || self.metadata_pack_objects > self.objects
            || (self.ready
                && (self.objects < 2 || self.native_files == 0 || self.native_bytes == 0))
        {
            return Err(invalid("byte source state bounds"));
        }
        if !self.begin.descriptor.as_value().is_null() {
            return Err(invalid("byte source recovery profile differs"));
        }
        self.recovery
            .validate_for(&self.begin.job.stream.scope)
            .map_err(protocol)?;
        if let Some(result) = &self.terminal {
            result.validate_for_begin(&self.begin).map_err(protocol)?;
            if matches!(result.status, JobStatus::Active) {
                return Err(invalid("byte source terminal active"));
            }
        } else if self.released && !self.local_cancelled {
            return Err(invalid("byte source release without terminal"));
        }
        if self.local_cancelled && self.terminal.is_some() {
            return Err(invalid("byte source cancel authority differs"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::storage::disk_backup::byte_backup) struct Segment {
    pub path: RelativePath,
    pub offset: u64,
    pub bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::storage::disk_backup::byte_backup) struct ObjectRecord {
    pub input: TransferInput,
    pub segments: Vec<Segment>,
    pub native_files: Vec<ClientNativeFile>,
    pub metadata_pack: bool,
    pub ciphertext: Option<String>,
    pub receipt: Option<ObjectReceipt>,
}

impl ObjectRecord {
    pub(in crate::storage::disk_backup::byte_backup) fn validate(
        &self,
        begin: &Begin,
    ) -> StorageResult<()> {
        self.input.validate().map_err(protocol)?;
        if self.input.job != begin.job
            || self.segments.len()
                > lastdb_model::storage_policy::backup_transport::MAX_PACK_MEMBERS
            || self.segments.is_empty() != self.ciphertext.is_some()
            || (self.metadata_pack && self.segments.is_empty())
            || (!self.metadata_pack && self.segments.len() > 1)
        {
            return Err(invalid("byte source object identity"));
        }
        let mut total = 0u64;
        for part in &self.segments {
            if part.bytes == 0 || part.offset.checked_add(part.bytes).is_none() {
                return Err(invalid("byte source segment bounds"));
            }
            total = total
                .checked_add(part.bytes)
                .ok_or_else(|| invalid("byte source segment overflow"))?;
        }
        if !self.segments.is_empty() && total != self.input.object.bytes.get() {
            return Err(invalid("byte source object exact bytes"));
        }
        for file in &self.native_files {
            file.validate()?;
        }
        if let Some(receipt) = &self.receipt {
            check_receipt(&self.input, receipt)?;
        }
        Ok(())
    }
}

pub(in crate::storage::disk_backup::byte_backup) fn check_receipt(
    input: &TransferInput,
    receipt: &ObjectReceipt,
) -> StorageResult<()> {
    receipt.validate().map_err(protocol)?;
    if receipt.scope != input.job.stream.scope || receipt.object != input.object {
        return Err(invalid("byte source verified receipt identity"));
    }
    Ok(())
}

pub(in crate::storage::disk_backup::byte_backup) fn object_name(index: u64) -> String {
    format!("object-{index:08}.json")
}

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

pub(in crate::storage::disk_backup::byte_backup) struct Inner {
    pub(in crate::storage::disk_backup::byte_backup) runtime: Arc<PackRuntime>,
    pub(in crate::storage::disk_backup::byte_backup) store: store::Store,
    pub(in crate::storage::disk_backup::byte_backup) state: Mutex<State>,
    pub(in crate::storage::disk_backup::byte_backup) run: Option<run_source::RunSource>,
    pub(in crate::storage::disk_backup::byte_backup) readers: std::sync::atomic::AtomicUsize,
    pub(in crate::storage::disk_backup::byte_backup) _admission: super::NativeCloudWorkGuard,
}

impl ByteBackupCapture {
    pub fn run_profile(&self) -> StorageResult<RunBackupProfile> {
        Ok(self.lock()?.run.clone())
    }
    pub fn mutation_log_anchor(&self) -> StorageResult<Option<super::mutation_log::LogAnchor>> {
        Ok(self.lock()?.journal_anchor.clone())
    }
    pub fn recommended_parallelism(&self) -> StorageResult<usize> {
        Ok(lastdb_model::storage_policy::BACKUP_RESOURCES.concurrent_streams)
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
        check_receipt(&object.input, receipt)?;
        if object
            .receipt
            .as_ref()
            .is_some_and(|prior| prior != receipt)
        {
            return Err(invalid("byte source receipt changed"));
        }
        object.receipt = Some(receipt.clone());
        self.inner.store.write(&object_name(index), &object)?;
        self.inner.advance(&mut state)
    }
    pub fn root_receipt_id(&self) -> StorageResult<Digest> {
        let state = self.lock()?;
        if let Some(JobResult {
            status: JobStatus::Published { backup },
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
    pub(in crate::storage::disk_backup::byte_backup) fn lock(
        &self,
    ) -> StorageResult<std::sync::MutexGuard<'_, State>> {
        self.inner
            .state
            .lock()
            .map_err(|_| invalid("byte source state lock"))
    }
    pub(in crate::storage::disk_backup::byte_backup) fn require_export(
        &self,
        state: &State,
    ) -> StorageResult<()> {
        if !state.released && !state.local_cancelled && state.terminal.is_none() {
            self.inner.require_available()?;
        }
        Ok(())
    }
}

impl Inner {
    pub(in crate::storage::disk_backup::byte_backup) fn require_idle(&self) -> StorageResult<()> {
        if self.readers.load(std::sync::atomic::Ordering::Acquire) != 0 {
            return Err(invalid("byte source readers are active"));
        }
        Ok(())
    }
    pub(in crate::storage::disk_backup::byte_backup) fn object(
        &self,
        state: &State,
        index: u64,
    ) -> StorageResult<ObjectRecord> {
        if index >= state.objects {
            return Err(invalid("byte source object index"));
        }
        let object: ObjectRecord = self.store.read(&object_name(index))?;
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
    pub(in crate::storage::disk_backup::byte_backup) fn advance(
        &self,
        state: &mut State,
    ) -> StorageResult<()> {
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

pub(in crate::storage::disk_backup::byte_backup) fn invalid(reason: &str) -> StorageError {
    StorageError::InvalidOperation(reason.into())
}

pub(in crate::storage::disk_backup::byte_backup) fn protocol(
    _error: ProtocolError,
) -> StorageError {
    invalid("byte protocol validation")
}
