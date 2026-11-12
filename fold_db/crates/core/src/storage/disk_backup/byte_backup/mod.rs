//! The Mini format and retained source of a byte backup of a disk format v2 home.

mod capture;
mod codec;
mod planner;
mod run_capture;
mod store;

use crate::storage::cloud_backup_plan::{expected_receipt, PlanSegment};
use crate::storage::disk_pack::PackRuntime;
use crate::storage::error::{StorageError, StorageResult};
use cloud_backup_protocol::v2::{
    jobs::{JobResult, JobStatus},
    limits::{MAX_PAGES, MAX_PAGE_ENTRIES},
    transfer::{ObjectReceipt, TransferInput},
    Begin,
};
use cloud_backup_protocol::{Digest, NativeRoot, ProtocolError, RecoveryDescriptor};
use serde::{Deserialize, Serialize};
use std::sync::{atomic::AtomicUsize, atomic::Ordering, Arc, Mutex, MutexGuard};

pub use codec::*;

pub(super) const RUN_INTENT_FILE: &str = "run-intent.json";
pub(super) const HELD_NODE_FILE: &str = "node";
pub(super) const STATE_FORMAT: u16 = 6;
pub(super) const MAX_OBJECTS: u64 = (MAX_PAGE_ENTRIES * MAX_PAGES) as u64;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    format: u16,
    begin: Begin,
    reference_id: uuid::Uuid,
    native_root: NativeRoot,
    key_sha256: Digest,
    ready: bool,
    objects: u64,
    verified: u64,
    native_files: u64,
    native_bytes: u64,
    metadata_pack_objects: u64,
    multi_file_pack_objects: u64,
    terminal: Option<JobResult>,
    local_cancelled: bool,
    released: bool,
    recovery: RecoveryDescriptor,
    run: RunBackupProfile,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    journal_anchor: Option<super::mutation_log::LogAnchor>,
}

impl State {
    fn validate(&self) -> StorageResult<()> {
        self.begin.validate().map_err(protocol)?;
        self.run.validate_root(&self.native_root)?;
        if let Some(a) = &self.journal_anchor {
            a.position()?;
            if a.main_home != self.run.main.home_id
                || a.frontiers.main != self.run.main.frontier.get()
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
            || !self.begin.descriptor.as_value().is_null()
            || (self.local_cancelled && self.terminal.is_some())
        {
            return Err(invalid("byte source state bounds"));
        }
        self.recovery
            .validate_for(&self.begin.job.stream.scope)
            .map_err(protocol)?;
        match &self.terminal {
            Some(res) => {
                res.validate_for_begin(&self.begin).map_err(protocol)?;
                if matches!(res.status, JobStatus::Active) {
                    return Err(invalid("byte source terminal active"));
                }
            }
            None if self.released && !self.local_cancelled => {
                return Err(invalid("byte source release without terminal"));
            }
            None => {}
        }
        Ok(())
    }

    fn is_active(&self) -> bool {
        self.terminal.is_none() && !self.local_cancelled && !self.released
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectRecord {
    input: TransferInput,
    segments: Vec<PlanSegment>,
    ciphertext: Option<String>,
    receipt: Option<ObjectReceipt>,
}

impl ObjectRecord {
    fn validate(&self, state: &State) -> StorageResult<()> {
        self.input.validate().map_err(protocol)?;
        if self.input.job != state.begin.job
            || self.segments.is_empty() != self.ciphertext.is_some()
        {
            return Err(invalid("byte source object identity"));
        }
        let total = crate::storage::cloud_backup_plan::sum_segments(&self.segments, |i| {
            ((i as u64) < state.native_files).then_some(u64::MAX)
        })
        .ok_or_else(|| invalid("byte source segment bounds"))?;
        if !self.segments.is_empty() && total != self.input.object.bytes.get() {
            return Err(invalid("byte source object exact bytes"));
        }
        self.receipt
            .as_ref()
            .map_or(Ok(()), |r| check_receipt(&self.input, r))
    }
}

fn check_receipt(input: &TransferInput, receipt: &ObjectReceipt) -> StorageResult<()> {
    receipt.validate().map_err(protocol)?;
    if receipt.scope != input.job.stream.scope || receipt.object != input.object {
        return Err(invalid("byte source verified receipt identity"));
    }
    Ok(())
}

fn object_name(index: u64) -> String {
    format!("object-{index:08}.json")
}

#[derive(Clone, Debug)]
pub struct ByteBackupObject {
    pub input: TransferInput,
    pub receipt: Option<ObjectReceipt>,
}

#[derive(Clone, Debug)]
pub struct ByteBackupCaptureStatus {
    pub native_files: u64,
    pub native_bytes: u64,
    pub verified_objects: u64,
    pub retained_source_bytes: u64,
    pub metadata_pack_objects: u64,
    pub multi_file_pack_objects: u64,
}

pub struct ByteBackupCapture {
    inner: Arc<Inner>,
}

struct Inner {
    runtime: Arc<PackRuntime>,
    store: store::Store,
    state: Mutex<State>,
    run: Option<planner::RunSource>,
    readers: AtomicUsize,
    _admission: super::NativeCloudWorkGuard,
}

impl ByteBackupCapture {
    pub fn run_profile(&self) -> StorageResult<RunBackupProfile> {
        Ok(self.lock()?.run.clone())
    }
    pub fn mutation_log_anchor(&self) -> StorageResult<Option<super::mutation_log::LogAnchor>> {
        Ok(self.lock()?.journal_anchor.clone())
    }
    pub fn begin(&self) -> StorageResult<Begin> {
        Ok(self.lock()?.begin.clone())
    }
    pub fn object_count(&self) -> StorageResult<u64> {
        let s = self.lock()?;
        self.require_export(&s).map(|()| s.objects)
    }
    pub fn status(&self) -> StorageResult<ByteBackupCaptureStatus> {
        let s = self.lock()?;
        self.require_export(&s)?;
        Ok(ByteBackupCaptureStatus {
            native_files: s.native_files,
            native_bytes: s.native_bytes,
            verified_objects: s.verified,
            retained_source_bytes: if s.released { 0 } else { s.native_bytes },
            metadata_pack_objects: s.metadata_pack_objects,
            multi_file_pack_objects: s.multi_file_pack_objects,
        })
    }
    pub fn object(&self, index: u64) -> StorageResult<ByteBackupObject> {
        let s = self.lock()?;
        self.require_export(&s)?;
        let obj = self.inner.object(&s, index)?;
        Ok(ByteBackupObject {
            input: obj.input,
            receipt: obj.receipt,
        })
    }
    pub fn confirm(&self, index: u64, receipt: &ObjectReceipt) -> StorageResult<()> {
        let mut s = self.lock()?;
        self.inner.require_idle()?;
        if !s.ready || !s.is_active() {
            return Err(invalid("byte source confirmation phase"));
        }
        let mut obj = self.inner.object(&s, index)?;
        check_receipt(&obj.input, receipt)?;
        if obj.receipt.as_ref().is_some_and(|prior| prior != receipt) {
            return Err(invalid("byte source receipt changed"));
        }
        obj.receipt = Some(receipt.clone());
        self.inner.store.write(&object_name(index), &obj)?;
        self.inner.advance(&mut s)
    }
    pub fn root_receipt_id(&self) -> StorageResult<Digest> {
        let s = self.lock()?;
        if let Some(JobResult {
            status: JobStatus::Published { backup },
            ..
        }) = &s.terminal
        {
            return Ok(backup.root_receipt_id.clone());
        }
        if !s.ready {
            return Err(invalid("byte source is not ready"));
        }
        expected_receipt(&self.inner.object(&s, s.objects - 1)?.input)
            .map(|r| r.receipt_id)
            .map_err(protocol)
    }
    pub fn set_coordinator_spool_bytes(&self, bytes: u64) -> StorageResult<()> {
        self.inner.store.coordinator(bytes)
    }
    fn lock(&self) -> StorageResult<MutexGuard<'_, State>> {
        self.inner
            .state
            .lock()
            .map_err(|_| invalid("byte source state lock"))
    }
    fn require_export(&self, state: &State) -> StorageResult<()> {
        (!state.is_active() || self.inner.run.is_some())
            .then_some(())
            .ok_or_else(|| invalid("byte retained source unavailable"))
    }
}

impl Inner {
    fn new(
        runtime: Arc<PackRuntime>,
        store: store::Store,
        state: State,
        run: Option<planner::RunSource>,
        _admission: super::NativeCloudWorkGuard,
    ) -> Self {
        Self {
            runtime,
            store,
            state: Mutex::new(state),
            run,
            readers: AtomicUsize::new(0),
            _admission,
        }
    }
    fn require_idle(&self) -> StorageResult<()> {
        (self.readers.load(Ordering::Acquire) == 0)
            .then_some(())
            .ok_or_else(|| invalid("byte source readers are active"))
    }
    fn object(&self, state: &State, index: u64) -> StorageResult<ObjectRecord> {
        if index >= state.objects {
            return Err(invalid("byte source object index"));
        }
        let obj: ObjectRecord = self.store.read(&object_name(index))?;
        obj.validate(state)?;
        if obj
            .ciphertext
            .as_ref()
            .is_some_and(|n| *n != format!("payload-{index:08}.enc"))
        {
            return Err(invalid("byte source ciphertext coordinate"));
        }
        Ok(obj)
    }
    fn advance(&self, state: &mut State) -> StorageResult<()> {
        if !state.released {
            while state.verified < state.objects
                && self.object(state, state.verified)?.receipt.is_some()
            {
                state.verified += 1;
            }
            state.validate()?;
            self.store.write("job.json", state)?;
        }
        Ok(())
    }
}

fn invalid(reason: &str) -> StorageError {
    StorageError::InvalidOperation(reason.into())
}

fn protocol(_error: ProtocolError) -> StorageError {
    invalid("byte protocol validation")
}
