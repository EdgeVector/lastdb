//! The Mini format and retained source of a byte backup of a disk format v2 home.
mod codec;
mod planner;
mod run_capture;
mod store;

use crate::storage::cloud_backup_plan::{PlanError, PlanSegment, PlannedRecord};
use crate::storage::disk_backup::NativeCloudWorkGuard;
use crate::storage::disk_pack::PackRuntime;
use crate::storage::error::{StorageError, StorageResult};
use cloud_backup_protocol::v2::{
    jobs::{JobResult, JobStatus},
    limits::{MAX_PAGES, MAX_PAGE_ENTRIES},
    transfer::ObjectReceipt,
    Begin,
};
use cloud_backup_protocol::{bytes_sha256, Digest, NativeRoot, ProtocolError, RecoveryDescriptor};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fs::File;
use std::io::{self, Read};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex, MutexGuard,
};

pub use codec::*;
pub use planner::seal_pages;

pub(super) const RUN_INTENT_FILE: &str = "run-intent.json";
pub(super) const HELD_NODE_FILE: &str = "node";
pub(super) const STATE_FORMAT: u16 = 6;
pub(super) const MAX_OBJECTS: u64 = (MAX_PAGE_ENTRIES * MAX_PAGES) as u64;
pub(super) type PublicationGate<'a> = &'a (dyn Fn() -> StorageResult<File> + Sync);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct State {
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
    pub(super) fn validate(&self) -> StorageResult<()> {
        self.begin.validate().map_err(protocol)?;
        self.run.validate_root(&self.native_root)?;
        self.recovery
            .validate_for(&self.begin.job.stream.scope)
            .map_err(protocol)?;
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
        match &self.terminal {
            Some(res) => {
                res.validate_for_begin(&self.begin).map_err(protocol)?;
                (!matches!(res.status, JobStatus::Active))
                    .then_some(())
                    .ok_or_else(|| invalid("byte source terminal active"))
            }
            None if self.released && !self.local_cancelled => {
                Err(invalid("byte source release without terminal"))
            }
            None => Ok(()),
        }
    }

    pub(super) fn is_active(&self) -> bool {
        self.terminal.is_none() && !self.local_cancelled && !self.released
    }
}

pub(super) fn object_name(index: u64) -> String {
    format!("object-{index:08}.json")
}
pub(super) fn payload_name(index: u64) -> String {
    format!("payload-{index:08}.enc")
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
    _admission: NativeCloudWorkGuard,
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
    pub fn object(&self, index: u64) -> StorageResult<PlannedRecord> {
        let s = self.lock()?;
        self.require_export(&s)
            .and_then(|()| self.inner.object(&s, index))
    }
    pub fn confirm(&self, index: u64, receipt: &ObjectReceipt) -> StorageResult<()> {
        let mut s = self.lock()?;
        self.inner.require_idle()?;
        if !s.ready || !s.is_active() {
            return Err(invalid("byte source confirmation phase"));
        }
        let mut obj = self.inner.object(&s, index)?;
        obj.confirm(receipt)?;
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
        s.ready
            .then_some(())
            .ok_or_else(|| invalid("byte source is not ready"))
            .and_then(|()| self.inner.expected_root_id(&s))
    }
    pub fn set_coordinator_spool_bytes(&self, bytes: u64) -> StorageResult<()> {
        self.inner.store.coordinator(bytes)
    }
    pub fn terminal(&self, result: &JobResult) -> StorageResult<()> {
        let mut state = self.lock()?;
        self.inner.require_idle()?;
        result.validate_for_begin(&state.begin).map_err(protocol)?;
        if state.local_cancelled || matches!(result.status, JobStatus::Active) {
            return Err(invalid("byte source terminal invalid"));
        }
        if let Some(prior) = &state.terminal {
            if prior != result {
                return Err(invalid("byte source terminal result changed"));
            }
        } else {
            if let JobStatus::Published { backup } = &result.status {
                self.inner.advance(&mut state)?;
                if !state.ready
                    || state.verified != state.objects
                    || backup.root_receipt_id != self.inner.expected_root_id(&state)?
                {
                    return Err(invalid("byte accepted result does not cover source"));
                }
            }
            state.terminal = Some(result.clone());
            if let Err(e) = self.inner.store.write("job.json", &*state) {
                state.terminal = None;
                return Err(e);
            }
        }
        drop(state);
        self.finish_terminal()
    }
    pub(super) fn finish_terminal(&self) -> StorageResult<()> {
        let mut state = self.lock()?;
        if state.terminal.is_none() && !state.local_cancelled {
            return Ok(());
        }
        self.inner.require_idle()?;
        if !state.released {
            if let (
                Some(a),
                Some(JobResult {
                    status: JobStatus::Published { .. },
                    ..
                }),
            ) = (&state.journal_anchor, &state.terminal)
            {
                self.inner.runtime.mutation_log.snapshot_accepted(a)?;
            }
            let held = planner::held_root(&self.inner.runtime, state.reference_id);
            planner::RunSource::release_saved(&held)?;
            self.inner.store.remove(RUN_INTENT_FILE)?;
            state.released = true;
            if let Err(e) = self.inner.store.write("job.json", &*state) {
                state.released = false;
                return Err(e);
            }
        }
        planner::clear_objects(&self.inner.store, state.objects)
    }
    pub(super) fn lock(&self) -> StorageResult<MutexGuard<'_, State>> {
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
        _admission: NativeCloudWorkGuard,
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
    pub(super) fn expected_root_id(&self, state: &State) -> StorageResult<Digest> {
        self.object(state, state.objects - 1)?
            .expected_receipt_id()
            .map_err(protocol)
    }
    pub(super) fn object(&self, state: &State, index: u64) -> StorageResult<PlannedRecord> {
        if index >= state.objects {
            return Err(invalid("byte source object index"));
        }
        let obj: PlannedRecord = self.store.read(&object_name(index))?;
        obj.validate(&state.begin.job, &payload_name(index), |i| {
            ((i as u64) < state.native_files).then_some(u64::MAX)
        })
        .map(|()| obj)
        .map_err(Into::into)
    }
    pub(super) fn advance(&self, state: &mut State) -> StorageResult<()> {
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

pub fn open_planned_reader<R: Read + Send + 'static, E: From<PlanError>>(
    record: PlannedRecord,
    read_payload: impl FnOnce(&str, u64) -> Result<Vec<u8>, E>,
    open_seg: impl FnMut(PlanSegment) -> io::Result<(R, u64)> + Send + 'static,
) -> Result<Box<dyn Read + Send>, E> {
    if let Some(name) = &record.payload {
        let bytes = read_payload(name, record.input.object.bytes.get())?;
        if bytes.len() as u64 != record.input.object.bytes.get()
            || bytes_sha256(&bytes) != record.input.object.sha256
        {
            return Err(PlanError::Conflict.into());
        }
        return Ok(Box::new(io::Cursor::new(bytes)));
    }
    Ok(Box::new(SegmentReader(
        record.segments.into(),
        None,
        open_seg,
    )))
}

struct SegmentReader<R, F>(VecDeque<PlanSegment>, Option<(R, u64)>, F);

impl<R: Read, F: FnMut(PlanSegment) -> io::Result<(R, u64)>> Read for SegmentReader<R, F> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        while !buf.is_empty() {
            if let Some((r, left)) = &mut self.1 {
                if *left == 0 {
                    self.1 = None;
                    continue;
                }
                let max = (*left).min(buf.len() as u64) as usize;
                let n = r.read(&mut buf[..max])?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "byte native source truncated",
                    ));
                }
                *left -= n as u64;
                return Ok(n);
            }
            let Some(seg) = self.0.pop_front() else {
                return Ok(0);
            };
            self.1 = Some((self.2)(seg)?);
        }
        Ok(0)
    }
}

pub(super) fn invalid(reason: &str) -> StorageError {
    StorageError::InvalidOperation(reason.into())
}
pub(super) fn protocol(_error: ProtocolError) -> StorageError {
    invalid("byte protocol validation")
}
