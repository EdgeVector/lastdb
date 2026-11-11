//! A bounded client plan for unchanged complete native mutation batches.
mod planner;
use super::{store::Store, work::LogContext, BackupError, Result};
use cloud_backup_protocol::v2::{
    jobs::{JobResult, JobStatus},
    transfer::{ObjectReceipt, TransferInput},
    Begin, Commit, Destination,
};
use cloud_backup_protocol::{bytes_sha256, Digest, RecoveryDescriptor};
use fold_db::storage::disk_pack::byte_backup::{ByteBackupCaptureStatus, ByteBackupObject};
use fold_db::storage::disk_pack::mutation_log::{
    FrozenLogCapture, FrozenLogFile, MutationLogCapture, MutationLogReader,
};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::io::{self, Cursor, Read};
use std::sync::Arc;

pub(super) const MAX_OBJECTS: u64 = 8192;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Segment {
    pub file: usize,
    pub offset: u64,
    pub bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ObjectRecord {
    pub input: TransferInput,
    pub segments: Vec<Segment>,
    pub payload: Option<String>,
    pub receipt: Option<ObjectReceipt>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Plan {
    pub version: u16,
    pub frozen_sha256: Digest,
    pub context: LogContext,
    pub objects: u64,
    pub files: u64,
    pub native_bytes: u64,
    pub packs: u64,
    pub multi_file_packs: u64,
    pub ready: bool,
}

pub(super) fn expected(input: &TransferInput) -> Result<ObjectReceipt> {
    Ok(fold_db::cloud_backup_plan::expected_receipt(input)?)
}
pub(super) fn object_name(index: u64) -> String {
    format!("log-object-{index:08}.json")
}
pub(super) fn file_name(index: usize) -> String {
    format!("log-file-{index:08}.json")
}
pub(super) fn payload_name(index: u64) -> String {
    format!("log-payload-{index:08}.enc")
}
/// Log files hold main-home runs only, which go to R2.
pub(super) fn destination(_file: &FrozenLogFile) -> Destination {
    Destination::R2
}

impl ObjectRecord {
    pub fn validate(&self, frozen: &FrozenLogCapture, index: u64) -> Result<()> {
        self.input.validate()?;
        if index >= MAX_OBJECTS
            || self.input.job != frozen.begin.job
            || self
                .payload
                .as_ref()
                .is_some_and(|name| *name != payload_name(index))
            || self.segments.len() > fold_db::storage_policy::backup_transport::MAX_PACK_MEMBERS
            || self.segments.is_empty() != self.payload.is_some()
        {
            return Err(BackupError::Invalid);
        }
        let mut bytes = 0u64;
        for segment in &self.segments {
            let file = frozen.files.get(segment.file).ok_or(BackupError::Invalid)?;
            if segment.bytes == 0
                || segment
                    .offset
                    .checked_add(segment.bytes)
                    .is_none_or(|end| end > file.bytes)
                || self.input.object.destination != destination(file)
            {
                return Err(BackupError::Identity);
            }
            bytes = bytes.checked_add(segment.bytes).ok_or(BackupError::Limit)?;
        }
        if self.payload.is_some() {
            if self.input.object.destination != Destination::R2
                || self.input.object.bytes.get()
                    > fold_db::storage::disk_pack::byte_backup::CODEC_ENVELOPE_MAX_BYTES
            {
                return Err(BackupError::Invalid);
            }
        } else if bytes != self.input.object.bytes.get() {
            return Err(BackupError::Identity);
        }
        if let Some(receipt) = &self.receipt {
            if *receipt != expected(&self.input)? {
                return Err(BackupError::Identity);
            }
        }
        Ok(())
    }
}

pub(super) struct LogCapture {
    core: Arc<MutationLogCapture>,
    frozen: FrozenLogCapture,
    store: Store,
    plan: Plan,
}
impl LogCapture {
    pub fn recovery_point(
        &self,
        accepted: &cloud_backup_protocol::v2::AcceptedBackup,
    ) -> super::recovery::RecoveryPoint {
        super::recovery::RecoveryPoint {
            accepted: accepted.clone(),
            base: self.plan.context.base.clone(),
            anchor: Some(self.frozen.anchor.clone()),
            through: Some(self.frozen.through.clone()),
            frontiers: self.frozen.through.frontiers,
        }
    }
    pub fn prepare(
        core: MutationLogCapture,
        store: &Store,
        context: &LogContext,
        recovery: &RecoveryDescriptor,
        key: &[u8; 32],
    ) -> Result<Self> {
        let frozen = core.manifest()?;
        if frozen.anchor != context.anchor || frozen.from != context.from {
            return Err(BackupError::Identity);
        }
        let frozen_sha256 = bytes_sha256(&cloud_backup_protocol::v2::hash::canonical_bytes(&(
            &frozen.begin,
            &frozen.anchor,
            &frozen.from,
            &frozen.through,
            &frozen.entries,
            &frozen.files,
        ))?);
        let plan = store.read::<Plan>("log-plan.json")?.unwrap_or(Plan {
            version: 1,
            frozen_sha256: frozen_sha256.clone(),
            context: context.clone(),
            objects: 0,
            files: frozen.files.len() as u64,
            native_bytes: frozen.native_bytes,
            packs: 0,
            multi_file_packs: 0,
            ready: false,
        });
        if plan.version != 1
            || plan.frozen_sha256 != frozen_sha256
            || plan.context != *context
            || plan.files != frozen.files.len() as u64
            || plan.native_bytes != frozen.native_bytes
            || plan.objects > MAX_OBJECTS
            || plan.multi_file_packs > plan.packs
        {
            return Err(BackupError::Identity);
        }
        let mut capture = Self {
            core: Arc::new(core),
            frozen,
            store: store.clone(),
            plan,
        };
        if !capture.plan.ready {
            if capture.frozen.terminal.is_some() || capture.frozen.local_cancelled {
                return Err(BackupError::Invalid);
            }
            store.write("log-plan.json", &capture.plan)?;
            planner::build(&mut capture, recovery, key)?;
        }
        Ok(capture)
    }
    pub fn begin(&self) -> Result<Begin> {
        Ok(self.frozen.begin.clone())
    }
    pub fn object_count(&self) -> Result<u64> {
        if !self.plan.ready {
            return Err(BackupError::Invalid);
        }
        Ok(self.plan.objects)
    }
    fn record(&self, index: u64) -> Result<ObjectRecord> {
        if index >= self.plan.objects {
            return Err(BackupError::Invalid);
        }
        let record: ObjectRecord = self
            .store
            .read(&object_name(index))?
            .ok_or(BackupError::Missing)?;
        record.validate(&self.frozen, index)?;
        Ok(record)
    }
    pub fn object(&self, index: u64) -> Result<ByteBackupObject> {
        let record = self.record(index)?;
        Ok(ByteBackupObject {
            input: record.input,
            receipt: record.receipt,
        })
    }
    pub fn reader(&self, index: u64) -> Result<Box<dyn Read + Send>> {
        let record = self.record(index)?;
        if let Some(name) = record.payload {
            let bytes = self.store.raw(&name)?.ok_or(BackupError::Missing)?;
            if bytes.len() as u64 != record.input.object.bytes.get()
                || bytes_sha256(&bytes) != record.input.object.sha256
            {
                return Err(BackupError::Identity);
            }
            return Ok(Box::new(Cursor::new(bytes)));
        }
        Ok(Box::new(Reader {
            core: Arc::clone(&self.core),
            remaining: record.segments.into(),
            active: None,
        }))
    }
    pub fn confirm(&self, index: u64, receipt: &ObjectReceipt) -> Result<()> {
        let mut record = self.record(index)?;
        if expected(&record.input)? != *receipt {
            return Err(BackupError::Identity);
        }
        record.receipt = Some(receipt.clone());
        self.store.write(&object_name(index), &record)
    }
    pub fn root_receipt_id(&self) -> Result<Digest> {
        Ok(expected(
            &self
                .record(
                    self.plan
                        .objects
                        .checked_sub(1)
                        .ok_or(BackupError::Invalid)?,
                )?
                .input,
        )?
        .receipt_id)
    }
    pub fn status(&self) -> Result<ByteBackupCaptureStatus> {
        let mut verified = 0;
        for index in 0..self.plan.objects {
            verified += u64::from(self.record(index)?.receipt.is_some());
        }
        let retained = self.core.retained_source_bytes()?;
        Ok(ByteBackupCaptureStatus {
            ready: self.plan.ready,
            source_available: retained > 0,
            native_files: self.plan.files,
            native_bytes: self.plan.native_bytes,
            objects: self.plan.objects,
            verified_objects: verified,
            retained_source_bytes: retained,
            metadata_spool_bytes: self.store.bytes(),
            metadata_pack_objects: self.plan.packs,
            multi_file_pack_objects: self.plan.multi_file_packs,
        })
    }
    pub fn budget(&self, bytes: u64) -> Result<()> {
        if bytes
            .checked_add(self.core.retained_source_bytes()?)
            .is_none_or(|n| n > cloud_backup_protocol::limits::DURABLE_LOCAL_SPOOL_MAX_BYTES)
        {
            return Err(BackupError::Limit);
        }
        Ok(())
    }
    pub fn seal_commit(&self, commit: &Commit) -> Result<()> {
        if commit.root_receipt_id != self.root_receipt_id()? {
            return Err(BackupError::Identity);
        }
        self.core.seal_commit(commit)?;
        Ok(())
    }
    pub fn terminal(&self, result: &JobResult) -> Result<()> {
        if let JobStatus::Published { backup } = &result.status {
            if self.root_receipt_id()? != backup.root_receipt_id
                || self.status()?.verified_objects != self.plan.objects
            {
                return Err(BackupError::Identity);
            }
        }
        self.core.terminal(result)?;
        Ok(())
    }
}

struct Reader {
    core: Arc<MutationLogCapture>,
    remaining: VecDeque<Segment>,
    active: Option<MutationLogReader>,
}
impl Read for Reader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        loop {
            if let Some(reader) = &mut self.active {
                let count = reader.read(buffer)?;
                if count > 0 {
                    return Ok(count);
                }
                self.active = None;
            }
            let Some(segment) = self.remaining.pop_front() else {
                return Ok(0);
            };
            self.active = Some(
                self.core
                    .reader(segment.file, segment.offset, segment.bytes)
                    .map_err(|_| io::Error::other("frozen mutation source unavailable"))?,
            );
        }
    }
}
