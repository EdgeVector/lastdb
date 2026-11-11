//! A bounded client plan for unchanged complete native mutation batches.
mod planner;

use super::{store::Store, work::LogContext, BackupError, Result};
use cloud_backup_protocol::v2::{
    jobs::{JobResult, JobStatus},
    transfer::{ObjectReceipt, TransferInput},
    AcceptedBackup, Begin, Commit, Destination,
};
use cloud_backup_protocol::{bytes_sha256, Digest, RecoveryDescriptor};
use fold_db::cloud_backup_plan::{expected_receipt, PlanSegment};
use fold_db::storage::disk_pack::byte_backup::{
    ByteBackupCaptureStatus, ByteBackupObject, CODEC_ENVELOPE_MAX_BYTES,
};
use fold_db::storage::disk_pack::mutation_log::{
    FrozenLogCapture, MutationLogCapture, MutationLogReader,
};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::io::{self, Cursor, Read};
use std::sync::Arc;

const MAX_OBJECTS: u64 = 8192;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectRecord {
    input: TransferInput,
    segments: Vec<PlanSegment>,
    payload: Option<String>,
    receipt: Option<ObjectReceipt>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    version: u16,
    frozen_sha256: Digest,
    context: LogContext,
    objects: u64,
    files: u64,
    native_bytes: u64,
    packs: u64,
    multi_file_packs: u64,
    ready: bool,
}

fn object_name(index: u64) -> String {
    format!("log-object-{index:08}.json")
}
fn payload_name(index: u64) -> String {
    format!("log-payload-{index:08}.enc")
}

impl ObjectRecord {
    fn validate(&self, frozen: &FrozenLogCapture, index: u64) -> Result<()> {
        self.input.validate()?;
        let max_members = fold_db::storage_policy::backup_transport::MAX_PACK_MEMBERS;
        if index >= MAX_OBJECTS
            || self.input.job != frozen.begin.job
            || self.input.object.destination != Destination::R2
            || self
                .payload
                .as_deref()
                .is_some_and(|n| n != payload_name(index))
            || self.segments.len() > max_members
            || self.segments.is_empty() != self.payload.is_some()
        {
            return Err(BackupError::Invalid);
        }
        if self.payload.is_some() {
            if self.input.object.bytes.get() > CODEC_ENVELOPE_MAX_BYTES {
                return Err(BackupError::Invalid);
            }
        } else {
            let mut bytes = 0u64;
            for seg in &self.segments {
                let file = frozen.files.get(seg.file).ok_or(BackupError::Invalid)?;
                if seg.bytes == 0
                    || seg
                        .offset
                        .checked_add(seg.bytes)
                        .is_none_or(|end| end > file.bytes)
                {
                    return Err(BackupError::Identity);
                }
                bytes = bytes.checked_add(seg.bytes).ok_or(BackupError::Limit)?;
            }
            if bytes != self.input.object.bytes.get() {
                return Err(BackupError::Identity);
            }
        }
        if let Some(receipt) = &self.receipt {
            if *receipt != expected_receipt(&self.input)? {
                return Err(BackupError::Identity);
            }
        }
        Ok(())
    }
}

pub(in crate::byte_cloud_backup) struct LogCapture {
    core: Arc<MutationLogCapture>,
    frozen: FrozenLogCapture,
    store: Store,
    plan: Plan,
}

impl LogCapture {
    pub fn recovery_point(&self, accepted: &AcceptedBackup) -> super::recovery::RecoveryPoint {
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
            capture.build(recovery, key)?;
        }
        Ok(capture)
    }

    pub fn begin(&self) -> &Begin {
        &self.frozen.begin
    }

    pub fn object_count(&self) -> u64 {
        self.plan.objects
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
        if expected_receipt(&record.input)? != *receipt {
            return Err(BackupError::Identity);
        }
        record.receipt = Some(receipt.clone());
        self.store.write(&object_name(index), &record)
    }

    pub fn root_receipt_id(&self) -> Result<Digest> {
        let last = self
            .plan
            .objects
            .checked_sub(1)
            .ok_or(BackupError::Invalid)?;
        Ok(expected_receipt(&self.record(last)?.input)?.receipt_id)
    }

    pub fn status(&self) -> Result<ByteBackupCaptureStatus> {
        let mut verified = 0;
        for index in 0..self.plan.objects {
            verified += u64::from(self.record(index)?.receipt.is_some());
        }
        Ok(ByteBackupCaptureStatus {
            native_files: self.plan.files,
            native_bytes: self.plan.native_bytes,
            verified_objects: verified,
            retained_source_bytes: self.core.retained_source_bytes()?,
            metadata_pack_objects: self.plan.packs,
            multi_file_pack_objects: self.plan.multi_file_packs,
        })
    }

    pub fn budget(&self, bytes: u64) -> Result<()> {
        let limit = cloud_backup_protocol::limits::DURABLE_LOCAL_SPOOL_MAX_BYTES;
        if bytes
            .checked_add(self.core.retained_source_bytes()?)
            .is_none_or(|n| n > limit)
        {
            return Err(BackupError::Limit);
        }
        Ok(())
    }

    pub fn seal_commit(&self, commit: &Commit) -> Result<()> {
        if commit.root_receipt_id != self.root_receipt_id()? {
            return Err(BackupError::Identity);
        }
        Ok(self.core.seal_commit(commit)?)
    }

    pub fn terminal(&self, result: &JobResult) -> Result<()> {
        if let JobStatus::Published { backup } = &result.status {
            if self.root_receipt_id()? != backup.root_receipt_id
                || self.status()?.verified_objects != self.plan.objects
            {
                return Err(BackupError::Identity);
            }
        }
        Ok(self.core.terminal(result)?)
    }
}

struct Reader {
    core: Arc<MutationLogCapture>,
    remaining: VecDeque<PlanSegment>,
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
            let Some(seg) = self.remaining.pop_front() else {
                return Ok(0);
            };
            self.active = Some(
                self.core
                    .reader(seg.file, seg.offset, seg.bytes)
                    .map_err(|_| io::Error::other("frozen mutation source unavailable"))?,
            );
        }
    }
}
