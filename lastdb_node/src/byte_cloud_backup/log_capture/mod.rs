//! A bounded client plan for unchanged complete native mutation batches.
mod metadata;
mod model;
mod planner;
mod reader;
use super::{store::Store, work::LogContext, BackupError, Result};
use cloud_backup_protocol::v2::{
    jobs::{JobResult, JobStatus},
    transfer::ObjectReceipt,
    Begin, Commit,
};
use cloud_backup_protocol::{bytes_sha256, Digest, RecoveryDescriptor};
use fold_db::storage::disk_pack::byte_backup::{ByteBackupCaptureStatus, ByteBackupObject};
use fold_db::storage::disk_pack::mutation_log::{FrozenLogCapture, MutationLogCapture};
use model::{ObjectRecord, Plan};
use std::io::Read;
use std::sync::Arc;

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
            || plan.objects > model::MAX_OBJECTS
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
            .read(&model::object_name(index))?
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
        reader::open(self, index)
    }
    pub fn confirm(&self, index: u64, receipt: &ObjectReceipt) -> Result<()> {
        let mut record = self.record(index)?;
        if model::expected(&record.input)? != *receipt {
            return Err(BackupError::Identity);
        }
        record.receipt = Some(receipt.clone());
        self.store.write(&model::object_name(index), &record)
    }
    pub fn root_receipt_id(&self) -> Result<Digest> {
        Ok(model::expected(
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
