//! Stream, plan, encrypt, and validate frozen mutation-log transfer objects.
use super::{
    store::Store,
    work::{LogContext, RecoveryPoint},
    BackupError, Result,
};
use crate::byte_cloud_log::{MutationLogPage, MutationLogRoot};
use cloud_backup_protocol::v2::{
    hash::canonical_bytes,
    jobs::{JobResult, JobStatus},
    transfer::ObjectReceipt,
    AcceptedBackup, Begin, Commit, Destination, Object,
};
use cloud_backup_protocol::{
    bytes_sha256, limits::DURABLE_LOCAL_SPOOL_MAX_BYTES, Digest, RecoveryDescriptor,
};
use fold_db::cloud_backup_plan::{plan_files, PlanFile, PlanSegment, PlannedRecord};
use fold_db::storage::disk_pack::byte_backup::{
    open_planned_reader, seal_pages, seal_stored_envelope, ByteBackupCaptureStatus,
};
use fold_db::storage::disk_pack::mutation_log::{FrozenLogCapture, MutationLogCapture};
use serde::{Deserialize, Serialize};
use std::io::{self, Read};
use std::sync::Arc;

const MAX_OBJECTS: u64 = 8192;

fn obj_file(index: u64) -> String {
    format!("log-object-{index:08}.json")
}
fn payload_file(index: u64) -> String {
    format!("log-payload-{index:08}.enc")
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

fn check_record(r: &PlannedRecord, frozen: &FrozenLogCapture, index: u64) -> Result<()> {
    if index >= MAX_OBJECTS || r.input.object.destination != Destination::R2 {
        return Err(BackupError::Invalid);
    }
    Ok(r.validate(&frozen.begin.job, &payload_file(index), |i| {
        frozen.files.get(i).map(|f| f.bytes)
    })?)
}

pub(in crate::byte_cloud_backup) struct LogCapture {
    core: Arc<MutationLogCapture>,
    frozen: FrozenLogCapture,
    store: Store,
    plan: Plan,
}

impl LogCapture {
    pub fn recovery_point(&self, accepted: &AcceptedBackup) -> RecoveryPoint {
        RecoveryPoint {
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
        ctx: &LogContext,
        recovery: &RecoveryDescriptor,
        key: &[u8; 32],
    ) -> Result<Self> {
        let f = core.manifest()?;
        if f.anchor != ctx.anchor || f.from != ctx.from {
            return Err(BackupError::Identity);
        }
        let frozen_sha256 = bytes_sha256(&canonical_bytes(&(
            &f.begin, &f.anchor, &f.from, &f.through, &f.entries, &f.files,
        ))?);
        let plan = store.read::<Plan>("log-plan.json")?.unwrap_or(Plan {
            version: 1,
            frozen_sha256: frozen_sha256.clone(),
            context: ctx.clone(),
            objects: 0,
            files: f.files.len() as u64,
            native_bytes: f.native_bytes,
            packs: 0,
            multi_file_packs: 0,
            ready: false,
        });
        if plan.version != 1
            || plan.frozen_sha256 != frozen_sha256
            || plan.context != *ctx
            || (plan.files, plan.native_bytes) != (f.files.len() as u64, f.native_bytes)
            || plan.objects > MAX_OBJECTS
            || plan.multi_file_packs > plan.packs
        {
            return Err(BackupError::Identity);
        }
        let mut cap = Self {
            core: Arc::new(core),
            frozen: f,
            store: store.clone(),
            plan,
        };
        if !cap.plan.ready {
            if cap.frozen.terminal.is_some() || cap.frozen.local_cancelled {
                return Err(BackupError::Invalid);
            }
            store.write("log-plan.json", &cap.plan)?;
            cap.build(recovery, key)?;
        }
        Ok(cap)
    }

    fn build(&mut self, recovery: &RecoveryDescriptor, key: &[u8; 32]) -> Result<()> {
        let mut files: Vec<_> = self
            .frozen
            .files
            .iter()
            .enumerate()
            .map(|(id, f)| PlanFile {
                id,
                path: f.path.clone(),
                bytes: f.bytes,
                destination: Destination::R2,
            })
            .collect();
        files.sort_by(|a, b| a.path.as_str().cmp(b.path.as_str()));
        let (core, scope) = (
            Arc::clone(&self.core),
            self.frozen.begin.job.stream.scope.clone(),
        );
        let mut native_files = Vec::with_capacity(files.len());
        let (index, packs, multi) = plan_files(
            &files,
            &scope,
            |f| core.reader(f.id, 0, f.bytes).map_err(BackupError::from),
            |idx, obj, segs, nfiles| {
                native_files.extend(nfiles);
                self.save_object(idx, (obj, segs, None)).map(|_| ())
            },
        )?;
        self.plan.packs = packs;
        self.plan.multi_file_packs = multi;
        let (frozen, ctx) = (self.frozen.clone(), self.plan.context.clone());
        let total = seal_pages(
            &native_files,
            index,
            |i, chunk| {
                MutationLogPage::new(i, &frozen.begin, chunk).map_err(|_| BackupError::Invalid)
            },
            |pages| {
                MutationLogRoot::from_frozen(
                    &frozen,
                    recovery.clone(),
                    ctx.base,
                    ctx.previous,
                    pages,
                )
                .map_err(|_| BackupError::Invalid)
            },
            |idx, plain| {
                let name = payload_file(idx);
                let (obj, fresh) = seal_stored_envelope(key, plain, self.store.raw(&name)?)
                    .map_err(|e| {
                        if e == "byte metadata encryption" {
                            BackupError::Invalid
                        } else {
                            BackupError::Identity
                        }
                    })?;
                if let Some(b) = fresh {
                    self.store.write_raw(&name, &b)?;
                }
                self.save_object(idx, (obj, Vec::new(), Some(name)))
            },
            |e| {
                if e.to_string().contains("protocol") {
                    BackupError::Invalid
                } else {
                    BackupError::Limit
                }
            },
        )?;
        (self.plan.objects == total)
            .then_some(())
            .ok_or(BackupError::Identity)?;
        self.plan.ready = true;
        self.store.write("log-plan.json", &self.plan)
    }

    fn save_object(
        &mut self,
        index: u64,
        spec: (Object, Vec<PlanSegment>, Option<String>),
    ) -> Result<PlannedRecord> {
        if index >= MAX_OBJECTS {
            return Err(BackupError::Limit);
        }
        let name = obj_file(index);
        let record = PlannedRecord::upsert(
            self.store.read::<PlannedRecord>(&name)?,
            &self.frozen.begin.job,
            spec,
            &payload_file(index),
            |i| self.frozen.files.get(i).map(|f| f.bytes),
        )?;
        check_record(&record, &self.frozen, index)?;
        self.store.write(&name, &record)?;
        self.plan.objects = self.plan.objects.max(index + 1);
        self.set_coordinator_spool_bytes(self.store.bytes())?;
        self.store
            .write("log-plan.json", &self.plan)
            .map(|()| record)
    }

    pub fn begin(&self) -> Result<Begin> {
        Ok(self.frozen.begin.clone())
    }
    pub fn object_count(&self) -> Result<u64> {
        Ok(self.plan.objects)
    }

    pub fn object(&self, index: u64) -> Result<PlannedRecord> {
        (index < self.plan.objects)
            .then_some(())
            .ok_or(BackupError::Invalid)?;
        let r: PlannedRecord = self
            .store
            .read(&obj_file(index))?
            .ok_or(BackupError::Missing)?;
        check_record(&r, &self.frozen, index).map(|()| r)
    }

    pub fn reader(&self, index: u64) -> Result<Box<dyn Read + Send>> {
        let core = Arc::clone(&self.core);
        open_planned_reader(
            self.object(index)?,
            |name, _| self.store.raw(name)?.ok_or(BackupError::Missing),
            move |seg: PlanSegment| {
                core.reader(seg.file, seg.offset, seg.bytes)
                    .map(|rd| (rd, seg.bytes))
                    .map_err(|_| io::Error::other("frozen mutation source unavailable"))
            },
        )
    }

    pub fn confirm(&self, index: u64, receipt: &ObjectReceipt) -> Result<()> {
        let mut r = self.object(index)?;
        r.confirm(receipt)?;
        self.store.write(&obj_file(index), &r)
    }

    pub fn root_receipt_id(&self) -> Result<Digest> {
        let last = self
            .plan
            .objects
            .checked_sub(1)
            .ok_or(BackupError::Invalid)?;
        Ok(self.object(last)?.expected_receipt_id()?)
    }

    pub fn status(&self) -> Result<ByteBackupCaptureStatus> {
        let verified_objects = (0..self.plan.objects).try_fold(0u64, |n, i| {
            Ok::<_, BackupError>(n + u64::from(self.object(i)?.receipt.is_some()))
        })?;
        Ok(ByteBackupCaptureStatus {
            native_files: self.plan.files,
            native_bytes: self.plan.native_bytes,
            verified_objects,
            retained_source_bytes: self.core.retained_source_bytes()?,
            metadata_pack_objects: self.plan.packs,
            multi_file_pack_objects: self.plan.multi_file_packs,
        })
    }

    pub fn set_coordinator_spool_bytes(&self, bytes: u64) -> Result<()> {
        bytes
            .checked_add(self.core.retained_source_bytes()?)
            .filter(|&n| n <= DURABLE_LOCAL_SPOOL_MAX_BYTES)
            .map(|_| ())
            .ok_or(BackupError::Limit)
    }

    pub fn seal_commit(&self, commit: &Commit) -> Result<()> {
        (commit.root_receipt_id == self.root_receipt_id()?)
            .then_some(())
            .ok_or(BackupError::Identity)?;
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
