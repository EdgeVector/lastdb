//! Stream, plan, encrypt, and validate frozen mutation-log transfer objects.
use super::{recovery::RecoveryPoint, store::Store, work::LogContext, BackupError, Result};
use crate::byte_cloud_log::{MutationLogPage, MutationLogRoot};
use cloud_backup_protocol::v2::{
    hash::canonical_bytes,
    jobs::{JobResult, JobStatus},
    transfer::{ObjectReceipt, TransferInput},
    AcceptedBackup, Begin, Commit, Destination, Object,
};
use cloud_backup_protocol::{
    bytes_sha256, limits::DURABLE_LOCAL_SPOOL_MAX_BYTES, Digest, RecoveryDescriptor,
};
use fold_db::cloud_backup_plan::{
    expected_receipt, new_transfer_input, plan_files, PlanFile, PlanSegment,
};
use fold_db::storage::disk_pack::byte_backup::{
    encode_metadata, seal_envelope, ByteBackupCaptureStatus, ByteBackupObject, ClientPageReference,
    SegmentReader, CODEC_ENVELOPE_MAX_BYTES, MAX_PAGE_FILES,
};
use fold_db::storage::disk_pack::mutation_log::{FrozenLogCapture, MutationLogCapture};
use fold_db::storage_policy::backup_transport::MAX_PACK_MEMBERS;
use serde::{Deserialize, Serialize};
use std::io::{self, Cursor, Read};
use std::sync::Arc;

const MAX_OBJECTS: u64 = 8192;

fn obj_file(index: u64) -> String {
    format!("log-object-{index:08}.json")
}
fn payload_file(index: u64) -> String {
    format!("log-payload-{index:08}.enc")
}

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

impl ObjectRecord {
    fn validate(&self, frozen: &FrozenLogCapture, index: u64) -> Result<()> {
        self.input.validate()?;
        let expected = self.input.object.bytes.get();
        if index >= MAX_OBJECTS
            || self.input.job != frozen.begin.job
            || self.input.object.destination != Destination::R2
            || self
                .payload
                .as_deref()
                .is_some_and(|n| n != payload_file(index))
            || self.segments.len() > MAX_PACK_MEMBERS
            || self.segments.iter().any(|s| s.file >= frozen.files.len())
            || self.segments.is_empty() != self.payload.is_some()
            || (self.payload.is_some() && expected > CODEC_ENVELOPE_MAX_BYTES)
        {
            return Err(BackupError::Invalid);
        }
        if self.payload.is_none()
            && fold_db::cloud_backup_plan::sum_segments(&self.segments, |i| {
                Some(frozen.files[i].bytes)
            }) != Some(expected)
        {
            return Err(BackupError::Identity);
        }
        (!self
            .receipt
            .as_ref()
            .is_some_and(|r| Ok(r) != expected_receipt(&self.input).as_ref()))
        .then_some(())
        .ok_or(BackupError::Identity)
    }
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
        let totals = plan_files(
            &files,
            &scope,
            |f| core.reader(f.id, 0, f.bytes).map_err(BackupError::from),
            |idx, p| {
                native_files.extend(p.native_files);
                self.save_object(idx, p.object, p.segments, None)
                    .map(|_| ())
            },
        )?;
        (self.plan.packs, self.plan.multi_file_packs) = (totals.packs, totals.multi_file_packs);
        let (mut index, mut pages) = (totals.objects, Vec::new());
        for (i, chunk) in native_files.chunks(MAX_PAGE_FILES).enumerate() {
            let page = MutationLogPage::new(i as u32, &self.frozen.begin, chunk.to_vec())
                .map_err(|_| BackupError::Invalid)?;
            let obj = self.freeze(key, index, &page)?;
            pages.push(ClientPageReference::for_object(
                page.page_index,
                &obj.input,
            )?);
            index += 1;
        }
        let ctx = &self.plan.context;
        let root = MutationLogRoot::from_frozen(
            &self.frozen,
            recovery.clone(),
            ctx.base.clone(),
            ctx.previous.clone(),
            pages,
        )
        .map_err(|_| BackupError::Invalid)?;
        self.freeze(key, index, &root)?;
        if self.plan.objects != index + 1 {
            return Err(BackupError::Identity);
        }
        self.plan.ready = true;
        self.store.write("log-plan.json", &self.plan)
    }

    fn freeze(&mut self, key: &[u8; 32], index: u64, val: &impl Serialize) -> Result<ObjectRecord> {
        let name = payload_file(index);
        let plain = encode_metadata(val).map_err(|_| BackupError::Limit)?;
        let (obj, bytes, fresh) =
            seal_envelope(key, &plain, self.store.raw(&name)?).map_err(|e| match e {
                "byte metadata encryption" => BackupError::Invalid,
                _ => BackupError::Identity,
            })?;
        if fresh {
            self.store.write_raw(&name, &bytes)?;
        }
        self.save_object(index, obj, Vec::new(), Some(name))
    }

    fn save_object(
        &mut self,
        index: u64,
        object: Object,
        segments: Vec<PlanSegment>,
        payload: Option<String>,
    ) -> Result<ObjectRecord> {
        if index >= MAX_OBJECTS {
            return Err(BackupError::Limit);
        }
        let name = obj_file(index);
        let record = if let Some(p) = self.store.read::<ObjectRecord>(&name)? {
            p.validate(&self.frozen, index)?;
            if (p.input.object != object) || (p.segments != segments) || (p.payload != payload) {
                return Err(BackupError::Identity);
            }
            p
        } else {
            ObjectRecord {
                input: new_transfer_input(self.frozen.begin.job.clone(), object, None)?,
                segments,
                payload,
                receipt: None,
            }
        };
        record.validate(&self.frozen, index)?;
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

    fn record(&self, index: u64) -> Result<ObjectRecord> {
        if index >= self.plan.objects {
            return Err(BackupError::Invalid);
        }
        let r: ObjectRecord = self
            .store
            .read(&obj_file(index))?
            .ok_or(BackupError::Missing)?;
        r.validate(&self.frozen, index).map(|()| r)
    }

    pub fn object(&self, index: u64) -> Result<ByteBackupObject> {
        let r = self.record(index)?;
        Ok(ByteBackupObject {
            input: r.input,
            receipt: r.receipt,
        })
    }

    pub fn reader(&self, index: u64) -> Result<Box<dyn Read + Send>> {
        let r = self.record(index)?;
        if let Some(name) = r.payload {
            let b = self.store.raw(&name)?.ok_or(BackupError::Missing)?;
            if b.len() as u64 != r.input.object.bytes.get()
                || bytes_sha256(&b) != r.input.object.sha256
            {
                return Err(BackupError::Identity);
            }
            return Ok(Box::new(Cursor::new(b)));
        }
        let core = Arc::clone(&self.core);
        Ok(Box::new(SegmentReader::new(
            r.segments,
            move |seg: PlanSegment| {
                core.reader(seg.file, seg.offset, seg.bytes)
                    .map(|rd| (rd, seg.bytes))
                    .map_err(|_| io::Error::other("frozen mutation source unavailable"))
            },
        )))
    }

    pub fn confirm(&self, index: u64, receipt: &ObjectReceipt) -> Result<()> {
        let mut r = self.record(index)?;
        if expected_receipt(&r.input)? != *receipt {
            return Err(BackupError::Identity);
        }
        r.receipt = Some(receipt.clone());
        self.store.write(&obj_file(index), &r)
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
        let verified_objects = (0..self.plan.objects).try_fold(0u64, |n, i| {
            Ok::<_, BackupError>(n + u64::from(self.record(i)?.receipt.is_some()))
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
