use super::{invalid, protocol, StorageResult};
use cloud_backup_protocol::v2::{
    jobs::{JobResult, JobStatus},
    transfer::{ObjectReceipt, TransferInput},
    Begin,
};
use cloud_backup_protocol::{Digest, NativeRoot, RecoveryDescriptor, RelativePath};
use serde::{Deserialize, Serialize};

pub(super) const MAX_OBJECTS: u64 = (cloud_backup_protocol::v2::limits::MAX_PAGE_ENTRIES
    * cloud_backup_protocol::v2::limits::MAX_PAGES) as u64;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct State {
    pub format: u16,
    pub begin: Begin,
    pub reference_id: uuid::Uuid,
    pub current_bytes: Vec<u8>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery: Option<RecoveryDescriptor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_profile: Option<super::StoredRecordProfile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<super::RunBackupProfile>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub max_coordinate_bytes: u64,
}
impl State {
    pub fn validate(&self) -> StorageResult<()> {
        self.begin.validate().map_err(protocol)?;
        if let Some(run) = &self.run {
            run.validate_root(&self.native_root)?;
        } else if self.recovery.is_some() {
            super::validate_client_native_root(&self.native_root)?;
        } else {
            self.native_root.validate().map_err(protocol)?;
        }
        if self.format != 2
            || self.reference_id.is_nil()
            || if self.run.is_some() {
                !self.current_bytes.is_empty()
            } else {
                self.current_bytes.len() != 4096
            }
            || self.objects > MAX_OBJECTS
            || self.verified > self.objects
            || self.multi_file_pack_objects > self.metadata_pack_objects
            || self.metadata_pack_objects > self.objects
            || (self.ready
                && (self.objects < if self.recovery.is_some() { 2 } else { 3 }
                    || self.native_files == 0
                    || self.native_bytes == 0))
        {
            return Err(invalid("byte source state bounds"));
        }
        if self.begin.descriptor.as_value().is_null() != self.recovery.is_some() {
            return Err(invalid("byte source recovery profile differs"));
        }
        if let Some(recovery) = &self.recovery {
            recovery
                .validate_for(&self.begin.job.stream.scope)
                .map_err(protocol)?;
        }
        if self.record_profile.is_some() && (self.recovery.is_none() || self.run.is_some())
            || self.run.is_some() && self.recovery.is_none()
            || self.ready
                && self.recovery.is_some()
                && self.record_profile.is_none()
                && self.run.is_none()
        {
            return Err(invalid("byte source stored profile phase"));
        }
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
pub(super) struct Segment {
    pub path: RelativePath,
    pub offset: u64,
    pub bytes: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ObjectRecord {
    pub input: TransferInput,
    pub segments: Vec<Segment>,
    pub native_files: Vec<super::ClientNativeFile>,
    pub metadata_pack: bool,
    pub ciphertext: Option<String>,
    pub receipt: Option<ObjectReceipt>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub record_segments: Vec<super::record_plan::RecordSegment>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_stream: Option<super::ClientStreamChunk>,
}
fn is_zero(value: &u64) -> bool {
    *value == 0
}
impl ObjectRecord {
    pub fn validate(&self, begin: &Begin) -> StorageResult<()> {
        self.input.validate().map_err(protocol)?;
        if self.input.job != begin.job
            || self.segments.len() > lastdb_storage_policy::backup_transport::MAX_PACK_MEMBERS
            || self.record_segments.len()
                > lastdb_storage_policy::backup_transport::MAX_PACK_MEMBERS
            || ((self.segments.is_empty() && self.record_segments.is_empty())
                != self.ciphertext.is_some())
            || (!self.segments.is_empty() && !self.record_segments.is_empty())
            || (self.metadata_pack && self.segments.is_empty() && self.record_segments.is_empty())
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
        for part in &self.record_segments {
            if part.bytes == 0 || part.offset.checked_add(part.bytes).is_none() {
                return Err(invalid("byte stored body segment range"));
            }
            total = total
                .checked_add(part.bytes)
                .ok_or_else(|| invalid("byte stored body segment sum"))?;
        }
        if (!self.segments.is_empty() || !self.record_segments.is_empty())
            && total != self.input.object.bytes.get()
        {
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
pub(super) fn check_receipt(input: &TransferInput, receipt: &ObjectReceipt) -> StorageResult<()> {
    receipt.validate().map_err(protocol)?;
    if receipt.scope != input.job.stream.scope || receipt.object != input.object {
        return Err(invalid("byte source verified receipt identity"));
    }
    Ok(())
}
pub(super) fn object_name(index: u64) -> String {
    format!("object-{index:08}.json")
}
