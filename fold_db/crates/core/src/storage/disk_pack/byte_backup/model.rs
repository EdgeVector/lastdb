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
    /// Always empty: a run capture keeps no CURRENT copy. A saved job record
    /// still names the field.
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
    pub run: Option<super::RunBackupProfile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal_anchor: Option<super::super::mutation_log::LogAnchor>,
}
impl State {
    pub fn validate(&self) -> StorageResult<()> {
        self.begin.validate().map_err(protocol)?;
        // Every capture is a run capture: a v2 home has no other source.
        let (Some(run), Some(recovery)) = (&self.run, &self.recovery) else {
            return Err(invalid("byte source stored profile phase"));
        };
        run.validate_root(&self.native_root)?;
        if let Some(anchor) = &self.journal_anchor {
            anchor.position()?;
            if anchor.main_home != run.main.home_id
                || anchor.cas_home != run.cas.home_id
                || anchor.frontiers.main != run.main.frontier.get()
                || anchor.frontiers.cas != run.cas.frontier.get()
            {
                return Err(invalid("snapshot mutation log anchor differs"));
            }
        }
        if self.format != 2
            || self.reference_id.is_nil()
            || !self.current_bytes.is_empty()
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
        recovery
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
}
impl ObjectRecord {
    pub fn validate(&self, begin: &Begin) -> StorageResult<()> {
        self.input.validate().map_err(protocol)?;
        if self.input.job != begin.job
            || self.segments.len() > lastdb_storage_policy::backup_transport::MAX_PACK_MEMBERS
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
