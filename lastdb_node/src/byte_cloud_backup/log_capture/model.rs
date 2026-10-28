use super::*;
use cloud_backup_protocol::v2::transfer::{ObjectReceipt, TransferInput};
use serde::{Deserialize, Serialize};

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
    input.validate()?;
    let mut receipt = ObjectReceipt {
        version: cloud_backup_protocol::v2::Version::V2,
        receipt_id: input.object.sha256.clone(),
        scope: input.job.stream.scope.clone(),
        object: input.object.clone(),
    };
    receipt.receipt_id = receipt.identity_sha256()?;
    receipt.validate()?;
    Ok(receipt)
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

impl ObjectRecord {
    pub fn validate(&self, frozen: &FrozenLogCapture, index: u64) -> Result<()> {
        self.input.validate()?;
        if index >= MAX_OBJECTS
            || self.input.job != frozen.begin.job
            || self
                .payload
                .as_ref()
                .is_some_and(|name| *name != payload_name(index))
            || self.segments.len() > lastdb_storage_policy::backup_transport::MAX_PACK_MEMBERS
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
                || self.input.object.destination != Some(destination(file))
            {
                return Err(BackupError::Identity);
            }
            bytes = bytes.checked_add(segment.bytes).ok_or(BackupError::Limit)?;
        }
        if self.payload.is_some() {
            if self.input.object.destination != Some(cloud_backup_protocol::v2::Destination::R2)
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
pub(super) fn destination(
    file: &fold_db::storage::disk_pack::mutation_log::FrozenLogFile,
) -> cloud_backup_protocol::v2::Destination {
    if file.role.is_cas() {
        cloud_backup_protocol::v2::Destination::B2
    } else {
        cloud_backup_protocol::v2::Destination::R2
    }
}
