use super::super::file_dependencies::{
    FileDependencyRecord, NativeCasProof, NATIVE_CAS_ROW_MAX_BYTES,
};
use super::{invalid, protocol, StorageResult};
use cloud_backup_protocol::transfer::{ObjectReceipt, TransferInput};
use cloud_backup_protocol::{BackupJob, Digest, ObjectKind};
use serde::{Deserialize, Serialize};

pub(super) const ENTRIES_MAX: u64 = 512 * 1024;
pub(super) const ENTRY_MAX: u64 = 16 * 1024;

/// One small nonsecret source selector. The original per-file key stays only
/// in the encrypted atom and in the caller's short-lived decoded dependency.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupFileDependencyEntry {
    pub job: BackupJob,
    pub index: u64,
    pub atom_scope: Option<String>,
    pub dependency: FileDependencyRecord,
    pub coverage: FileCoverage,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
pub enum FileCoverage {
    NativeCovered {
        proof: NativeCasProof,
    },
    ExternalNeeded,
    BackupOwned {
        input: Box<TransferInput>,
        input_sha256: Digest,
        receipt: Option<ObjectReceipt>,
    },
}

impl BackupFileDependencyEntry {
    /// Build an encrypted manifest entry only from the saved authoritative receipt.
    /// Native-covered bytes are already present in the exact native inventory.
    pub fn external_entry(
        &self,
    ) -> StorageResult<Option<cloud_backup_protocol::ExternalFileDependency>> {
        self.validate(&self.job, self.index)?;
        let FileCoverage::BackupOwned {
            input,
            receipt: Some(receipt),
            ..
        } = &self.coverage
        else {
            if matches!(self.coverage, FileCoverage::NativeCovered { .. }) {
                return Ok(None);
            }
            return Err(invalid(
                "backup file dependency needs a verified cloud receipt",
            ));
        };
        let value = cloud_backup_protocol::ExternalFileDependency {
            kind: self.dependency.kind,
            logical_pointer_sha256: self.dependency.logical_pointer_sha256.clone(),
            stored_source_sha256: input.object.sha256.clone(),
            stored_source_bytes: input.object.bytes,
            encryption_suite: cloud_backup_protocol::Text::try_from(
                self.dependency.encryption_suite.clone(),
            )
            .map_err(protocol)?,
            extents: vec![cloud_backup_protocol::ObjectExtent {
                receipt_id: receipt.receipt_id.clone(),
                offset: cloud_backup_protocol::DecimalU64::new(0),
                length: input.object.bytes,
            }],
        };
        value.validate().map_err(protocol)?;
        Ok(Some(value))
    }

    pub(super) fn validate(&self, job: &BackupJob, index: u64) -> StorageResult<()> {
        if self.job != *job
            || self.index != index
            || index >= ENTRIES_MAX
            || self.dependency.version != 1
        {
            return Err(invalid("backup dependency entry identity"));
        }
        match &self.coverage {
            FileCoverage::NativeCovered { proof } => {
                if proof.collection != "cas_blobs"
                    || proof.stored_row_bytes == 0
                    || proof.stored_row_bytes > NATIVE_CAS_ROW_MAX_BYTES
                    || proof.key
                        != crate::storage::laststore::native_routing::encode(
                            self.dependency.blob_ref.as_bytes(),
                        )
                {
                    return Err(invalid("backup native CAS proof identity"));
                }
            }
            FileCoverage::ExternalNeeded => {}
            FileCoverage::BackupOwned {
                input,
                input_sha256,
                receipt,
            } => {
                input.validate().map_err(protocol)?;
                if input.job != *job
                    || input.object.kind != ObjectKind::BlobPart
                    || input.object.bytes.get() != self.dependency.expected_stored_bytes
                    || input.input_sha256().map_err(protocol)? != *input_sha256
                {
                    return Err(invalid("backup file transfer identity"));
                }
                if let Some(receipt) = receipt {
                    receipt.validate().map_err(protocol)?;
                    if receipt.scope != job.stream.scope || receipt.object != input.object {
                        return Err(invalid("backup file receipt identity"));
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DependencyState {
    pub format: u16,
    pub job: BackupJob,
    pub current_sha256: Digest,
    pub certified: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub metadata_covered: bool,
    pub entries: u64,
    pub native_covered: u64,
    pub external_confirmed: u64,
    pub catalog_bytes: u64,
    pub active_external: Option<u64>,
    pub active_entry_bytes: u64,
}

impl DependencyState {
    pub fn validate(&self, job: &BackupJob, current: &Digest) -> StorageResult<()> {
        if self.format != 1
            || self.job != *job
            || self.current_sha256 != *current
            || self.entries > ENTRIES_MAX
            || self.native_covered.saturating_add(self.external_confirmed) > self.entries
            || self.catalog_bytes > cloud_backup_protocol::limits::DURABLE_LOCAL_SPOOL_MAX_BYTES
            || self.active_entry_bytes > ENTRY_MAX
            || (self.active_external.is_none() && self.active_entry_bytes != 0)
        {
            return Err(invalid("backup dependency state identity"));
        }
        if self
            .active_external
            .is_some_and(|index| index >= self.entries)
        {
            return Err(invalid("backup dependency active source identity"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct BackupFileDependencyStatus {
    pub certified: bool,
    pub entries: u64,
    pub native_covered: u64,
    pub external_needed: u64,
    pub external_prepared: u64,
    pub external_confirmed: u64,
    pub spool_bytes: u64,
}
