//! Fixed-size transport extents over one authenticated original ciphertext.
use super::dependencies::{ready, selection};
use super::dependency_state::{BackupFileDependencyEntry, FileCoverage, ENTRY_MAX};
use super::dependency_store::{optional_size, DependencyStore};
use super::{invalid, protocol, NativeBackupCapture, StorageResult};
use cloud_backup_protocol::limits::{CLOUD_OBJECT_MAX_BYTES, MANIFEST_PAGE_ENTRIES_MAX};
use cloud_backup_protocol::transfer::{ObjectReceipt, TransferInput};
use cloud_backup_protocol::{
    BackupJob, DecimalU64, Digest, ExternalFileDependency, Id, ObjectExtent, ObjectIdentity,
    ObjectKind, Text, Version,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::fs::File;
use std::io::{Read, Seek};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupFilePart {
    pub job: BackupJob,
    pub dependency: u64,
    pub part: u16,
    pub offset: u64,
    pub input: TransferInput,
    pub input_sha256: Digest,
    pub receipt: Option<ObjectReceipt>,
}

impl BackupFilePart {
    pub(super) fn validate(&self, entry: &BackupFileDependencyEntry) -> StorageResult<()> {
        self.input.validate().map_err(protocol)?;
        let offset = u64::from(self.part) * CLOUD_OBJECT_MAX_BYTES;
        let length = entry
            .dependency
            .expected_stored_bytes
            .saturating_sub(offset)
            .min(CLOUD_OBJECT_MAX_BYTES);
        if self.job != entry.job
            || self.input.job != entry.job
            || self.dependency != entry.index
            || self.offset != offset
            || length == 0
            || self.part as usize >= MANIFEST_PAGE_ENTRIES_MAX
            || self.input.object.kind != ObjectKind::BlobPart
            || self.input.object.bytes.get() != length
            || self.input_sha256 != self.input.input_sha256().map_err(protocol)?
        {
            return Err(invalid("backup file part identity"));
        }
        if let Some(receipt) = &self.receipt {
            receipt.validate().map_err(protocol)?;
            if receipt.scope != entry.job.stream.scope || receipt.object != self.input.object {
                return Err(invalid("backup file part receipt identity"));
            }
        }
        Ok(())
    }
}

impl DependencyStore {
    pub(super) fn part_path(&self, index: u64, part: u16) -> std::path::PathBuf {
        self.root.join(format!("chunk-{index:06}-{part:03}.json"))
    }
    pub(super) fn part(
        &self,
        entry: &BackupFileDependencyEntry,
        part: u16,
    ) -> StorageResult<BackupFilePart> {
        let value: BackupFilePart = self.read(&self.part_path(entry.index, part))?;
        value.validate(entry)?;
        Ok(value)
    }
    pub(super) fn save_part(
        &self,
        entry: &BackupFileDependencyEntry,
        part: &BackupFilePart,
    ) -> StorageResult<()> {
        part.validate(entry)?;
        self.write(&self.part_path(entry.index, part.part), part)
    }
    pub(super) fn part_bytes(&self, index: u64, parts: u16) -> StorageResult<u64> {
        if parts == 0 {
            return Ok(0);
        }
        let first = self.part_path(index, 0);
        if !first.try_exists()? && !first.with_extension("pending").try_exists()? {
            return Ok(0);
        }
        let mut bytes = 0u64;
        for part in 0..parts {
            let path = self.part_path(index, part);
            bytes = bytes
                .checked_add(
                    optional_size(&path)? + optional_size(&path.with_extension("pending"))?,
                )
                .ok_or_else(|| invalid("backup part metadata overflow"))?;
        }
        Ok(bytes)
    }
}

impl NativeBackupCapture {
    pub fn file_part(&self, index: u64, part: u16) -> StorageResult<BackupFilePart> {
        let (job, current) = selection(&self.inner)?;
        let store = DependencyStore::open(&self.inner.store, &job)?;
        let state = ready(&store, &job, &current)?;
        if index >= state.entries {
            return Err(invalid("backup dependency does not exist"));
        }
        let entry = store.entry(&job, index)?;
        match &entry.coverage {
            FileCoverage::BackupOwnedExtents { parts, .. } if part < *parts => {
                store.part(&entry, part)
            }
            FileCoverage::BackupOwned {
                input,
                input_sha256,
                receipt,
            } if part == 0 => Ok(BackupFilePart {
                job,
                dependency: index,
                part,
                offset: 0,
                input: (**input).clone(),
                input_sha256: input_sha256.clone(),
                receipt: receipt.clone(),
            }),
            _ => Err(invalid("backup file has no selected frozen part")),
        }
    }

    /// Assemble only verified receipt extents. The bounded list contains no payload bytes.
    pub fn external_file_dependency(
        &self,
        index: u64,
    ) -> StorageResult<Option<ExternalFileDependency>> {
        let (job, current) = selection(&self.inner)?;
        let store = DependencyStore::open(&self.inner.store, &job)?;
        let state = ready(&store, &job, &current)?;
        if index >= state.entries {
            return Err(invalid("backup dependency does not exist"));
        }
        let entry = store.entry(&job, index)?;
        let FileCoverage::BackupOwnedExtents {
            stored_source_sha256,
            stored_source_bytes,
            parts,
            confirmed,
        } = &entry.coverage
        else {
            return entry.external_entry();
        };
        if confirmed != parts {
            return Err(invalid("backup file needs complete verified part receipts"));
        }
        let mut extents = Vec::with_capacity(*parts as usize);
        for part in 0..*parts {
            let saved = store.part(&entry, part)?;
            let receipt = saved
                .receipt
                .ok_or_else(|| invalid("backup file part receipt is missing"))?;
            extents.push(ObjectExtent {
                receipt_id: receipt.receipt_id,
                offset: DecimalU64::new(0),
                length: saved.input.object.bytes,
            });
        }
        let dependency = ExternalFileDependency {
            kind: entry.dependency.kind,
            logical_pointer_sha256: entry.dependency.logical_pointer_sha256,
            stored_source_sha256: stored_source_sha256.clone(),
            stored_source_bytes: *stored_source_bytes,
            encryption_suite: Text::try_from(entry.dependency.encryption_suite)
                .map_err(protocol)?,
            extents,
        };
        dependency.validate().map_err(protocol)?;
        Ok(Some(dependency))
    }
}

pub(super) fn metadata_reserve() -> u64 {
    2 * ENTRY_MAX * MANIFEST_PAGE_ENTRIES_MAX as u64
}

/// Freeze each exact extent only after complete source authentication. Existing
/// part identities survive retries; a different source can never replace them.
pub(super) fn freeze(
    store: &DependencyStore,
    entry: &BackupFileDependencyEntry,
    file: &mut File,
    expected: &Digest,
) -> StorageResult<()> {
    file.rewind()?;
    let mut buffer = vec![0; lastdb_storage_policy::BACKUP_RESOURCES.stream_buffer_bytes as usize];
    let count = entry
        .dependency
        .expected_stored_bytes
        .div_ceil(CLOUD_OBJECT_MAX_BYTES);
    let mut complete_hash = Sha256::new();
    for part in 0..count as u16 {
        let offset = u64::from(part) * CLOUD_OBJECT_MAX_BYTES;
        let length = (entry.dependency.expected_stored_bytes - offset).min(CLOUD_OBJECT_MAX_BYTES);
        let mut remaining = length;
        let mut hash = Sha256::new();
        while remaining > 0 {
            let count = remaining.min(buffer.len() as u64) as usize;
            file.read_exact(&mut buffer[..count])?;
            hash.update(&buffer[..count]);
            complete_hash.update(&buffer[..count]);
            remaining -= count as u64;
        }
        let object = ObjectIdentity {
            sha256: Digest::try_from(crate::hex::hex_lower(hash.finalize())).map_err(protocol)?,
            bytes: DecimalU64::new(length),
            kind: ObjectKind::BlobPart,
        };
        let path = store.part_path(entry.index, part);
        if path.try_exists()? {
            let prior = store.part(entry, part)?;
            if prior.input.object != object {
                return Err(invalid("backup file differs from frozen part"));
            }
        } else {
            let input = TransferInput {
                version: Version::V1,
                request_id: Id::try_from(uuid::Uuid::new_v4().to_string()).map_err(protocol)?,
                job: entry.job.clone(),
                object,
            };
            let value = BackupFilePart {
                job: entry.job.clone(),
                dependency: entry.index,
                part,
                offset,
                input_sha256: input.input_sha256().map_err(protocol)?,
                input,
                receipt: None,
            };
            store.save_part(entry, &value)?;
        }
    }
    let mut extra = [0];
    if file.read(&mut extra)? != 0 {
        return Err(invalid("backup file changed during extent capture"));
    }
    if crate::hex::hex_lower(complete_hash.finalize()) != expected.as_str() {
        return Err(invalid("backup file changed after authentication"));
    }
    Ok(())
}
