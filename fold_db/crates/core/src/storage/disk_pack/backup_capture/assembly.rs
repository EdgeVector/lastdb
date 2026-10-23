//! Exact source selection and shared metadata admission for manifest assembly.
use super::dependencies::{ready, selection};
use super::dependency_state::FileCoverage;
use super::dependency_store::DependencyStore;
use super::{
    invalid, protocol, BackupCaptureObject, BackupCapturePage, NativeBackupCapture, StorageResult,
};
use cloud_backup_protocol::jobs::JobBeginInput;
use cloud_backup_protocol::transfer::ObjectReceipt;
use cloud_backup_protocol::{Digest, ExternalFileDependency};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AssemblySelection {
    pub begin: JobBeginInput,
    pub catalogue_sha256: Digest,
    pub native_files: u64,
    pub native_bytes: u64,
    pub native_objects: u64,
    pub dependencies: u64,
    pub external_dependencies: u64,
}

impl NativeBackupCapture {
    /// Conservative capture-only metadata size without a catalogue file census.
    /// The active file reserves every possible part record before its upload.
    pub fn metadata_spool_bytes(&self) -> StorageResult<u64> {
        let record = self
            .inner
            .record
            .lock()
            .map_err(|_| invalid("backup capture state lock failed"))?;
        let store = DependencyStore {
            root: self.inner.store.root().join(format!(
                "dependencies-{}",
                record.begin.job.commit_id.as_str()
            )),
        };
        let files = store.state(&record.begin.job, &record.begin.native_root.current_sha256)?;
        let mut bytes = record.spool_bytes;
        if let Some(state) = files {
            bytes = bytes
                .checked_add(state.catalog_bytes)
                .and_then(|bytes| {
                    bytes.checked_add(if state.active_external.is_some() {
                        super::dependency_parts::metadata_reserve()
                    } else {
                        0
                    })
                })
                .ok_or_else(|| invalid("capture metadata spool overflow"))?;
        }
        bytes
            .checked_add(1024 * 1024)
            .ok_or_else(|| invalid("capture metadata spool overflow"))
    }

    pub(crate) fn reserve_assembly_output(
        &self,
        bytes: u64,
    ) -> StorageResult<laststore::disk_pack::maintenance::SpaceReservation> {
        self.inner
            .runtime
            .writer
            .lock()
            .map_err(|_| invalid("backup writer lock failed"))?
            .reserve_space(bytes, &self.inner.runtime.space_policy)
            .map_err(super::super::map)
    }
    pub fn set_coordinator_spool_bytes(&self, bytes: u64) -> StorageResult<()> {
        if bytes > lastdb_storage_policy::BACKUP_RESOURCES.durable_spool_bytes {
            return Err(invalid("coordinator spool exceeds backup policy"));
        }
        self.inner
            .coordinator_spool_bytes
            .store(bytes, std::sync::atomic::Ordering::Release);
        self.admit_assembly_metadata(
            self.inner
                .assembly_spool_bytes
                .load(std::sync::atomic::Ordering::Acquire),
        )
    }
    pub(crate) fn track_assembly_metadata(&self, bytes: u64) -> StorageResult<()> {
        self.inner
            .assembly_spool_bytes
            .store(bytes, std::sync::atomic::Ordering::Release);
        self.admit_assembly_metadata(bytes)
    }
    pub(crate) fn assembly_selection(&self) -> StorageResult<AssemblySelection> {
        let (job, current) = selection(&self.inner)?;
        let store = DependencyStore::open(&self.inner.store, &job)?;
        let files = ready(&store, &job, &current)?;
        let record = self
            .inner
            .record
            .lock()
            .map_err(|_| invalid("backup capture state lock failed"))?;
        Ok(AssemblySelection {
            begin: record.begin.clone(),
            catalogue_sha256: cloud_backup_protocol::input_digest(
                "lastdb:manifest-source:v1",
                &record.pages,
            )
            .map_err(protocol)?,
            native_files: record.native_files,
            native_bytes: record.native_bytes,
            native_objects: record.pages.iter().map(|page| page.objects).sum(),
            dependencies: files.entries,
            external_dependencies: files.entries - files.native_covered,
        })
    }

    pub(crate) fn assembly_directory(&self) -> StorageResult<PathBuf> {
        let job = self.frozen_begin()?.job;
        Ok(self
            .inner
            .store
            .root()
            .join(format!("assembly-{}", job.commit_id.as_str())))
    }

    pub(crate) fn assembly_native(
        &self,
        ordinal: u64,
        cache: &mut Option<BackupCapturePage>,
    ) -> StorageResult<BackupCaptureObject> {
        let (page_index, offset) = {
            let record = self
                .inner
                .record
                .lock()
                .map_err(|_| invalid("backup capture state lock failed"))?;
            record.require_ready()?;
            let mut start = 0;
            let mut found = None;
            for page in &record.pages {
                if ordinal >= start && ordinal - start < page.objects {
                    found = Some((page.page_index, (ordinal - start) as usize));
                    break;
                }
                start += page.objects;
            }
            found.ok_or_else(|| invalid("native capture object ordinal"))?
        };
        if cache
            .as_ref()
            .is_none_or(|page| page.page_index != page_index)
        {
            *cache = Some(self.page(page_index)?);
        }
        cache
            .as_ref()
            .and_then(|page| page.objects.get(offset))
            .cloned()
            .ok_or_else(|| invalid("native capture object coordinate"))
    }

    pub(crate) fn assembly_external(
        &self,
        index: u64,
    ) -> StorageResult<Option<(ExternalFileDependency, Vec<ObjectReceipt>)>> {
        let entry = self.file_dependency(index)?;
        let Some(dependency) = self.external_file_dependency(index)? else {
            return Ok(None);
        };
        let parts = match entry.coverage {
            FileCoverage::BackupOwned { .. } => 1,
            FileCoverage::BackupOwnedExtents { parts, .. } => parts,
            _ => return Err(invalid("external capture lacks verified source")),
        };
        let mut receipts = Vec::with_capacity(parts as usize);
        for part in 0..parts {
            receipts.push(
                self.file_part(index, part)?
                    .receipt
                    .ok_or_else(|| invalid("external capture part receipt"))?,
            );
        }
        Ok(Some((dependency, receipts)))
    }

    pub(crate) fn admit_assembly_metadata(&self, assembly_bytes: u64) -> StorageResult<()> {
        let total = assembly_bytes
            .checked_add(self.metadata_spool_bytes()?)
            .and_then(|bytes| {
                bytes.checked_add(
                    self.inner
                        .coordinator_spool_bytes
                        .load(std::sync::atomic::Ordering::Acquire),
                )
            })
            .ok_or_else(|| invalid("combined backup metadata overflow"))?;
        if total > lastdb_storage_policy::BACKUP_RESOURCES.durable_spool_bytes {
            return Err(invalid("combined backup metadata spool exceeds policy"));
        }
        Ok(())
    }
}
