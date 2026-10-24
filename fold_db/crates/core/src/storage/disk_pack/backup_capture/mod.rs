//! Local backup capture with exact native bytes and complete file dependency proof.
//!
//! A session retains an exact source root across process exit. Its catalogue
//! contains frozen transfer identities and original native file extents. Explicit
//! file capture reuses native CAS bytes and copies authenticated external files.
//! This module does not activate a cloud worker or publish a backup.

mod assembly;
mod capture;
pub(crate) use assembly::AssemblySelection;
mod catalog;
mod dependencies;
mod dependency_confirm;
mod dependency_copy;
mod dependency_io;
mod dependency_native;
mod dependency_parts;
mod dependency_read;
mod dependency_reader;
mod dependency_state;
mod dependency_status;
mod dependency_store;
mod inspect;
mod reader;
mod state;
mod store;
mod terminal;

pub use catalog::{BackupCaptureFile, BackupCaptureObject, BackupCapturePage};
pub use dependencies::{AuthorizedFileSource, FileSource};
pub use dependency_parts::BackupFilePart;
pub use dependency_reader::NativeBackupFileReader;
pub use dependency_state::{BackupFileDependencyEntry, BackupFileDependencyStatus, FileCoverage};
pub use reader::NativeBackupObjectReader;
pub use state::{BackupCapturePhase, NativeBackupCaptureStatus};

use super::{PackRuntime, StorageError, StorageResult};
use cloud_backup_protocol::jobs::JobBeginInput;
use cloud_backup_protocol::publication::BackupHead;
use cloud_backup_protocol::{BackupJob, ProtocolError, RecoveryDescriptor};
use laststore::disk_pack::backup::export::SnapshotExport;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Caller-supplied immutable job identity and account recovery descriptor.
/// The selected native cut is supplied by the writer under its capture gate.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeBackupCaptureRequest {
    pub job: BackupJob,
    pub recovery: RecoveryDescriptor,
    pub predecessor: Option<BackupHead>,
}

/// One process-wide active source session. Drop leaves durable work retained.
pub struct NativeBackupCapture {
    inner: Arc<ExportInner>,
}

struct ExportInner {
    runtime: Arc<PackRuntime>,
    store: store::LocalStore,
    record: Mutex<state::ExportRecord>,
    source: Option<SnapshotExport>,
    source_error: Option<String>,
    readers: AtomicUsize,
    coordinator_spool_bytes: AtomicU64,
    assembly_spool_bytes: AtomicU64,
    dependency_work: Arc<tokio::sync::Mutex<()>>,
    _admission: Admission,
}

type Admission = super::NativeCloudWorkGuard;

impl NativeBackupCapture {
    pub fn status(&self) -> StorageResult<NativeBackupCaptureStatus> {
        let record = self
            .inner
            .record
            .lock()
            .map_err(|_| invalid("backup capture state lock failed"))?;
        let mut status = record.status(self.inner.readers.load(Ordering::Acquire));
        status.source_available = self.inner.source.is_some();
        status.source_error = self.inner.source_error.clone();
        if !matches!(
            record.phase,
            BackupCapturePhase::Terminal {
                source_released: true,
                ..
            }
        ) {
            status.retained_source_bytes = self
                .inner
                .source
                .as_ref()
                .map(SnapshotExport::bytes)
                .or_else(|| {
                    matches!(record.phase, BackupCapturePhase::Ready).then_some(record.native_bytes)
                });
        }
        status.last_accepted_backup = self.inner.store.accepted()?;
        status.spool_bytes = self.inner.store.spool_bytes(&record)?;
        if matches!(record.phase, BackupCapturePhase::Ready) && status.source_available {
            let store =
                dependency_store::DependencyStore::open(&self.inner.store, &record.begin.job)?;
            if let Some(state) =
                store.state(&record.begin.job, &record.begin.native_root.current_sha256)?
            {
                let coverage = dependency_status::status(&store, &state)?;
                status.spool_bytes += coverage.spool_bytes;
                status.payload_spool_bytes = coverage.payload_spool_bytes;
                if status.spool_bytes > cloud_backup_protocol::limits::DURABLE_LOCAL_SPOOL_MAX_BYTES
                {
                    return Err(invalid("backup capture combined spool exceeds its cap"));
                }
                status.complete_backup_eligible = coverage.certified
                    && coverage.external_needed == 0
                    && coverage.external_prepared == 0;
                status.blocked_reason = if status.complete_backup_eligible {
                    ""
                } else {
                    "backup file dependencies need complete verified copies"
                };
                status.file_dependencies = Some(coverage);
            }
        }
        Ok(status)
    }

    /// This frozen input also supplies the exact hash required by jobs/close.
    /// It is not a dependency-completeness certificate or activation decision.
    pub fn frozen_begin(&self) -> StorageResult<JobBeginInput> {
        let record = self
            .inner
            .record
            .lock()
            .map_err(|_| invalid("backup capture state lock failed"))?;
        Ok(record.begin.clone())
    }

    /// Read only the requested bounded catalogue page, from this exact job.
    pub fn page(&self, page_index: u32) -> StorageResult<BackupCapturePage> {
        let record = self
            .inner
            .record
            .lock()
            .map_err(|_| invalid("backup capture state lock failed"))?;
        record.require_ready()?;
        let descriptor = record
            .pages
            .get(page_index as usize)
            .ok_or_else(|| invalid("backup capture catalogue page does not exist"))?;
        self.inner.store.read_page(&record.begin.job, descriptor)
    }
}

fn protocol(error: ProtocolError) -> StorageError {
    let ProtocolError { code, field } = error;
    invalid(&format!("backup capture protocol {code:?}: {field}"))
}

fn invalid(detail: &str) -> StorageError {
    StorageError::InvalidOperation(detail.into())
}
