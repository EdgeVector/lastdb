//! Local native export source. No cloud transport or activation lives here.
//!
//! A session retains an exact source root across process exit. Its catalogue
//! contains frozen transfer identities and original native file extents. File
//! dependency completeness is deliberately uncertified until the B2 owner adds
//! the atomic native dependency directory and backup-owned blob copies.

mod capture;
mod catalog;
mod reader;
mod state;
mod store;
mod terminal;

pub use catalog::{ExportFile, ExportObject, ExportPage};
pub use reader::NativeCloudObjectReader;
pub use state::{ExportPhase, NativeCloudExportStatus};

use super::{PackRuntime, StorageError, StorageResult};
use cloud_backup_protocol::jobs::JobBeginInput;
use cloud_backup_protocol::publication::BackupHead;
use cloud_backup_protocol::{BackupJob, ProtocolError, RecoveryDescriptor};
use laststore::disk_pack::backup::export::SnapshotExport;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Caller-supplied immutable job identity and account recovery descriptor.
/// The selected native cut is supplied by the writer under its capture gate.
#[derive(Clone, Debug)]
pub struct NativeCloudExportRequest {
    pub job: BackupJob,
    pub recovery: RecoveryDescriptor,
    pub predecessor: Option<BackupHead>,
}

/// One process-wide active source session. Drop leaves durable work retained.
pub struct NativeCloudExport {
    inner: Arc<ExportInner>,
}

struct ExportInner {
    runtime: Arc<PackRuntime>,
    store: store::LocalStore,
    record: Mutex<state::ExportRecord>,
    source: Option<SnapshotExport>,
    source_error: Option<String>,
    readers: AtomicUsize,
    _admission: Admission,
}

static ADMITTED: AtomicBool = AtomicBool::new(false);

struct Admission;

impl Admission {
    fn acquire() -> StorageResult<Self> {
        ADMITTED
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| invalid("one native cloud export session is already active"))?;
        Ok(Self)
    }
}

impl Drop for Admission {
    fn drop(&mut self) {
        ADMITTED.store(false, Ordering::Release);
    }
}

impl NativeCloudExport {
    pub fn status(&self) -> StorageResult<NativeCloudExportStatus> {
        let record = self
            .inner
            .record
            .lock()
            .map_err(|_| invalid("native export state lock failed"))?;
        let mut status = record.status(self.inner.readers.load(Ordering::Acquire));
        status.source_available = self.inner.source.is_some();
        status.source_error = self.inner.source_error.clone();
        if !matches!(
            record.phase,
            ExportPhase::Terminal {
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
                    matches!(record.phase, ExportPhase::Ready).then_some(record.native_bytes)
                });
        }
        status.last_accepted_backup = self.inner.store.accepted()?;
        status.spool_bytes = self.inner.store.spool_bytes(&record)?;
        Ok(status)
    }

    /// This frozen input also supplies the exact hash required by jobs/close.
    /// It is not a dependency-completeness certificate or activation decision.
    pub fn frozen_begin(&self) -> StorageResult<JobBeginInput> {
        let record = self
            .inner
            .record
            .lock()
            .map_err(|_| invalid("native export state lock failed"))?;
        Ok(record.begin.clone())
    }

    /// Read only the requested bounded catalogue page, from this exact job.
    pub fn page(&self, page_index: u32) -> StorageResult<ExportPage> {
        let record = self
            .inner
            .record
            .lock()
            .map_err(|_| invalid("native export state lock failed"))?;
        record.require_ready()?;
        let descriptor = record
            .pages
            .get(page_index as usize)
            .ok_or_else(|| invalid("native export catalogue page does not exist"))?;
        self.inner.store.read_page(&record.begin.job, descriptor)
    }
}

fn protocol(error: ProtocolError) -> StorageError {
    let ProtocolError { code, field } = error;
    invalid(&format!("native cloud export protocol {code:?}: {field}"))
}

fn invalid(detail: &str) -> StorageError {
    StorageError::InvalidOperation(detail.into())
}
