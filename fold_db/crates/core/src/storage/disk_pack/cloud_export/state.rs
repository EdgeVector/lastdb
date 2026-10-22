use super::{invalid, protocol, StorageResult};
use cloud_backup_protocol::jobs::{JobBeginInput, JobResult, JobStatus};
use cloud_backup_protocol::{DecimalU64, Digest};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExportPhase {
    Preparing,
    Ready,
    Terminal {
        result: Box<JobResult>,
        source_released: bool,
    },
}

#[derive(Clone, Debug, Serialize)]
pub struct NativeCloudExportStatus {
    pub begin: JobBeginInput,
    pub phase: ExportPhase,
    pub native_files: u64,
    pub native_bytes: u64,
    pub retained_source_bytes: Option<u64>,
    pub source_available: bool,
    pub source_error: Option<String>,
    pub last_accepted_backup: Option<cloud_backup_protocol::publication::AcceptedBackup>,
    pub spool_bytes: u64,
    pub object_count: u64,
    pub page_count: usize,
    pub active_readers: usize,
    pub complete_backup_eligible: bool,
    pub blocked_reason: &'static str,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ExportRecord {
    pub format: u16,
    pub begin: JobBeginInput,
    pub reference_id: uuid::Uuid,
    pub current_bytes: Vec<u8>,
    pub phase: ExportPhase,
    pub pages: Vec<PageDescriptor>,
    pub native_files: u64,
    pub native_bytes: u64,
    pub spool_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PageDescriptor {
    pub page_index: u32,
    pub sha256: Digest,
    pub bytes: DecimalU64,
    pub objects: u64,
    pub files: u64,
}

impl ExportRecord {
    pub fn validate(&self) -> StorageResult<()> {
        use cloud_backup_protocol::limits::*;
        self.begin.validate().map_err(protocol)?;
        if self.format != 1
            || self.reference_id.is_nil()
            || self.current_bytes.len() != 4096
            || self.pages.len() > MANIFEST_PAGES_MAX
            || self.native_files > NATIVE_FILES_MAX as u64
            || self.native_bytes > BACKUP_STORED_BYTES_MAX
            || self.spool_bytes > DURABLE_LOCAL_SPOOL_MAX_BYTES
        {
            return Err(invalid("native export saved record bounds"));
        }
        let mut files = 0;
        let mut spool = 0;
        for (index, page) in self.pages.iter().enumerate() {
            if page.page_index as usize != index
                || page.objects == 0
                || page.files < page.objects
                || page.files > 512
                || page.bytes.get() == 0
                || page.bytes.get() > super::store::PAGE_MAX
            {
                return Err(invalid("native export saved page bounds"));
            }
            files += page.files;
            spool += page.bytes.get();
        }
        if files != self.native_files || spool != self.spool_bytes {
            return Err(invalid("native export saved page totals"));
        }
        match &self.phase {
            ExportPhase::Ready if self.pages.is_empty() || self.native_bytes == 0 => {
                return Err(invalid("native export ready without a complete catalogue"))
            }
            ExportPhase::Terminal { result, .. } => {
                result.validate_for_begin(&self.begin).map_err(protocol)?;
                if matches!(result.status, JobStatus::Active) {
                    return Err(invalid("active cloud job cannot release its native source"));
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub fn require_ready(&self) -> StorageResult<()> {
        if !matches!(self.phase, ExportPhase::Ready) {
            return Err(invalid(
                "native export source is not ready or its job is terminal",
            ));
        }
        Ok(())
    }

    pub fn status(&self, active_readers: usize) -> NativeCloudExportStatus {
        let released = matches!(
            self.phase,
            ExportPhase::Terminal {
                source_released: true,
                ..
            }
        );
        NativeCloudExportStatus {
            begin: self.begin.clone(), phase: self.phase.clone(),
            native_files: self.native_files, native_bytes: self.native_bytes,
            retained_source_bytes: if released { Some(0) } else { None },
            source_available: false,
            source_error: None,
            last_accepted_backup: None,
            spool_bytes: self.spool_bytes,
            object_count: self.pages.iter().map(|page| page.objects).sum(),
            page_count: self.pages.len(), active_readers,
            complete_backup_eligible: false,
            blocked_reason: "native file dependency completeness requires B2 certification; cloud activation stays disabled",
        }
    }
}
