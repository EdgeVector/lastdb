use super::state::ExportPhase;
use super::{invalid, protocol, NativeCloudExport, StorageResult};
use cloud_backup_protocol::jobs::{CloseReason, JobCloseRequest, JobResult, JobStatus};
use cloud_backup_protocol::Version;
use laststore::disk_pack::lifetime::{release_owned_reference, ReferencePurpose};
use std::sync::atomic::Ordering;

impl NativeCloudExport {
    /// NotFound, a timeout, and a rejected transfer never release the source.
    /// Send this exact request to the authority if begin has an uncertain result.
    pub fn close_request(&self, reason: CloseReason) -> StorageResult<JobCloseRequest> {
        let record = self
            .inner
            .record
            .lock()
            .map_err(|_| invalid("native export state lock failed"))?;
        let request = JobCloseRequest {
            version: Version::V1,
            job: record.begin.job.clone(),
            job_input_sha256: record.begin.input_sha256().map_err(protocol)?,
            reason,
        };
        request.validate().map_err(protocol)?;
        Ok(request)
    }

    /// The caller supplies the authoritative authenticated job result. Save its
    /// exact terminal outcome before source release. This call performs no
    /// remote request and cannot infer closure from absence.
    pub fn confirm_terminal(&self, result: JobResult) -> StorageResult<()> {
        let mut record = self
            .inner
            .record
            .lock()
            .map_err(|_| invalid("native export state lock failed"))?;
        result.validate_for_begin(&record.begin).map_err(protocol)?;
        if matches!(result.status, JobStatus::Active) {
            return Err(invalid(
                "active cloud result cannot release the native source",
            ));
        }
        if let ExportPhase::Terminal {
            result: previous, ..
        } = &record.phase
        {
            if previous.as_ref() != &result {
                return Err(invalid("native export terminal result changed"));
            }
        } else {
            let mut confirmed = record.clone();
            confirmed.phase = ExportPhase::Terminal {
                result: Box::new(result),
                source_released: false,
            };
            // If this barrier fails, the durable reference stays in place.
            self.inner.store.save(&confirmed)?;
            *record = confirmed;
        }
        drop(record);
        self.finish_terminal_release()
    }

    /// Retry after readers stop or a local barrier fails. A restart also calls
    /// this method. An already released exact reference is idempotent only
    /// because the terminal authority result is already durable locally.
    pub fn finish_terminal_release(&self) -> StorageResult<()> {
        let mut record = self
            .inner
            .record
            .lock()
            .map_err(|_| invalid("native export state lock failed"))?;
        if matches!(
            record.phase,
            ExportPhase::Terminal {
                source_released: true,
                ..
            }
        ) {
            return self.inner.store.cleanup_pages(&record);
        }
        if !matches!(record.phase, ExportPhase::Terminal { .. }) {
            return Ok(());
        }
        if self.inner.readers.load(Ordering::Acquire) != 0 {
            return Err(invalid(
                "native export terminal result saved; source readers must stop before release",
            ));
        }
        if let ExportPhase::Terminal { result, .. } = &record.phase {
            if let JobStatus::Published { backup } = &result.status {
                self.inner.store.save_accepted(backup)?;
            }
        }
        let mut current_digest = [0; 32];
        let text = record.begin.native_root.current_sha256.as_str();
        for (index, byte) in current_digest.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16)
                .map_err(|_| invalid("native export CURRENT digest is not hexadecimal"))?;
        }
        release_owned_reference(
            self.inner.runtime.home.path(),
            record.reference_id,
            ReferencePurpose::CloudRecovery,
            &current_digest,
        )
        .map_err(super::super::map)?;
        let mut released = record.clone();
        if let ExportPhase::Terminal {
            source_released, ..
        } = &mut released.phase
        {
            *source_released = true;
        }
        self.inner.store.save(&released)?;
        *record = released;
        // These are catalogue pages only. Original native files are reclaimed
        // by the native writer from its ordinary retained-generation rules.
        self.inner.store.cleanup_pages(&record)
    }
}
