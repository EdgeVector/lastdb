//! Only a never-submitted request without a frozen native capture can be replaced.
use super::*;
use std::path::Path;

impl PackRuntime {
    pub fn mutation_log_capture_materialized(&self, request: &Id) -> StorageResult<bool> {
        let root = self.mutation_log.root.join("captures");
        let state: Option<FrozenLogCapture> =
            store::read(&root.join(request.as_str()).join("capture.json"))?;
        if let Some(state) = state {
            state.validate()?;
            if state.begin.job.commit_id != *request
                || state.anchor.main_home != self.mutation_log.main_home
                || state.anchor.cas_home != self.mutation_log.cas_home
            {
                return Err(invalid("mutation capture replacement identity"));
            }
            return Ok(true);
        }
        Ok(false)
    }

    /// The coordinator holds its owner lock and saves the local-only intent first.
    pub fn cancel_unmaterialized_mutation_log_capture(&self, request: &Id) -> StorageResult<()> {
        let _admission = super::super::super::NativeCloudWorkGuard::acquire()?;
        if self.mutation_log_capture_materialized(request)? {
            return Err(invalid("frozen mutation capture cannot be replaced"));
        }
        cancel(&self.mutation_log.root.join("captures"), request)
    }
}

pub(super) fn cancel(root: &Path, request: &Id) -> StorageResult<()> {
    let directory = root.join(request.as_str());
    store::remove_metadata(&directory.join("capture.pending"))?;
    // Unknown files, including native links, prevent removal.
    store::remove_empty(&directory)?;
    if root.try_exists()? {
        File::open(root)?.sync_all()?;
    }
    retirement::cancel_absent(root, request)
}
