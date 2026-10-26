use super::{invalid, protocol, ByteBackupCapture, StorageResult};
use cloud_backup_protocol::v2::jobs::{JobResult, JobStatus};

impl ByteBackupCapture {
    /// Only authenticated exact terminal authority releases a submitted job.
    /// A timeout, transfer rejection or missing remote record never does so.
    pub fn terminal(&self, result: &JobResult) -> StorageResult<()> {
        let mut state = self.lock()?;
        self.inner.require_idle()?;
        if state.local_cancelled {
            return Err(invalid(
                "byte locally cancelled source has no cloud terminal",
            ));
        }
        result.validate_for_begin(&state.begin).map_err(protocol)?;
        if matches!(result.status, JobStatus::Active) {
            return Err(invalid("byte source active result is not terminal"));
        }
        if let Some(prior) = &state.terminal {
            if prior != result {
                return Err(invalid("byte source terminal result changed"));
            }
        } else {
            if let JobStatus::Published { backup } = &result.status {
                self.inner.advance(&mut state)?;
                if !state.ready
                    || state.verified != state.objects
                    || backup.root_receipt_id
                        != super::planner::expected_receipt(
                            &self.inner.object(&state, state.objects - 1)?.input,
                        )?
                        .receipt_id
                {
                    return Err(invalid("byte accepted result does not cover source"));
                }
            }
            let mut confirmed = state.clone();
            confirmed.terminal = Some(result.clone());
            self.inner.store.write("job.json", &confirmed)?;
            *state = confirmed;
        }
        drop(state);
        self.finish_terminal()
    }
    pub(super) fn finish_terminal(&self) -> StorageResult<()> {
        let mut state = self.lock()?;
        if state.terminal.is_none() && !state.local_cancelled {
            return Ok(());
        }
        self.inner.require_idle()?;
        if !state.released {
            let root = super::run_capture::held_root(&self.inner.runtime, state.reference_id);
            super::run_source::RunSource::release_saved(&root)?;
            self.inner.store.remove("run-intent.json")?;
            let mut released = state.clone();
            released.released = true;
            self.inner.store.write("job.json", &released)?;
            *state = released;
        }
        super::capture::clear_previous(&self.inner.store, &state)
    }
}
