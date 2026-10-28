use super::*;
use cloud_backup_protocol::v2::{
    jobs::{JobResult, JobStatus},
    Commit,
};

impl MutationLogCapture {
    pub fn seal_commit(&self, commit: &Commit) -> StorageResult<()> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| invalid("mutation capture state owner"))?;
        commit
            .validate_for(&state.begin)
            .map_err(|_| invalid("mutation capture sealed commit input"))?;
        if !state.ready
            || state.local_cancelled
            || state.terminal.is_some()
            || state.released
            || state.commit.as_ref().is_some_and(|old| old != commit)
        {
            return Err(invalid("mutation capture commit phase or identity"));
        }
        let mut saved = state.clone();
        saved.commit = Some(commit.clone());
        store::write(&self.inner.directory.join("capture.json"), &saved)?;
        *state = saved;
        Ok(())
    }

    pub fn terminal(&self, result: &JobResult) -> StorageResult<()> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| invalid("mutation capture state owner"))?;
        if self.inner.readers.load(Ordering::Acquire) != 0 {
            return Err(invalid("mutation capture still has readers"));
        }
        if state.local_cancelled {
            return Err(invalid(
                "locally cancelled mutation capture has no cloud terminal",
            ));
        }
        state.validate_terminal(result)?;
        if state.terminal.as_ref().is_some_and(|old| old != result) {
            return Err(invalid("mutation capture terminal authority changed"));
        }
        if state.released {
            return Ok(());
        }
        if state.terminal.is_none() {
            let mut saved = state.clone();
            saved.terminal = Some(result.clone());
            store::write(&self.inner.directory.join("capture.json"), &saved)?;
            *state = saved;
        }
        if matches!(result.status, JobStatus::Published { .. }) {
            self.inner
                .runtime
                .mutation_log
                .acknowledge(&state.through)?;
        }
        for file in &state.files {
            store::remove_part(
                &self
                    .inner
                    .directory
                    .join(format!("{}.ldrun", file.run_id.as_str())),
                file.bytes,
            )?;
        }
        File::open(&self.inner.directory)?.sync_all()?;
        let mut saved = state.clone();
        saved.released = true;
        store::write(&self.inner.directory.join("capture.json"), &saved)?;
        *state = saved;
        Ok(())
    }

    /// Only the coordinator's durable never-submitted branch can use this path.
    pub fn cancel_unsubmitted(&self) -> StorageResult<()> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| invalid("mutation capture state owner"))?;
        if self.inner.readers.load(Ordering::Acquire) != 0
            || state.terminal.is_some()
            || state.commit.is_some()
        {
            return Err(invalid("mutation capture cannot be locally cancelled"));
        }
        if state.released {
            return Ok(());
        }
        let mut saved = state.clone();
        saved.local_cancelled = true;
        store::write(&self.inner.directory.join("capture.json"), &saved)?;
        *state = saved;
        for file in &state.files {
            store::remove_part(
                &self
                    .inner
                    .directory
                    .join(format!("{}.ldrun", file.run_id.as_str())),
                file.bytes,
            )?;
        }
        File::open(&self.inner.directory)?.sync_all()?;
        let mut saved = state.clone();
        saved.released = true;
        store::write(&self.inner.directory.join("capture.json"), &saved)?;
        *state = saved;
        Ok(())
    }
}
