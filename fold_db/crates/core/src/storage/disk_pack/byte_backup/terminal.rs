use super::{invalid, protocol, ByteBackupCapture, StorageResult};
use cloud_backup_protocol::v2::jobs::{JobResult, JobStatus};
use laststore::disk_pack::lifetime::{release_owned_reference, ReferencePurpose};

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
            self.inner.home.validate()?;
            let mut digest = [0u8; 32];
            for (index, out) in digest.iter_mut().enumerate() {
                *out = u8::from_str_radix(
                    &state.native_root.current_sha256.as_str()[index * 2..index * 2 + 2],
                    16,
                )
                .map_err(|_| invalid("byte source CURRENT digest"))?;
            }
            release_owned_reference(
                self.inner.home.home().path(),
                state.reference_id,
                ReferencePurpose::CloudRecovery,
                &digest,
            )
            .map_err(super::super::map)?;
            let mut released = state.clone();
            released.released = true;
            self.inner.store.write("job.json", &released)?;
            *state = released;
        }
        super::certify::scratch_cleanup(&self.inner.store.root)?;
        super::capture::clear_previous(&self.inner.store, &state)
    }
}
