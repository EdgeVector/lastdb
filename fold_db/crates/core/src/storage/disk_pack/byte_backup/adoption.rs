//! A held capture supplies a stable local cut for read-only adoption metadata.

use super::{invalid, ByteBackupCapture, StorageResult};
pub use crate::storage::disk_pack::logical_capture::{
    SnapshotAdoptionCandidate, SnapshotAdoptionPlan,
};

impl ByteBackupCapture {
    pub fn snapshot_adoption_plan(
        &self,
        candidate: &SnapshotAdoptionCandidate,
    ) -> StorageResult<SnapshotAdoptionPlan> {
        // Cancellation and new stream admission use this same lock. No await
        // occurs while the bounded native reads retain the captured files.
        let state = self.lock()?;
        if !state.ready || state.released || state.local_cancelled || state.terminal.is_some() {
            return Err(invalid(
                "snapshot adoption needs an active retained capture",
            ));
        }
        self.inner.require_idle()?;
        self.require_export(&state)?;
        state.validate()?;
        let local = state
            .logical_inclusion
            .as_ref()
            .ok_or_else(|| invalid("snapshot adoption refuses legacy unknown coverage"))?;
        self.inner
            .run
            .as_ref()
            .ok_or_else(|| invalid("snapshot adoption retained source is unavailable"))?
            .snapshot_adoption_plan(local, candidate)
    }
}
