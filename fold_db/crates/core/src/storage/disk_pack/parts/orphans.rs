//! The sweep for part files that no receipt names (see `laststore` reclaim).

use super::*;
use laststore::space::SpacePolicy;
use tracing::Instrument;

impl PackRuntime {
    /// Remove part files that no receipt names and that are older than `min_age_ms`.
    /// Returns the files and bytes removed, and whether active reads deferred the sweep.
    /// Capture uses only the writer mutex; inventory and deletion need no node lock.
    pub(super) async fn sweep_orphans_native(
        self: &Arc<Self>,
        min_age_ms: u64,
    ) -> StorageResult<(u64, u64, bool)> {
        let runtime = Arc::clone(self);
        tokio::spawn(
            async move { run_blocking(move || runtime.sweep_orphans_blocking(min_age_ms)).await }
                .in_current_span(),
        )
        .await
        .map_err(|error| StorageError::BackendError(format!("native orphan owner: {error}")))?
    }

    fn sweep_orphans_blocking(&self, min_age_ms: u64) -> StorageResult<(u64, u64, bool)> {
        self.require_initialized()?;
        if self.failed.load(Ordering::Acquire) {
            return Err(StorageError::BackendError(
                "native orphan sweep requires complete recovery of the failed writer".into(),
            ));
        }
        let policy = SpacePolicy {
            grace_ms: min_age_ms,
            ..self.space_policy.clone()
        };
        let (_, _, plan, _) = self.capture_native_cleanup()?;
        let receipt = plan.execute(&policy, true).map_err(map)?;
        Ok((
            receipt.files_removed,
            receipt.bytes_removed,
            receipt.reclaim_deferred,
        ))
    }
}
