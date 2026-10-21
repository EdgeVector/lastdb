//! The sweep for part files that no receipt names (see `laststore` reclaim).

use super::*;
use crate::storage::laststore::LastStoreKvStore;
use laststore::disk_pack::maintenance::SpacePolicy;

impl PackRuntime {
    /// Remove part files that no receipt names and that are older than `min_age_ms`.
    /// Returns the files and bytes removed, and whether active reads deferred the sweep.
    /// It holds the write gate and the view lock for the scan, so the worker calls it
    /// once after a drain, not for each write.
    pub(super) async fn sweep_orphans_native(
        self: &Arc<Self>,
        min_age_ms: u64,
    ) -> StorageResult<(u64, u64, bool)> {
        let gate = self.enter_write().await;
        let runtime = Arc::clone(self);
        LastStoreKvStore::run_blocking(move || {
            let _gate = gate;
            runtime.sweep_orphans_blocking(min_age_ms)
        })
        .await
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
        let mut writer = self.journal.lock().expect("native journal owner");
        // Keep new reads out while the sweep excludes the reads that already run.
        let mut view = self.view.write().expect("native complete read view");
        if !view.accepted.is_empty() {
            return Err(StorageError::BackendError(
                "native orphan sweep cannot replace an accepted write view".into(),
            ));
        }
        view.current = writer.current().clone();
        let receipt = writer.reclaim_orphans(&policy).map_err(map)?;
        view.current = writer.current().clone();
        Ok((
            receipt.files_removed,
            receipt.bytes_removed,
            receipt.reclaim_deferred,
        ))
    }
}
