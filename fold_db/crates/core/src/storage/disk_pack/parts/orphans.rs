//! The sweep for part files that no receipt names (see `laststore` reclaim).

use super::*;
use crate::storage::laststore::LastStoreKvStore;
use laststore::disk_pack::maintenance::SpacePolicy;
use tracing::Instrument;

impl PackRuntime {
    /// Remove part files that no receipt names and that are older than `min_age_ms`.
    /// Returns the files and bytes removed, and whether active reads deferred the sweep.
    /// It releases the write gate and view lock before any inventory or deletion.
    pub(super) async fn sweep_orphans_native(
        self: &Arc<Self>,
        min_age_ms: u64,
    ) -> StorageResult<(u64, u64, bool)> {
        let runtime = Arc::clone(self);
        tokio::spawn(
            async move {
                let maintenance = Arc::clone(&runtime.maintenance_gate).lock_owned().await;
                let gate = runtime.enter_write().await;
                LastStoreKvStore::run_blocking(move || {
                    let _maintenance = maintenance;
                    runtime.sweep_orphans_blocking(min_age_ms, gate)
                })
                .await
            }
            .in_current_span(),
        )
        .await
        .map_err(|error| StorageError::BackendError(format!("native orphan owner: {error}")))?
    }

    fn sweep_orphans_blocking(
        &self,
        min_age_ms: u64,
        gate: tokio::sync::OwnedMutexGuard<()>,
    ) -> StorageResult<(u64, u64, bool)> {
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
        let writer = self.journal.lock().expect("native journal owner");
        // Install the exact deletion floor before new reads capture their root.
        let mut view = self.view.write().expect("native complete read view");
        if !view.accepted.is_empty() {
            return Err(StorageError::BackendError(
                "native orphan sweep cannot replace an accepted write view".into(),
            ));
        }
        view.current = writer.current().clone();
        let plan = writer.capture_reclaim().map_err(map)?;
        drop(view);
        drop(writer);
        drop(gate);
        let receipt = plan.execute(&policy, true).map_err(map)?;
        Ok((
            receipt.files_removed,
            receipt.bytes_removed,
            receipt.reclaim_deferred,
        ))
    }
}
