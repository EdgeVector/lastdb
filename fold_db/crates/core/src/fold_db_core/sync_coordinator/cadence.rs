use super::*;

impl SyncCoordinator {
    /// Start the local automatic plane self-compaction cadence.
    ///
    /// Every sweep in [`crate::sync::capture::PlaneCompactor`] used to run only
    /// inside `SyncEngine::do_sync`. A node booted without `cloud_sync.json`
    /// has no engine and therefore compacted nothing: `tips` and `atoms` grew
    /// with no automatic byte-return path at all, while `gc-atoms` — which does
    /// have a local cadence — kept stepping and made the growth look tended.
    ///
    /// This timer does not wait for a sync cycle, exactly like
    /// the cloud sync loop. Idempotent: a second call is a
    /// no-op. A zero probe interval does not spawn.
    ///
    /// Each tick prefers the *engine's* compactor when an engine exists, so a
    /// cloud-synced node keeps one set of trigger state and one packing lock
    /// rather than a second sweeper racing `do_sync`. The engine can be set
    /// after this timer starts (`cloud on`), so the choice is made per tick,
    /// not once at spawn.
    pub fn start_automatic_plane_compaction(&self, db_ops: &Arc<DbOperations>) {
        let interval_secs = plane_compaction_interval_secs();
        if interval_secs == 0 {
            info!(
                target: "fold_db::plane_compaction",
                "automatic plane self-compaction cadence disabled (probe interval is 0)"
            );
            return;
        }
        let mut slot = self.plane_compaction_task.lock().unwrap();
        if slot.is_some() {
            return;
        }
        let local = Arc::new(crate::sync::capture::PlaneCompactor::new(
            db_ops.namespaced_store(),
            Arc::clone(&self.backup_publish_target),
            Arc::clone(&self.cloud_sync_disabled_at),
        ));
        *self.local_plane_compactor.write().unwrap() = Some(Arc::clone(&local));
        let engine_slot = Arc::clone(&self.engine);
        let interval = tokio::time::Duration::from_secs(interval_secs);
        // lint:spawn-bare-ok process-lifetime local plane-compaction cadence — not request-scoped.
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Skip the immediate first tick: a probe before the caller has
            // written anything measures an empty store and claims the
            // rate-limit slot for nothing.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let engine = engine_slot.read().unwrap().clone();
                if let Some(engine) = engine {
                    Arc::clone(&engine.compaction).run_all_sweeps().await;
                } else if engine_slot.read().unwrap().is_some() {
                    // `set_engine` won between the clone and this arm.
                    // Refuse the local sweep so a dummy-lock rewrite cannot
                    // start under a live cut. The next tick uses the engine.
                    continue;
                } else {
                    local.run_all_sweeps().await;
                }
            }
        });
        info!(
            target: "fold_db::plane_compaction",
            interval_secs,
            "automatic plane self-compaction local cadence started"
        );
        *slot = Some(handle);
    }

    /// Last-fire automatic compaction reports for `lastdb status`.
    ///
    /// Reads the engine's compactor when cloud sync is on and the local one
    /// otherwise, so the status line has one shape on both node shapes.
    /// `None` means no cadence has started in this process.
    pub async fn automatic_compaction_status(
        &self,
    ) -> Option<std::collections::BTreeMap<String, crate::sync::engine::AutomaticCompactionStatus>>
    {
        if let Some(engine) = self.engine() {
            return Some(engine.compaction.status_snapshot().await);
        }
        let local = self.local_plane_compactor.read().unwrap().clone()?;
        Some(local.status_snapshot().await)
    }
}
