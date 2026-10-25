use super::*;

impl SyncCoordinator {
    /// Start the local automatic plane compaction cadence once.
    /// Zero disables the cadence. Local policy, trigger order and packing locks stay unchanged.
    pub fn start_automatic_plane_compaction(&self, db_ops: &Arc<DbOperations>) {
        let interval_secs = env_flag::var_or("LASTDB_PLANE_COMPACTION_INTERVAL_SECS", 60u64);
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
                local.run_all_sweeps().await;
            }
        });
        info!(
            target: "fold_db::plane_compaction",
            interval_secs,
            "automatic plane self-compaction local cadence started"
        );
        *slot = Some(handle);
    }

    /// Return local automatic-compaction reports, or None before cadence setup.
    pub async fn automatic_compaction_status(
        &self,
    ) -> Option<std::collections::BTreeMap<String, crate::sync::engine::AutomaticCompactionStatus>>
    {
        let local = self.local_plane_compactor.read().unwrap().clone()?;
        Some(local.status_snapshot().await)
    }
}
