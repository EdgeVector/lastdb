use super::*;

impl SyncCoordinator {
    /// Start the local automatic `gc-atoms` cadence.
    ///
    /// This timer does not wait for [`SyncEngine::sync`] to succeed. A
    /// sync-disabled node and a node whose peer-apply is stalled still step.
    /// Idempotent: a second call is a no-op. A zero byte cap does not spawn.
    pub fn start_automatic_gc_atoms(&self, db_ops: Arc<DbOperations>) {
        let settings = AutomaticGcAtomsSettings::from_env();
        self.gc_atoms_status.mark_started(&settings, unix_secs());
        if !settings.enabled() {
            info!(
                target: "fold_db::gc_atoms",
                "automatic gc-atoms local cadence disabled (byte cap is 0)"
            );
            return;
        }
        let mut slot = self.gc_atoms_task.lock().unwrap();
        if slot.is_some() {
            return;
        }
        let engine_slot = Arc::clone(&self.engine);
        let status = self.gc_atoms_status.clone();
        let interval = tokio::time::Duration::from_secs(settings.step_interval_secs.max(1));
        // lint:spawn-bare-ok process-lifetime local gc-atoms cadence — not request-scoped.
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Skip the immediate first tick so FoldDB construction in tests
            // does not start a probe before the caller stores a first atom.
            ticker.tick().await;
            let last_step = AtomicU64::new(0);
            let restart_lap_at = AtomicU64::new(0);
            loop {
                ticker.tick().await;
                let engine = engine_slot.read().unwrap().clone();
                let deadline = tokio::time::Instant::now()
                    + tokio::time::Duration::from_secs(settings.step_budget_secs.max(1));
                let now = unix_secs();
                let lock_wait = tokio::time::Duration::from_secs(GC_ATOMS_PACKING_LOCK_WAIT_SECS);
                if let Some(engine) = engine {
                    advance_automatic_gc_atoms(
                        AutomaticGcAtomsRuntime {
                            atoms: db_ops.atoms(),
                            engine: Some(engine.as_ref()),
                            status: &status,
                        },
                        &last_step,
                        &restart_lap_at,
                        settings,
                        now,
                        deadline,
                        || lock_engine_backup_cut(&engine, lock_wait),
                    )
                    .await;
                } else {
                    advance_automatic_gc_atoms(
                        AutomaticGcAtomsRuntime {
                            atoms: db_ops.atoms(),
                            engine: None,
                            status: &status,
                        },
                        &last_step,
                        &restart_lap_at,
                        settings,
                        now,
                        deadline,
                        || async { CutLock::Acquired(()) },
                    )
                    .await;
                }
            }
        });
        info!(
            target: "fold_db::gc_atoms",
            interval_secs = settings.step_interval_secs,
            step_budget_secs = settings.step_budget_secs,
            max_unreferenced_bytes = settings.max_unreferenced_bytes,
            "automatic gc-atoms local cadence started"
        );
        *slot = Some(handle);
    }

    /// Start the local automatic plane self-compaction cadence.
    ///
    /// Every sweep in [`crate::sync::capture::PlaneCompactor`] used to run only
    /// inside `SyncEngine::do_sync`. A node booted without `cloud_sync.json`
    /// has no engine and therefore compacted nothing: `tips` and `atoms` grew
    /// with no automatic byte-return path at all, while `gc-atoms` — which does
    /// have a local cadence — kept stepping and made the growth look tended.
    ///
    /// This timer does not wait for a sync cycle, exactly like
    /// [`Self::start_automatic_gc_atoms`]. Idempotent: a second call is a
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
