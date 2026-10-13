use super::*;

impl SyncCoordinator {
    /// Spawn the background sync timer task. No-op if no engine is configured.
    ///
    /// ### Offline backoff
    ///
    /// While the engine state is [`SyncState::Offline`] (i.e. the last sync
    /// failed with a network-class error), the inter-cycle delay doubles on
    /// each consecutive failure — `interval_ms`, 2×, 4×, … — capped at
    /// [`MAX_OFFLINE_BACKOFF`] (10 minutes). On the next successful cycle the
    /// delay resets to `interval_ms`. This matters for laptops resuming from
    /// sleep and phones with flaky connectivity: without the backoff, the
    /// coordinator would hammer the presign endpoint every `interval_ms` for
    /// the entire offline window (CPU and battery cost, plus noisy retries on
    /// cold Lambda).
    ///
    /// Auth failures also back off. The engine still does an immediate
    /// refresh-and-retry inside a single cycle, but once that fails, repeating
    /// the same rejected credential or locked credential-store read every base
    /// interval can exhaust local resources without making progress.
    ///
    /// Idempotent: a second call while a loop is already running is a no-op.
    /// The `task` slot is checked and reserved under one lock acquisition so a
    /// racing caller (e.g. two near-simultaneous `start_sync_engine_runtime`
    /// calls) cannot both observe an empty slot and both spawn a loop — the
    /// older loop would then be unabortable, since `abort_task`/`stop` can
    /// only abort whatever handle is currently stored.
    pub fn start_background_sync(&self, interval_ms: u64) {
        let mut slot = self.task.lock().unwrap();
        if slot.is_some() {
            return;
        }

        let engine = match &*self.engine.read().unwrap() {
            Some(e) => Arc::clone(e),
            None => return,
        };

        let wake = engine.wake_handle();

        // lint:spawn-bare-ok boot-time sync poll loop — perpetual worker, no per-request parent span.
        let handle = tokio::spawn(async move {
            let base_interval = tokio::time::Duration::from_millis(interval_ms);
            let max_delay = MAX_OFFLINE_BACKOFF;
            let mut current_delay = base_interval;
            let mut suppress_wake_once = false;
            loop {
                // Sleep up to `current_delay`, or wake early if a local write
                // arrived. A write fires `engine.wake.notify_one()`, which
                // resolves the `notified()` future and aborts the timeout so
                // the flush happens near-immediately instead of waiting the
                // full polling interval. `timeout` returns `Err` on the timer
                // path and `Ok(())` on the wake path — either way, the same
                // check-and-sync logic below fires.
                if suppress_wake_once {
                    tokio::time::sleep(current_delay).await;
                } else {
                    let _ = tokio::time::timeout(current_delay, wake.notified()).await;
                }
                // Always call sync() when a sync engine is configured. A
                // passive reader on a personal prefix (another device
                // restored from the same mnemonic) needs the poll to see
                // peer writes, even when locally clean and without org
                // memberships. Previous "skip if nothing to upload and no
                // orgs" check broke multi-device convergence — it matched
                // an equivalent bailout inside `sync()` that #607 removed,
                // but that was only half the fix. `sync()` is cheap on a
                // no-op cycle (one list request per target with an
                // already-advanced cursor → typically 0 matches).
                //
                // Automatic gc-atoms does **not** run here. A stalled or
                // failed sync cycle must not starve the local cadence; that
                // timer starts from [`Self::start_automatic_gc_atoms`].
                match engine.sync().await {
                    Ok(_) => {
                        // A coalesce hold asks for a short retry so the group
                        // flushes when the quiet window or the max hold ends.
                        // A write wake still aborts this wait. `0` keeps the
                        // normal interval.
                        let retry_ms = engine
                            .mutation_log_coalesce_retry_ms
                            .load(std::sync::atomic::Ordering::Acquire);
                        current_delay = if retry_ms > 0 {
                            tokio::time::Duration::from_millis(retry_ms)
                        } else {
                            base_interval
                        };
                        suppress_wake_once = false;
                    }
                    Err(e) => {
                        warn!("sync cycle failed: {e}");
                        let suppress_wake = failure_should_suppress_wakeups(&e);
                        current_delay = next_backoff_on_failure(
                            current_delay,
                            base_interval,
                            max_delay,
                            MAX_AUTH_BACKOFF,
                            &e,
                        );
                        suppress_wake_once = suppress_wake;
                    }
                }
            }
        });

        *slot = Some(handle);
        drop(slot);
        self.start_capture_reexport_drain();
    }

    fn start_capture_reexport_drain(&self) {
        let mut slot = self.capture_reexport_task.lock().unwrap();
        if slot.is_some() {
            return;
        }
        let Some(engine) = self.engine() else {
            return;
        };
        if !matches!(
            engine.config.capture_mode,
            crate::sync::engine::CaptureMode::MutationLog
        ) {
            return;
        }
        // lint:spawn-bare-ok process-lifetime bounded capture drain — not request-scoped.
        let handle = tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            let mut lap_progress = false;
            loop {
                if !engine.should_stage_cloud_mutations().await {
                    lap_progress = false;
                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                    continue;
                }
                let delay = match engine.run_capture_marker_tick().await {
                    Ok(stats) => capture_reexport_drain_delay(&stats, &mut lap_progress),
                    Err(error) => {
                        lap_progress = false;
                        warn!("capture re-export drain failed: {error}");
                        tokio::time::Duration::from_secs(30)
                    }
                };
                tokio::time::sleep(delay).await;
            }
        });
        *slot = Some(handle);
    }
}
