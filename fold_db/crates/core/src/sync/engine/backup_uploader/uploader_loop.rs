use super::*;

impl SyncEngine {
    /// Whether this LastStore home has durable evidence of at least one
    /// committed snapshot manifest (the mutation-log plane's required S0).
    ///
    /// Engines without a LastStore backup source are test/legacy shapes and do
    /// not participate in the sealed-home S0 contract.
    pub(crate) fn mutation_log_snapshot_base_committed(&self) -> SyncResult<bool> {
        let Some(source) = self.laststore_backup_source.as_ref() else {
            return Ok(true);
        };
        source
            .backup_durability()
            .map(|durability| durability.is_some_and(|state| state.backup_manifest_counter > 0))
            .map_err(|err| {
                SyncError::Storage(format!("read mutation-log snapshot-base durability: {err}"))
            })
    }

    /// Ask the backup-uploader std thread to exit. Idempotent if never started.
    pub(crate) fn stop_laststore_backup_uploader(&self) {
        self.laststore_backup_uploader_stop
            .store(true, Ordering::SeqCst);
    }

    pub(crate) fn start_laststore_backup_uploader(self: &Arc<Self>) {
        if self.laststore_backup_source.is_none() {
            return;
        }
        if self
            .laststore_backup_uploader_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        self.laststore_backup_uploader_stop
            .store(false, Ordering::SeqCst);

        let engine = Arc::clone(self);
        let spawn = std::thread::Builder::new()
            .name("laststore-backup-uploader".to_string())
            .spawn(move || {
                LIVE_BACKUP_UPLOADER_THREADS.fetch_add(1, Ordering::SeqCst);
                throttle_this_thread_io();
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(err) => {
                        tracing::error!(
                            target: "fold_db::sync::backup",
                            error = %err,
                            "laststore backup uploader runtime construction failed"
                        );
                        LIVE_BACKUP_UPLOADER_THREADS.fetch_sub(1, Ordering::SeqCst);
                        engine
                            .laststore_backup_uploader_started
                            .store(false, Ordering::SeqCst);
                        return;
                    }
                };
                let started_flag = Arc::clone(&engine);
                runtime.block_on(engine.run_laststore_backup_uploader_forever());
                LIVE_BACKUP_UPLOADER_THREADS.fetch_sub(1, Ordering::SeqCst);
                started_flag
                    .laststore_backup_uploader_started
                    .store(false, Ordering::SeqCst);
            });
        if let Err(err) = spawn {
            self.laststore_backup_uploader_started
                .store(false, Ordering::SeqCst);
            tracing::error!(
                target: "fold_db::sync::backup",
                error = %err,
                "failed to spawn laststore backup uploader"
            );
        }
    }

    pub(super) async fn sleep_or_stop(&self, duration: Duration) {
        let slice = Duration::from_millis(100);
        let deadline = Instant::now() + duration;
        loop {
            if self.laststore_backup_uploader_stop.load(Ordering::SeqCst) {
                return;
            }
            let now = Instant::now();
            if now >= deadline {
                return;
            }
            tokio::time::sleep((deadline - now).min(slice)).await;
        }
    }

    // lint:fn-size-ok moved verbatim from backup_uploader.rs; splitting this function is separate work.
    pub(super) async fn run_laststore_backup_uploader_forever(self: Arc<Self>) {
        let interval = backup_uploader_interval();
        let publish_every = snapshot_log_publish_interval();
        let mut last_publish = Instant::now()
            .checked_sub(publish_every)
            .unwrap_or_else(Instant::now);
        // Last successfully CAS-committed manifest. Threaded into every later
        // cut so `cut_backup_manifest` carries atom chunks as a monotonic
        // superset and `validate_manifest_chain` can enforce hash-chain /
        // counter / cut_csn / atom-rollback checks. Always passing `None` made
        // those checks dead on the continuous production path.
        let mut last_committed_manifest: Option<BackupManifest> = None;
        // Back off CAS/snapshot attempts after failures so a stuck cloud pointer
        // (e.g. store_uuid_mismatch) cannot hammer the API or inflate RAM.
        // Chunk-only drain still runs every `interval` (never blocks local R/W)
        // unless drain-PUT backoff is also armed (see below).
        let mut cas_backoff = Duration::from_secs(0);
        let mut next_cas_attempt = Instant::now();
        // Drain-PUT backoff: a cycle that attempted PUTs and completed none
        // (quota hard-fail, auth poison, region outage) used to retry at the
        // full interval forever — measured ~8k rejections / 25 min on a home
        // over quota. Enumerate still runs every `interval` so remaining counts
        // stay honest; only the PUT fan-out waits on `next_drain_put_attempt`.
        let mut drain_put_backoff = Duration::from_secs(0);
        let mut next_drain_put_attempt = Instant::now();
        loop {
            if self.laststore_backup_uploader_stop.load(Ordering::SeqCst) {
                break;
            }
            // Hard interlock: intentional Cloud Sync Off must not presign/PUT/CAS.
            // Sleep the normal interval so re-enable is picked up without a restart.
            if !self.cloud_plane_allows_upload().await {
                self.sleep_or_stop(interval).await;
                continue;
            }
            // Mutation-log-first Phase A: continuous full-home sealed re-upload is
            // demoted after S0 — log segments under log/{writer_id}/ are the
            // steady-state durability engine. Before S0 exists, however, the
            // uploader must cut and durably CAS one base; the log publisher is
            // fail-closed on the same marker.
            //
            // Exception (won't-undo): a *held incomplete cut* must still drain
            // + CAS to completion. Demotion must not strand mid-drain targets
            // that only finish via operator laststore-snapshot kicks.
            let demoted = self.pin_log.continuous_sealed_home_backup_demoted();
            let snapshot_base_committed = match self.mutation_log_snapshot_base_committed() {
                Ok(committed) => committed,
                Err(err) => {
                    tracing::warn!(
                        target: "fold_db::sync::snapshot_log",
                        error = %err,
                        "cannot prove mutation-log S0; keeping log publish gated and retrying bootstrap"
                    );
                    false
                }
            };
            if demoted && snapshot_base_committed && !self.has_backup_publish_target().await {
                tracing::debug!(
                    target: "fold_db::sync::snapshot_log",
                    "continuous sealed-home backup demoted after durable S0; no held cut — mutation-log plane is active durability engine"
                );
                self.sleep_or_stop(interval).await;
                continue;
            }
            if demoted && snapshot_base_committed {
                tracing::info!(
                    target: "fold_db::sync::snapshot_log",
                    "continuous sealed-home demoted but held cut present; drain-only cycle (no new full-home seal)"
                );
            } else if demoted {
                tracing::info!(
                    target: "fold_db::sync::snapshot_log",
                    "mutation-log S0 is not committed; running mandatory snapshot-base bootstrap"
                );
            }
            let allow_drain_puts = Instant::now() >= next_drain_put_attempt;
            let allow_cas = Instant::now() >= next_cas_attempt && allow_drain_puts;
            // A demoted home may cut exactly the missing mandatory S0. Once its
            // manifest commit is durable, later loops return to drain-only
            // behavior until the ratio-compaction policy requests a new base.
            let allow_new_cut = !demoted || !snapshot_base_committed;
            match self
                .run_snapshot_log_publish_cycle(
                    last_committed_manifest.as_ref(),
                    &mut last_publish,
                    allow_cas,
                    allow_drain_puts,
                    allow_new_cut,
                )
                .await
            {
                Ok(report) => {
                    // `run_snapshot_log_publish_cycle` already recorded this
                    // cycle's sample. Recording it again here fed the same
                    // `bytes_uploaded` into the EWMA twice, inflating the very
                    // throughput number the ETA is derived from.
                    if report.snapshot_published {
                        if let Some(manifest) = report.published_manifest {
                            last_committed_manifest = Some(manifest);
                        }
                        cas_backoff = Duration::from_secs(0);
                        next_cas_attempt = Instant::now();
                        // Detached post-CAS orphan GC (latest tip keep-set).
                        // Never await here: local R/W and the next drain cycle
                        // must not wait on cloud list/DELETE.
                        self.schedule_post_cas_backup_orphan_gc();
                    }
                    // The escape hatch must be reachable before a successful
                    // tip: quota rejection is exactly what can prevent that
                    // tip forever. Keep-set MUST include the last published tip
                    // (local last_committed and/or cloud backup/latest) plus
                    // any held cut — never empty-published while tip N is live.
                    self.maybe_run_quota_recovery_orphan_gc(
                        &report.chunks,
                        last_committed_manifest.as_ref(),
                    )
                    .await;
                    // Drain-PUT backoff: success resets; fully-failed cycle arms.
                    if report.chunks.uploaded > 0 {
                        drain_put_backoff = Duration::from_secs(0);
                        next_drain_put_attempt = Instant::now();
                    } else if crate::backup_drain_plan::drain_attempted_and_completed_none(
                        report.chunks.selected,
                        report.chunks.uploaded,
                        report.chunks.failed,
                    ) {
                        drain_put_backoff =
                            crate::backup_drain_plan::next_drain_put_backoff(drain_put_backoff);
                        next_drain_put_attempt = Instant::now() + drain_put_backoff;
                        tracing::warn!(
                            target: "fold_db::sync::snapshot_log",
                            selected = report.chunks.selected,
                            failed = report.chunks.failed,
                            drain_put_backoff_secs = drain_put_backoff.as_secs(),
                            "backup drain completed no PUTs; backing off put fan-out (enumerate continues)"
                        );
                    }
                    if report.chunks.selected > 0 || report.snapshot_published {
                        tracing::info!(
                            target: "fold_db::sync::snapshot_log",
                            uploaded = report.chunks.uploaded,
                            already_present = report.chunks.already_present,
                            failed = report.chunks.failed,
                            bytes_uploaded = report.chunks.bytes_uploaded,
                            chunks_present = report.chunks.chunks_present,
                            chunks_total = report.chunks.candidates,
                            selected = report.chunks.selected,
                            snapshot_published = report.snapshot_published,
                            frontier_through = ?report.frontier_through,
                            cas_counter = ?report.cas_counter,
                            gc_eligible = report.gc_eligible_log_segments,
                            phase = %report.publish_phase,
                            "snapshot+log continuous publish cycle"
                        );
                    }
                }
                Err(err) => {
                    let err = redact_sync_error_text(&err.to_string());
                    // Record the failure so status reflects it. Without this a
                    // uploader whose every cycle errors never produces a
                    // sample, and `/api/status` reported backup.complete=true.
                    self.record_backup_progress_failure(&err);
                    // Exponential backoff for CAS/publish failures (cap 15 min).
                    cas_backoff = if cas_backoff.is_zero() {
                        Duration::from_secs(30)
                    } else {
                        (cas_backoff * 2).min(Duration::from_secs(900))
                    };
                    next_cas_attempt = Instant::now() + cas_backoff;
                    // A cycle that returned Err after being allowed to PUT also
                    // counts as drain failure (e.g. all PUTs failed and the
                    // drain escalated to Err). Enumerate-only cycles never arm
                    // this path because they do not take the put fan-out.
                    if allow_drain_puts {
                        drain_put_backoff =
                            crate::backup_drain_plan::next_drain_put_backoff(drain_put_backoff);
                        next_drain_put_attempt = Instant::now() + drain_put_backoff;
                    }
                    tracing::warn!(
                        target: "fold_db::sync::snapshot_log",
                        error = %err,
                        cas_backoff_secs = cas_backoff.as_secs(),
                        drain_put_backoff_secs = drain_put_backoff.as_secs(),
                        "snapshot+log publish cycle failed; will retry later (local R/W unaffected)"
                    );
                }
            }
            self.sleep_or_stop(interval).await;
        }
    }
}
