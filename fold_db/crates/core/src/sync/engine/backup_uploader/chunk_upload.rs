use super::*;

impl SyncEngine {
    pub async fn upload_laststore_backup_chunks_once(
        &self,
        previous_manifest: Option<&BackupManifest>,
    ) -> SyncResult<LastStoreBackupUploadStats> {
        if !self.cloud_plane_allows_upload().await {
            return Err(SyncError::Storage(
                "cloud plane Off: backup chunk upload refused (lastdb cloud off)".into(),
            ));
        }
        self.ensure_backup_presence_cache_loaded().await;
        let store = self.laststore_backup_source.as_ref().ok_or_else(|| {
            SyncError::Storage("laststore backup uploader has no LastStore source".to_string())
        })?;
        let scan = store
            .scan_backup_chunks(previous_manifest)
            .map_err(|e| SyncError::Storage(format!("enumerate backup chunks failed: {e}")))?;
        if !scan.unresolvable.is_empty() {
            // Drain the healthy chunks anyway. A chunk the store lists but
            // cannot verify used to abort this walk outright, which stalled
            // every other chunk's upload behind it; the manifest cut is the
            // gate that stays closed until the store is repaired.
            tracing::warn!(
                unresolvable = scan.unresolvable.len(),
                drainable = scan.candidates.len(),
                first_collection = %scan.unresolvable[0].collection,
                first_chunk_uuid = %scan.unresolvable[0].chunk_uuid,
                "backup chunks could not be verified; uploading the rest and \
                 holding the manifest cut until they are repaired"
            );
        }
        // Refresh policy for side effects / logging; drain bounds are put-count
        // (upload_target), not a fixed head take that can permanently stall.
        let _caps = self.refresh_upload_policy().await;
        self.seed_backup_presence_if_cold(&scan.candidates).await;
        // No held cut here: this path drains a freshly scanned set, so there is
        // no generation to scope a source-missing verdict to.
        self.drain_backup_candidates(scan.candidates, true, None)
            .await
    }

    /// How many locally-unknown candidates justify a fresh presence listing
    /// on a publish attempt. Below this, per-chunk probes are cheaper than a
    /// full paginated listing; above it, one listing replaces hundreds of
    /// serial round trips — and, critically, it SEES uploads that arrived
    /// out-of-band (another process, an operator bulk-push) which the
    /// process-local cache can never learn about otherwise.
    pub(super) const PRESENCE_RESEED_SHORTFALL: usize = 64;

    /// Seed `backup_known_present` from ONE paginated cloud listing when the
    /// cache is cold (empty) and there is real work to skip.
    ///
    /// Presence truth is the object store either way — this is the same
    /// bucket-HEAD truth `presign_backup_chunk_upload` answers with, fetched
    /// at ~1000 keys per round trip instead of one. Without it, a restarted
    /// node re-proves presence one serial round trip per chunk (~16k probes
    /// on the 2026-07-30 primary — hours), because the cache is process-local.
    /// Best-effort: a failed listing just leaves the per-chunk path in place.
    pub(super) async fn seed_backup_presence_if_cold(
        &self,
        candidates: &[BackupChunkUploadCandidate],
    ) {
        if candidates.is_empty() {
            return;
        }
        {
            let known = self.backup_known_present.lock().await;
            if !known.is_empty() {
                return;
            }
        }
        self.seed_backup_presence_from_listing().await;
    }

    /// Re-seed on a publish attempt whose LOCAL view says many chunks are
    /// missing. On a busy store the cut rotates while capped uploads chase it;
    /// if the shortfall is mostly stale-negative cache (bytes that already
    /// reached the bucket out-of-band), one listing collapses it to the true
    /// churn-since-cut and the attempt can actually reach CAS (2026-07-30:
    /// local view said 4,936 missing while the bucket was ~fully populated).
    pub(super) async fn reseed_backup_presence_on_shortfall(
        &self,
        candidates: &[BackupChunkUploadCandidate],
    ) {
        let missing = {
            let known = self.backup_known_present.lock().await;
            candidates
                .iter()
                .filter(|c| !known.contains(&c.chunk.sha256))
                .count()
        };
        if missing < Self::PRESENCE_RESEED_SHORTFALL {
            return;
        }
        let min_interval = backup_presence_reseed_min_interval();
        let last = *self.backup_presence_listed_at.lock().await;
        if !presence_reseed_is_due(last, Instant::now(), min_interval) {
            return;
        }
        tracing::info!(
            target: "fold_db::sync::backup",
            locally_missing = missing,
            "publish attempt sees a large local shortfall; refreshing presence from cloud listing"
        );
        self.seed_backup_presence_from_listing().await;
    }

    pub(super) async fn seed_backup_presence_from_listing(&self) {
        let min_interval = backup_presence_reseed_min_interval();
        {
            let last = *self.backup_presence_listed_at.lock().await;
            if !presence_reseed_is_due(last, Instant::now(), min_interval) {
                tracing::debug!(
                    target: "fold_db::sync::backup",
                    min_interval_secs = min_interval.as_secs(),
                    "skipping backup presence re-list; last listing was recent"
                );
                return;
            }
        }
        let listed = match self.auth.list_objects("backup/chunks/").await {
            Ok(objects) => objects,
            Err(e) => {
                tracing::warn!(
                    target: "fold_db::sync::backup",
                    error = %redact_sync_error_text(&e.to_string()),
                    "backup presence seed listing failed; falling back to per-chunk probes"
                );
                return;
            }
        };
        let shas = shas_from_backup_chunk_listing(&listed);
        if shas.is_empty() {
            return;
        }
        let seeded = shas.len();
        self.backup_known_present.lock().await.extend(shas);
        self.persist_backup_presence_cache().await;
        *self.backup_presence_listed_at.lock().await = Some(Instant::now());
        tracing::info!(
            target: "fold_db::sync::backup",
            seeded,
            listed = listed.len(),
            "seeded backup presence cache from cloud listing"
        );
    }

    /// Complete object-store presence for carried-forward atom reconciliation.
    ///
    /// Returns `Some(CloudChunkPresence { listing_complete: true, … })` only
    /// when a full `backup/chunks/` listing succeeds. Empty listings are still
    /// complete (every digest is proven-absent). Listing errors return `None`
    /// so the cut keeps unconditional carry-forward rather than guessing.
    pub(super) async fn list_backup_chunk_presence_for_reconcile(
        &self,
    ) -> Option<CloudChunkPresence> {
        let listed = match self.auth.list_objects("backup/chunks/").await {
            Ok(objects) => objects,
            Err(e) => {
                tracing::warn!(
                    target: "fold_db::sync::backup",
                    error = %redact_sync_error_text(&e.to_string()),
                    "backup presence listing for atom reconcile failed; cut will not retire carried-forward atoms"
                );
                return None;
            }
        };
        let shas = shas_from_backup_chunk_listing(&listed);
        let seeded = shas.len();
        if !shas.is_empty() {
            self.backup_known_present
                .lock()
                .await
                .extend(shas.iter().cloned());
            self.persist_backup_presence_cache().await;
        }
        tracing::info!(
            target: "fold_db::sync::backup",
            seeded,
            listed = listed.len(),
            "complete backup chunk listing for carried-forward atom reconcile"
        );
        Some(CloudChunkPresence::from_complete_listing(shas))
    }

    /// Walk the full sorted candidate list until `upload_target` successful puts
    /// (or end-of-list). Already-present head units are skipped so later missing
    /// segs still drain — never pre-truncate to a fixed probe window.
    ///
    /// Contract (see `backup_drain_plan::plan_backup_uploads`): full walk, put
    /// budget only; skip digests already in `backup_known_present` without
    /// re-presign; network I/O does not hold the known-present lock.
    ///
    /// PUTs fan out with bounded concurrency (upload-policy concurrency, or
    /// `LASTDB_BACKUP_UPLOAD_CONCURRENCY`) so a single serial stream is no
    /// longer the hard product ceiling (~137 KB/s on a multi-Mbps link).
    // lint:fn-size-ok moved verbatim from backup_uploader.rs; splitting this function is separate work.
    pub(super) async fn drain_backup_candidates(
        &self,
        candidates: Vec<BackupChunkUploadCandidate>,
        allow_puts: bool,
        target_generation: Option<u64>,
    ) -> SyncResult<LastStoreBackupUploadStats> {
        let total_candidates = candidates.len();
        let mut stats = LastStoreBackupUploadStats {
            candidates: total_candidates,
            // Placeholder; overwritten with the real per-cycle work count after
            // the known-present walk. Using `total_candidates` here defeated the
            // no-op-cycle log gate (selected was almost always > 0 on any node
            // with prior data).
            selected: 0,
            ..LastStoreBackupUploadStats::default()
        };

        // Always refresh full-list present/remaining so a backed-off drain
        // still keeps `chunks_remaining` / `bytes_remaining` honest. Remaining
        // is also the catch-up input: a fresh local last-publish (or a CoW of
        // one) must not keep a multi-thousand-chunk first fill on the 4-PUT
        // steady-state budget.
        let remaining_chunks = {
            let known = self.backup_known_present.lock().await;
            let mut present = 0usize;
            let mut bytes_remaining = 0u64;
            for candidate in &candidates {
                if known.contains(&candidate.chunk.sha256) {
                    present += 1;
                    stats.already_present += 1;
                } else {
                    bytes_remaining = bytes_remaining.saturating_add(candidate.chunk.bytes);
                }
            }
            stats.chunks_present = present;
            stats.bytes_remaining = bytes_remaining;
            total_candidates.saturating_sub(present)
        };
        // A home with no known restore base, a stale committed cut, *or* a
        // remaining shortfall large enough that we re-list the bucket every
        // cycle, gets the catch-up budget. Staleness alone is not enough: a
        // CoW of a just-published primary pointed at empty DEV still has a
        // fresh local last-publish and would otherwise PUT 4/cycle (measured
        // 2026-08-20: 7.3 GiB in 2h21m at ~4.6 Mbps; the link would have done
        // it in ~10 min).
        let last_publish_age_secs = self
            .backup_progress
            .lock()
            .ok()
            .and_then(|tracker| tracker.snapshot().last_publish_age_secs);
        let catching_up = backup_is_catching_up_for_drain(
            last_publish_age_secs,
            backup_catchup_staleness_secs_from_env(),
            remaining_chunks,
            Self::PRESENCE_RESEED_SHORTFALL,
        );
        let upload_target = backup_upload_target_per_cycle(catching_up);

        if !allow_puts {
            // Enumerate-only cycle under drain-PUT backoff: no selected work,
            // so the forever-loop does not re-arm backoff from this sample.
            tracing::debug!(
                target: "fold_db::sync::backup",
                candidates = total_candidates,
                chunks_present = stats.chunks_present,
                bytes_remaining = stats.bytes_remaining,
                "drain-PUT backoff active; enumerate-only cycle (no puts)"
            );
            return Ok(stats);
        }

        // Collect work items first (skip known-present without re-presign).
        let mut work: Vec<BackupChunkUploadCandidate> = Vec::new();
        {
            let known = self.backup_known_present.lock().await;
            let mut missing = self.backup_unresolvable.lock().await;
            let missing = missing.for_generation(target_generation);
            // already_present was counted above for remaining honesty; rebuild
            // the work list from the same known set without double-counting.
            stats.already_present = 0;
            stats.source_missing = 0;
            for candidate in &candidates {
                if known.contains(&candidate.chunk.sha256) {
                    stats.already_present += 1;
                } else if missing.contains(&candidate.chunk.sha256) {
                    // Head-of-line: candidates are walked in a stable order and
                    // a chunk with no source never enters `known`, so without
                    // this skip the same dead prefix is re-selected every cycle
                    // and the drain never reaches the chunks behind it.
                    // Measured on the 2026-08-06 primary: the same 256 shas for
                    // hours while 4,694 remaining chunks went untried.
                    stats.source_missing += 1;
                } else if work.len() < upload_target {
                    work.push(candidate.clone());
                }
            }
        }
        // Selected = work attempted this cycle (bounded by upload_target), not
        // the full candidate census. This is what the log-spam gate inspects.
        stats.selected = work.len();

        let concurrency = self
            .effective_backup_upload_concurrency(
                self.active_upload_caps().await.concurrency,
                catching_up,
            )
            .await;
        // Bound how long ONE catch-up cycle may run. Without this the larger put
        // budget turns into an unbounded cycle on a slow link, starving the
        // publish attempt and freezing progress samples.
        let deadline = catching_up.then(|| Instant::now() + backup_catchup_cycle_budget());
        // One candidate's failure must not abort the walk: 2026-07-30 a 10%
        // server-side presign poison turned into an ~80% cycle-abort rate
        // because every error here was `?`-propagated. Isolate per candidate;
        // surface the first error only when the cycle achieved nothing at all.
        let engine = self;
        // Wall time of the PUT fan-out alone. Measured around the whole parallel
        // cycle rather than summed per candidate, so bounded concurrency is
        // accounted for automatically (summing per-chunk durations would
        // overcount by the concurrency factor).
        let transfer_started = Instant::now();
        let cycle = parallel_backup_upload_cycle(work, concurrency, |candidate| async move {
            let sha = candidate.chunk.sha256.clone();
            let bytes = candidate.chunk.bytes;
            // Checked when this candidate is *polled*, not when the stream was
            // built, so the deadline applies to work not yet started.
            if deadline.is_some_and(|d| Instant::now() >= d) {
                return (sha, bytes, Ok(UploadOneOutcome::DeadlineReached));
            }
            let outcome = engine.upload_one_backup_candidate(&candidate, &sha).await;
            (sha, bytes, outcome)
        })
        .await;
        stats.transfer_secs = transfer_started.elapsed().as_secs_f64();
        if cycle.deadline_skipped > 0 {
            tracing::info!(
                target: "fold_db::sync::backup",
                uploaded = cycle.uploaded,
                skipped = cycle.deadline_skipped,
                budget_secs = backup_catchup_cycle_budget().as_secs(),
                "catch-up cycle hit its wall-clock budget; held cut resumes next cycle"
            );
        }
        let cache_changed = cycle.uploaded > 0 || cycle.already_present > 0;
        stats.uploaded = cycle.uploaded;
        stats.already_present = stats.already_present.saturating_add(cycle.already_present);
        stats.failed = cycle.failed;
        stats.quota_exceeded = cycle.quota_exceeded;
        stats.bytes_uploaded = cycle.bytes_uploaded;
        if !cycle.source_missing.is_empty() {
            let newly_missing = cycle.source_missing.len();
            {
                let mut missing = self.backup_unresolvable.lock().await;
                let missing = missing.for_generation(target_generation);
                missing.extend(cycle.source_missing);
                stats.source_missing = missing.len();
            }
            // WARN, not debug: a cut that names chunks with no source can never
            // reach 100% present, so this is the moment the held cut becomes
            // unpublishable. Logging it once per cycle (the set only grows as
            // new dead shas are discovered) keeps it out of the flood class
            // that cost the 2026-08-05 diagnosis its log history.
            tracing::warn!(
                target: "fold_db::sync::backup",
                newly_missing,
                source_missing = stats.source_missing,
                generation = ?target_generation,
                "backup cut names chunks whose local sealed file is gone; \
                 skipping them so the drain advances — this cut cannot publish \
                 and is a candidate for abandon/re-cut under the reseal bound"
            );
        }
        if let Some(e) = cycle.first_error {
            // Record the cause unconditionally. `already_present` counts chunks
            // a PREVIOUS session landed in cloud; it is not evidence that THIS
            // cycle achieved anything, so it must not be what decides whether
            // the error is kept. Gating the text on it is how a home whose
            // every PUT was rejected still reported a clean cycle: 10,723
            // already-present chunks made the condition below false, the error
            // was dropped on the floor, and nothing downstream could name why
            // 868 chunks were not moving.
            stats.first_error = Some(redact_sync_error_text(&e.to_string()));
            if stats.uploaded == 0 && stats.already_present == 0 {
                return Err(e);
            }
        }

        // Recompute present/remaining after successful puts updated known set.
        {
            let known = self.backup_known_present.lock().await;
            let mut present = 0usize;
            let mut bytes_remaining = 0u64;
            for candidate in &candidates {
                if known.contains(&candidate.chunk.sha256) {
                    present += 1;
                } else {
                    bytes_remaining = bytes_remaining.saturating_add(candidate.chunk.bytes);
                }
            }
            stats.chunks_present = present;
            stats.bytes_remaining = bytes_remaining;
        }
        if cache_changed {
            self.persist_backup_presence_cache().await;
        }

        Ok(stats)
    }

    /// Pure plan for one parallel drain cycle: how many PUTs may run at once.
    /// Exposed for unit tests so concurrency policy is not re-implemented.
    #[must_use]
    pub fn backup_upload_concurrency_for_test(
        policy_concurrency: usize,
        catching_up: bool,
    ) -> usize {
        backup_upload_concurrency(policy_concurrency, catching_up)
    }

    /// Replace or clear the process-local backup PUT concurrency override.
    ///
    /// This changes subsequent scheduling only: in-flight PUTs are not
    /// cancelled and the held backup cut remains intact.
    pub async fn set_backup_upload_concurrency_override(
        &self,
        value: Option<usize>,
    ) -> Result<BackupUploadConcurrencyStatus, String> {
        if value.is_some_and(|value| !(1..=32).contains(&value)) {
            return Err("backup upload concurrency must be between 1 and 32".to_string());
        }
        *self.backup_upload_concurrency_override.lock().await = value;
        Ok(self.backup_upload_concurrency_status().await)
    }

    /// Status for the explicit override layer. Environment remains
    /// authoritative; adaptive/catch-up policy applies when neither override
    /// is set.
    pub async fn backup_upload_concurrency_status(&self) -> BackupUploadConcurrencyStatus {
        let runtime_override = *self.backup_upload_concurrency_override.lock().await;
        backup_upload_concurrency_status(runtime_override)
    }

    pub(super) async fn effective_backup_upload_concurrency(
        &self,
        policy_concurrency: usize,
        catching_up: bool,
    ) -> usize {
        let runtime_override = *self.backup_upload_concurrency_override.lock().await;
        backup_upload_concurrency_with_runtime(policy_concurrency, catching_up, runtime_override)
    }

    /// Pure plan for one drain cycle: how many PUTs it may complete.
    /// Exposed for unit tests so budget policy is not re-implemented.
    #[must_use]
    pub fn backup_upload_target_for_test(catching_up: bool) -> usize {
        backup_upload_target_per_cycle(catching_up)
    }

    /// Presign + PUT + confirm one candidate; caches presence on success.
    pub(super) async fn upload_one_backup_candidate(
        &self,
        candidate: &BackupChunkUploadCandidate,
        sha: &str,
    ) -> SyncResult<UploadOneOutcome> {
        // Before spending a presign round trip: is the source still there? A
        // held cut names shas that LastStore may have resealed away, and those
        // candidates fail after the presign, once per attempt, forever. On the
        // 2026-08-06 primary that was 256 dead chunks × ~20 attempts/cycle —
        // 5,120 presign calls per log rotation buying nothing.
        if !tokio::fs::try_exists(&candidate.path).await.unwrap_or(true) {
            return Ok(UploadOneOutcome::SourceMissing);
        }
        let presign = self
            .auth
            .presign_backup_chunk_upload(sha, candidate.chunk.bytes)
            .await?;
        if presign.already_present {
            self.backup_known_present
                .lock()
                .await
                .insert(sha.to_string());
            return Ok(UploadOneOutcome::AlreadyPresent);
        }
        let url = presign.url.ok_or_else(|| {
            SyncError::Auth("backup chunk upload presign returned no URL".to_string())
        })?;
        let verify_candidate = candidate.clone();
        let span = tracing::Span::current();
        let source = tokio::task::spawn_blocking(move || {
            span.in_scope(|| open_verified_backup_candidate(&verify_candidate))
        })
        .await
        .map_err(|error| SyncError::Storage(format!("verify backup chunk task: {error}")))??;
        let Some(source) = source else {
            return Ok(UploadOneOutcome::SourceMissing);
        };
        let started = Instant::now();
        self.s3
            .upload_backup_chunk_file(&url, source, candidate.chunk.bytes)
            .await?;
        self.auth.confirm_backup_chunk_upload(sha).await?;
        self.backup_known_present
            .lock()
            .await
            .insert(sha.to_string());
        self.upload_policy
            .record_upload_sample(candidate.chunk.bytes, started.elapsed().as_secs_f64());
        Ok(UploadOneOutcome::Uploaded)
    }
}
// lint:file-size-ok moved verbatim from backup_uploader.rs; cohesive unit, split further in a later pass
