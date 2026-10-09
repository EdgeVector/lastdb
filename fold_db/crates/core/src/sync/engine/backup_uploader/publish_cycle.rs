use super::*;

impl SyncEngine {
    /// One continuous publisher cycle: drain sealed chunks, then optionally CAS snapshot+F.
    ///
    /// Does not take any local write lock that blocks Mini mutations.
    /// When `allow_cas` is false, only sealed-chunk drain runs (CAS backoff path).
    /// When `allow_drain_puts` is false, the cycle still enumerates / refreshes
    /// remaining counts but skips the PUT fan-out (drain-PUT backoff path).
    /// When `allow_new_cut` is false (MutationLog demotion held-cut drain), never
    /// establish a new full-home publish target — only drain/CAS an existing hold.
    pub async fn run_snapshot_log_publish_cycle(
        &self,
        previous_manifest: Option<&BackupManifest>,
        last_publish: &mut Instant,
        allow_cas: bool,
        allow_drain_puts: bool,
        allow_new_cut: bool,
    ) -> SyncResult<SnapshotLogPublishCycleReport> {
        let mut report = SnapshotLogPublishCycleReport {
            publish_phase: format!("{:?}", PublishPhase::Building).to_ascii_lowercase(),
            ..Default::default()
        };
        let mut drain_elapsed: Option<Duration> = None;
        let outcome = self
            .run_snapshot_log_publish_cycle_inner(
                previous_manifest,
                last_publish,
                allow_cas,
                allow_drain_puts,
                allow_new_cut,
                &mut report,
                &mut drain_elapsed,
            )
            .await;

        // The cycle's phase is only known HERE. Recording the sample inline
        // right after the drain — which is what the code used to do — stamped
        // every sample with the `building` initializer, because every
        // `publish_phase` assignment happens after that point. `/api/status`
        // was pinned to "building" for the life of the process as a result.
        if let Some(elapsed) = drain_elapsed {
            if outcome.is_err() {
                report.publish_phase = "failing".into();
            }
            self.record_backup_progress_cycle(&report, elapsed);
        }
        outcome.map(|()| report)
    }

    /// The cycle body. Writes its outcome into `report` rather than returning
    /// it, so the caller can record one progress sample carrying the phase the
    /// cycle actually reached — including on the error path.
    ///
    /// `drain_elapsed` is set only once the drain has run; a cycle that returns
    /// early (upload gate closed, cut rate-limited) produced no drain and must
    /// not manufacture a sample.
    #[allow(clippy::too_many_arguments)] // report + drain_elapsed out-params; keep call site readable
                                         // lint:fn-size-ok moved verbatim from backup_uploader.rs; splitting this function is separate work.
    pub(super) async fn run_snapshot_log_publish_cycle_inner(
        &self,
        previous_manifest: Option<&BackupManifest>,
        last_publish: &mut Instant,
        allow_cas: bool,
        allow_drain_puts: bool,
        allow_new_cut: bool,
        report: &mut SnapshotLogPublishCycleReport,
        drain_elapsed: &mut Option<Duration>,
    ) -> SyncResult<()> {
        // Fail-closed upload gate (defense in depth; forever-loop also checks).
        if !self.cloud_plane_allows_upload().await {
            report.publish_phase = "idle".into();
            return Ok(());
        }

        // The operator snapshot and the continuous publisher share one held
        // target. Keep its full lifecycle in one turn so neither caller can
        // retire the target while the other caller verifies it for CAS.
        let _publish_turn = self.backup_publish_turn.lock().await;

        let current_previous = self.effective_previous_manifest(previous_manifest)?;
        let previous_manifest = current_previous.as_ref();

        let drain_started = Instant::now();
        let force_env = env_flag::var_truthy("LASTDB_SNAPSHOT_LOG_PUBLISH_EVERY_CYCLE");
        // Rate-limit *cutting* a new target (the sealed-file walk is the
        // expensive part). Draining an existing target is never rate-limited —
        // that is the work that has to finish. Enumerate-only (drain-PUT
        // backoff) still refreshes remaining counts on an existing target
        // every interval.
        let has_held = self.has_backup_publish_target().await;
        if !has_held {
            if !allow_new_cut {
                // Demoted continuous path: never open a new full-home seal.
                report.publish_phase = "idle".into();
                return Ok(());
            }
            if !force_env && last_publish.elapsed() < snapshot_log_publish_interval() {
                report.publish_phase = "idle".into();
                return Ok(());
            }
        }
        // Drain the sticky target, not a freshly re-derived live cut. Re-cutting
        // each attempt is what made a busy home non-convergent: the denominator
        // moved faster than the 16-put budget could close it.
        self.ensure_backup_publish_target(previous_manifest).await?;
        let (chunk_stats, generation) = self.drain_backup_publish_target(allow_drain_puts).await?;
        report.chunks = chunk_stats;
        report.target_generation = generation;
        // The drain round-tripped: a sample is owed for this cycle. The caller
        // records it once the phase is settled — progress is about sealed-chunk
        // presence, not CAS success, so status % still moves when CAS waits on
        // an incomplete target or fails outright.
        *drain_elapsed = Some(drain_started.elapsed());

        if !allow_drain_puts {
            // Enumerate-only: remaining counts refreshed; PUT fan-out skipped.
            report.publish_phase = "chunks_only".into();
            return Ok(());
        }

        if !allow_cas {
            report.publish_phase = "chunks_only".into();
            return Ok(());
        }

        // Proven unpublishability (`source_missing > 0` after cloud-presence
        // heal): abandon under the consecutive-reseal bound and re-cut so the
        // replacement names the post-reseal sealed set. A mere incomplete drain
        // (source still present) must NOT re-cut — that is the 2026-07-31
        // livelock. Loop is bound-limited by handle_unpublishable_backup_cut.
        //
        // Before treating residual source_missing as terminal, promote digests
        // already in cloud (or already in known_present) so a prior generation's
        // uploads can still satisfy CAS. Measured 2026-08-14: residual=215 was
        // reported as "cannot publish" without re-checking cloud presence.
        //
        // Demoted continuous drain (`allow_new_cut=false`) must not re-cut a
        // full-home snapshot. When residual source_missing remains under
        // demotion, abandon the held cut so the continuous loop returns to the
        // demoted-idle branch (mutation-log durability) instead of spinning
        // FAILING forever with a fixed residual (2026-08-14 primary: 6d+ on
        // remaining=215 all_source_missing auto_recut=disabled).
        if report.chunks.source_missing > 0 {
            let heal = self
                .resolve_source_missing_via_cloud_presence(report.target_generation)
                .await;
            if heal.remaining < report.chunks.source_missing {
                // Healed digests are now in known_present; refresh present count
                // so known_missing_chunks / CAS gates see the post-heal truth.
                report.chunks.chunks_present = self
                    .count_held_candidates_in_known_present()
                    .await
                    .unwrap_or(report.chunks.chunks_present);
            }
            report.chunks.source_missing = heal.remaining;
            if heal.probe_error && report.chunks.source_missing > 0 {
                // At least one presence check could not be confirmed this
                // cycle (transport/auth error) — fail closed, do not abandon
                // or re-cut on an unconfirmed residual. The next cycle
                // re-probes with a fresh presign call.
                report.publish_phase = "chunks_only".into();
                return Ok(());
            }
        }
        while report.chunks.source_missing > 0 {
            if !allow_new_cut {
                let generation = report.target_generation;
                let n = report.chunks.source_missing;
                tracing::error!(
                    target: "fold_db::sync::backup",
                    source_missing = n,
                    generation = ?generation,
                    "held backup cut is provably unpublishable under demoted continuous \
                     sealed-home (residual source_missing after cloud-presence heal); \
                     abandoning held cut so the publisher idles on mutation-log durability \
                     instead of spinning FAILING forever"
                );
                self.retire_backup_publish_target().await;
                self.record_sealed_base_abandoned_marker();
                report.publish_phase = "failing".into();
                report.chunks.first_error = Some(format!(
                    "backup cut abandoned: {n} chunk(s) have no local sealed file and are not \
                     in cloud; continuous sealed-home is demoted so automatic re-cut is \
                     disabled — held cut released; mutation-log plane is active durability; \
                     operator re-cut / bootstrap required for a new sealed-home base"
                ));
                // Keep source_missing=n on this cycle's sample so progress/status
                // records the terminal reason. The hold is already released; the
                // next demoted cycle has no target and goes idle.
                return Ok(());
            }
            let generation = report.target_generation;
            let abandoned = self
                .handle_unpublishable_backup_cut(report.chunks.source_missing, generation)
                .await;
            if !abandoned {
                report.publish_phase = "failing".into();
                if report.chunks.first_error.is_none() {
                    let consecutive = self.backup_consecutive_reseal_kills.load(Ordering::Relaxed);
                    report.chunks.first_error = Some(format!(
                        "backup cut cannot publish: {} chunk(s) have no local sealed file; \
                         {consecutive} consecutive reseal-killed cuts hit the re-cut bound \
                         (max={MAX_CONSECUTIVE_RESEAL_KILLED_CUTS}) — reseal rate is the defect",
                        report.chunks.source_missing
                    ));
                }
                return Ok(());
            }
            self.ensure_backup_publish_target(previous_manifest).await?;
            let (chunk_stats, generation) =
                self.drain_backup_publish_target(allow_drain_puts).await?;
            report.chunks = chunk_stats;
            report.target_generation = generation;
            if report.chunks.source_missing > 0 {
                let heal = self
                    .resolve_source_missing_via_cloud_presence(report.target_generation)
                    .await;
                if heal.remaining < report.chunks.source_missing {
                    report.chunks.chunks_present = self
                        .count_held_candidates_in_known_present()
                        .await
                        .unwrap_or(report.chunks.chunks_present);
                }
                report.chunks.source_missing = heal.remaining;
                if heal.probe_error && report.chunks.source_missing > 0 {
                    report.publish_phase = "chunks_only".into();
                    return Ok(());
                }
            }
        }

        // A target that is not yet fully uploaded cannot CAS. That is ordinary
        // progress, not a failure: reporting it as `Err` drove the 15-minute CAS
        // backoff and stamped `last_error` on a publisher that was working
        // exactly as intended, which is a large part of why a 10-day-old backup
        // still read green-ish. Keep draining the SAME target next cycle.
        if let Some(missing) = report.chunks.known_missing_chunks() {
            report.publish_phase = "draining".into();
            tracing::debug!(
                target: "fold_db::sync::snapshot_log",
                missing,
                generation = ?report.target_generation,
                uploaded_this_cycle = report.chunks.uploaded,
                "publish target still draining; holding the cut"
            );
            return Ok(());
        }

        report.publish_phase = format!("{:?}", PublishPhase::Publishing).to_ascii_lowercase();
        // Pass the same previous-manifest used at cut time so a store_uuid
        // rebind retry re-cuts under the same chain head (not a silent None).
        match self.cas_backup_publish_target(previous_manifest).await {
            Ok((manifest, snap)) => {
                *last_publish = Instant::now();
                report.snapshot_published = true;
                report.frontier_through = Some(snap.cut_csn);
                report.cas_counter = Some(snap.counter);
                report.snapshot_id = Some(snap.manifest_sha256.clone());
                report.gc_eligible_log_segments = snap.gc_eligible_log_segments;
                report.publish_phase =
                    format!("{:?}", PublishPhase::Published).to_ascii_lowercase();
                // Object-model view of what we just published (for logs / future GC).
                let payload = LatestCasPayload::from_backup_cut(
                    &snap.manifest_sha256,
                    snap.cut_csn,
                    snap.counter,
                    &manifest.store_uuid,
                    manifest.epoch,
                );
                let record = SnapshotRecord::v1(&snap.manifest_sha256, snap.cut_csn);
                tracing::info!(
                    target: "fold_db::sync::snapshot_log",
                    snapshot_id = %payload.snapshot_id,
                    frontier_through = snap.cut_csn,
                    cas_counter = payload.counter,
                    model_version = payload.model_version,
                    snapshot_model = record.model_version,
                    previous_counter = previous_manifest.map(|m| m.counter),
                    "CAS latest published (S, F, counter)"
                );
                report.published_manifest = Some(manifest);
                Ok(())
            }
            Err(e) => {
                // CAS/build failure: do not mark GC eligible (PublishPhase stays
                // non-Published). The caller stamps the sample `failing` and the
                // forever-loop records the failure, in that order, so the drain
                // progress operators need is still reported without the sample
                // laundering the failure it precedes.
                report.publish_phase = "chunks_only".into();
                Err(e)
            }
        }
    }

    /// Pure helper: count mutation-log segments that become GC-eligible after a successful CAS.
    pub fn count_gc_eligible_log_segments_after_cas(
        frontier_through: u64,
        log_through_ids: impl IntoIterator<Item = u64>,
    ) -> usize {
        let f = Frontier::scalar(frontier_through);
        log_through_ids
            .into_iter()
            .filter(|id| {
                let seg = MutationLogSegmentId::single_publisher(
                    *id,
                    MutationLogSegmentId::default_object_key(None, *id),
                );
                gc_eligibility_for_log(PublishPhase::Published, Some(&f), &seg)
                    == GcEligibility::EligibleAfterGrace
            })
            .count()
    }
}
