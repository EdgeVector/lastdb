use super::gc_execute_batches::GcKeep;
use super::*;

impl SyncEngine {
    pub(super) async fn execute_backup_gc_job(
        &self,
        live_manifests: &[BackupManifest],
        dry_run: bool,
        generation_guard: Option<u64>,
        keep_proof: BackupGcKeepProof,
        job_id: &str,
    ) -> SyncResult<Option<BackupOrphanGcReport>> {
        self.backup_gc_jobs.phase(job_id, "wait_executor")?;
        let _gc_single_flight = self.backup_orphan_gc_mutex.lock().await;
        if let Some(g) = generation_guard {
            if self.post_cas_backup_gc_superseded(g) {
                return Ok(None);
            }
        }
        // Published tips + in-flight cut (when held). Without the in-flight
        // hold, every chunk the drain uploaded for the next generation that is
        // not already on the previous tip is classified as an orphan and
        // deleted — then known_present is cleared, so the drain re-uploads and
        // GC deletes again (livelock).
        self.backup_gc_jobs.phase(job_id, "wait_publication")?;
        let (keep_parts, publication_state, refreshed_keep) = self
            .gc_capture_keep_parts(live_manifests, keep_proof)
            .await?;
        let live_manifests: &[BackupManifest] = refreshed_keep.as_deref().unwrap_or(live_manifests);
        let keep = Self::gc_validated_keep_set(live_manifests, &keep_parts)?;
        self.gc_record_selection(job_id, &keep, live_manifests, &publication_state)?;
        // Fail closed above before even listing cloud objects: an empty keep
        // set must never produce an orphan count, much less reach DELETE.
        self.backup_gc_jobs.phase(job_id, "list")?;
        let listed = self.auth.list_objects("backup/chunks/").await?;
        let cloud_shas = shas_from_backup_chunk_listing(&listed);
        let footprint = backup_storage_footprint_from_listing(&listed, &keep);
        self.store_backup_storage_footprint(footprint);
        let keep_refs: Vec<&str> = keep.iter().map(String::as_str).collect();
        let cloud_refs: Vec<&str> = cloud_shas.iter().map(String::as_str).collect();
        let orphans = select_orphan_backup_chunk_shas(cloud_refs, keep_refs);
        let mut report = BackupOrphanGcReport {
            cloud_chunks_listed: cloud_shas.len(),
            live_referenced: keep.len(),
            published_referenced: keep_parts.published.len(),
            in_flight_referenced: keep_parts.in_flight.len(),
            orphans_selected: orphans.len(),
            deleted: 0,
            failed: 0,
            superseded: false,
            referenced_bytes: footprint.referenced_bytes,
            billed_bytes: footprint.billed_bytes,
            reclaimable_bytes: footprint.reclaimable_bytes,
        };
        self.backup_gc_jobs
            .update(job_id, |job| job.report = Some(report.clone()))?;
        if dry_run || orphans.is_empty() {
            self.gc_finish_read_only_report(
                live_manifests,
                keep_proof,
                &publication_state,
                &mut report,
                dry_run,
            )
            .await;
            return Ok(Some(report));
        }
        let sizes = Self::gc_orphan_object_sizes(&listed)?;
        self.backup_gc_jobs.phase(job_id, "presign")?;
        self.gc_delete_orphan_batches(
            job_id,
            &orphans,
            &sizes,
            GcKeep {
                live_manifests,
                keep_proof,
                publication_state: &publication_state,
            },
            &mut report,
        )
        .await?;
        if report.deleted > 0 {
            self.persist_backup_presence_cache().await;
        }
        Self::gc_log_execute_report(&report);
        Ok(Some(report))
    }

    /// Capture the keep partitions and publication state in one publish turn.
    async fn gc_capture_keep_parts(
        &self,
        live_manifests: &[BackupManifest],
        keep_proof: BackupGcKeepProof,
    ) -> SyncResult<(
        BackupOrphanGcKeepParts,
        BackupGcPublicationState,
        Option<Vec<BackupManifest>>,
    )> {
        // Keep the local tip and held-target snapshot in the same publish
        // turn. The turn is released before the remote object listing.
        let _publish_turn = self.backup_publish_turn.lock().await;
        // The supplied keep set was read before this wait. A publish that
        // landed during the wait moved the process tip past it; re-resolve
        // from the mirror this process wrote, still under the turn.
        let refreshed = self
            .refresh_keep_set_for_current_process_tip(live_manifests)
            .await?;
        let live_manifests: &[BackupManifest] = refreshed.as_deref().unwrap_or(live_manifests);
        let state = self
            .capture_backup_gc_publication_state_for_keep_set(live_manifests, keep_proof)
            .await?;
        let mut parts = BackupOrphanGcKeepParts::from_published_manifests(live_manifests);
        if let Some(target) = state.in_flight.as_ref() {
            parts
                .in_flight
                .extend(target.referenced_shas.iter().cloned());
        }
        Ok((parts, state, refreshed))
    }

    /// Fail closed: callers that named published tip(s) must not produce a
    /// keep-set whose published partition omits those tips' digests (the
    /// empty-published-keep defect: only held-cut digests survive, then
    /// N-only cloud objects are DELETE'd while latest still points at N).
    fn gc_validated_keep_set(
        live_manifests: &[BackupManifest],
        keep_parts: &BackupOrphanGcKeepParts,
    ) -> SyncResult<std::collections::BTreeSet<String>> {
        if !live_manifests.is_empty() && keep_parts.published.is_empty() {
            return Err(SyncError::Storage(
                "backup orphan GC refused: published tip(s) supplied but published keep partition \
                 is empty (would exclude live published tip)"
                    .into(),
            ));
        }
        for m in live_manifests {
            for d in manifest_referenced_chunk_shas(m) {
                if !keep_parts.published.contains(&d) {
                    return Err(SyncError::Storage(format!(
                        "backup orphan GC refused: published tip counter={} digest excluded from \
                         published keep partition",
                        m.counter
                    )));
                }
            }
        }
        let keep = keep_parts.keep_union();
        if keep.is_empty() {
            return Err(SyncError::Storage(
                "backup orphan GC refused: empty keep set (no published manifest or in-flight cut)"
                    .into(),
            ));
        }
        Ok(keep)
    }

    /// Stamp the job with the selection (cloud scope, tips, keep digest).
    fn gc_record_selection(
        &self,
        job_id: &str,
        keep: &std::collections::BTreeSet<String>,
        live_manifests: &[BackupManifest],
        publication_state: &BackupGcPublicationState,
    ) -> SyncResult<()> {
        use sha2::{Digest, Sha256};
        let mut digest = Sha256::new();
        for sha in keep {
            digest.update(sha.as_bytes());
        }
        let selection = super::super::gc_jobs::GcSelection {
            cloud_db_hash: self
                .laststore_backup_source
                .as_ref()
                .map(|source| source.cloud_db_hash())
                .transpose()
                .map_err(|e| SyncError::Storage(e.to_string()))?
                .flatten(),
            post_cas_generation: publication_state.post_cas_generation,
            published_manifest_sha256: live_manifests
                .iter()
                .map(manifest_sha256_hex)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| SyncError::Storage(e.to_string()))?,
            keep_sha256: format!("{:x}", digest.finalize()),
        };
        self.backup_gc_jobs
            .update(job_id, |job| job.selection = Some(selection))
    }

    /// Dry-run / zero-orphan path: mark staleness and log the report.
    async fn gc_finish_read_only_report(
        &self,
        live_manifests: &[BackupManifest],
        keep_proof: BackupGcKeepProof,
        publication_state: &BackupGcPublicationState,
        report: &mut BackupOrphanGcReport,
        dry_run: bool,
    ) {
        // Even a read-only or zero-work report must say when its selection
        // became stale during the remote listing. It never serves as proof
        // for a later execute sweep.
        let publish_turn = self.backup_publish_turn.lock().await;
        report.superseded = match self
            .capture_backup_gc_publication_state_for_keep_set(live_manifests, keep_proof)
            .await
        {
            Ok(current) => &current != publication_state,
            Err(e) => {
                tracing::warn!(
                    target: "fold_db::sync::backup",
                    error = %redact_sync_error_text(&e.to_string()),
                    "backup orphan GC could not revalidate publication state after listing; report is superseded"
                );
                true
            }
        };
        drop(publish_turn);
        tracing::info!(
            target: "fold_db::sync::backup",
            orphans_selected = report.orphans_selected,
            deleted = 0usize,
            failed = 0usize,
            superseded = report.superseded,
            dry_run,
            cloud_chunks_listed = report.cloud_chunks_listed,
            live_referenced = report.live_referenced,
            published_referenced = report.published_referenced,
            in_flight_referenced = report.in_flight_referenced,
            "backup orphan GC report"
        );
    }
}
