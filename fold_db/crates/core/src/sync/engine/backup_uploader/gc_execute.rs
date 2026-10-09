use super::*;

impl SyncEngine {
    // lint:fn-size-ok moved verbatim from backup_uploader.rs; splitting this function is separate work.
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
        let (keep_parts, publication_state, refreshed_keep) = {
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
            (parts, state, refreshed)
        };
        let live_manifests: &[BackupManifest] = refreshed_keep.as_deref().unwrap_or(live_manifests);
        // Fail closed: callers that named published tip(s) must not produce a
        // keep-set whose published partition omits those tips' digests (the
        // empty-published-keep defect: only held-cut digests survive, then
        // N-only cloud objects are DELETE'd while latest still points at N).
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
        {
            use sha2::{Digest, Sha256};
            let mut digest = Sha256::new();
            for sha in &keep {
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
                .update(job_id, |job| job.selection = Some(selection))?;
        }
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
            // Even a read-only or zero-work report must say when its selection
            // became stale during the remote listing. It never serves as proof
            // for a later execute sweep.
            let publish_turn = self.backup_publish_turn.lock().await;
            report.superseded = match self
                .capture_backup_gc_publication_state_for_keep_set(live_manifests, keep_proof)
                .await
            {
                Ok(current) => current != publication_state,
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
            return Ok(Some(report));
        }
        let mut sizes = std::collections::HashMap::new();
        for object in &listed {
            // Any remainder under backup/chunks/ (v1 semantics, unchanged);
            // a v2 key never has that prefix, so it can never size an orphan.
            if let Some(sha) = super::super::backup_keys::v1_chunk_remainder(&object.key) {
                if let Some(previous) = sizes.insert(sha, object.size) {
                    if previous != object.size {
                        return Err(SyncError::Storage(
                            "GC listing contains conflicting object sizes".into(),
                        ));
                    }
                }
            }
        }
        self.backup_gc_jobs.phase(job_id, "presign")?;
        for batch in orphans.chunks(BACKUP_GC_DELETE_BATCH) {
            // Presign only mints URLs. Keep it outside the publication turn
            // so a slow auth request cannot delay snapshot CAS.
            let presigns = join_all(
                batch
                    .iter()
                    .map(|sha| self.auth.presign_backup_chunk_delete(sha)),
            )
            .await;

            // Re-check once per batch. Hold the turn across every DELETE in
            // the batch so this process cannot publish a new reference to
            // those SHAs while the HTTP calls are in flight.
            let publish_turn = self.backup_publish_turn.lock().await;
            // SHAs whose DELETE the provider acknowledged in this batch; their
            // meter debit is confirmed after the publication fence is released.
            let mut deleted_shas: Vec<String> = Vec::new();
            let current_state = self
                .capture_backup_gc_publication_state_for_keep_set(live_manifests, keep_proof)
                .await;
            let mut now_keep: Option<std::collections::BTreeSet<String>> = None;
            let publication_changed = match &current_state {
                Ok(current) if current == &publication_state => false,
                Ok(_) | Err(_) => true,
            };
            if publication_changed {
                report.superseded = true;
                if let Err(e) = &current_state {
                    tracing::warn!(
                        target: "fold_db::sync::backup",
                        batch_size = batch.len(),
                        deleted_before_stop = report.deleted,
                        error = %redact_sync_error_text(&e.to_string()),
                        "backup orphan GC publication revalidate failed after DELETE presign; retargeting this batch then stopping"
                    );
                } else {
                    tracing::info!(
                        target: "fold_db::sync::backup",
                        batch_size = batch.len(),
                        deleted_before_stop = report.deleted,
                        "backup orphan GC publication state changed after DELETE presign; retargeting this batch then stopping"
                    );
                }
                let refreshed = match self
                    .refresh_keep_set_for_current_process_tip(live_manifests)
                    .await
                {
                    Ok(value) => value,
                    Err(e) => {
                        tracing::warn!(
                            target: "fold_db::sync::backup",
                            error = %redact_sync_error_text(&e.to_string()),
                            "backup orphan GC could not refresh keep-set after publication change; stopping without DELETE"
                        );
                        drop(publish_turn);
                        break;
                    }
                };
                let live = refreshed.as_deref().unwrap_or(live_manifests);
                match self
                    .capture_backup_gc_publication_state_for_keep_set(live, keep_proof)
                    .await
                {
                    Ok(state) => {
                        let mut parts = BackupOrphanGcKeepParts::from_published_manifests(live);
                        if let Some(target) = state.in_flight.as_ref() {
                            parts
                                .in_flight
                                .extend(target.referenced_shas.iter().cloned());
                        }
                        now_keep = Some(parts.keep_union());
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: "fold_db::sync::backup",
                            error = %redact_sync_error_text(&e.to_string()),
                            "backup orphan GC could not prove a keep-set after publication change; stopping without DELETE"
                        );
                        drop(publish_turn);
                        break;
                    }
                }
            }

            let mut failed_specs: Vec<(String, u64)> = Vec::new();
            let mut ready: Vec<(String, crate::sync::s3::PresignedUrl, u64)> = Vec::new();
            for (sha, presign) in batch.iter().zip(presigns) {
                if now_keep
                    .as_ref()
                    .is_some_and(|keep| keep.contains(sha.as_str()))
                {
                    continue;
                }
                let bytes = sizes[sha.as_str()];
                match presign {
                    Ok(url) => ready.push((sha.clone(), url, bytes)),
                    Err(e) => {
                        tracing::warn!(
                            target: "fold_db::sync::backup",
                            chunk_sha256 = %sha,
                            error = %redact_sync_error_text(&e.to_string()),
                            "backup orphan chunk delete presign failed; continuing sweep"
                        );
                        failed_specs.push((sha.clone(), bytes));
                    }
                }
            }

            if !failed_specs.is_empty() {
                report.failed += failed_specs.len();
                let intents = self.backup_gc_jobs.intent_batch(job_id, &failed_specs)?;
                self.backup_gc_jobs.outcome_batch(
                    intents
                        .into_iter()
                        .map(|intent| (intent, GcObjectOutcome::FailedBeforeDispatch))
                        .collect(),
                )?;
            }

            if !ready.is_empty() {
                let specs: Vec<(String, u64)> = ready
                    .iter()
                    .map(|(sha, _, bytes)| (sha.clone(), *bytes))
                    .collect();
                let intents = self.backup_gc_jobs.intent_batch(job_id, &specs)?;
                let mut known_present = self.backup_known_present.lock().await;
                let deletes = join_all(ready.iter().map(|(_, url, _)| self.s3.delete(url))).await;
                let mut outcomes = Vec::with_capacity(intents.len());
                for ((sha, _, _), (intent, result)) in
                    ready.iter().zip(intents.into_iter().zip(deletes))
                {
                    match result {
                        Ok(()) => {
                            report.deleted += 1;
                            known_present.remove(sha);
                            deleted_shas.push(sha.clone());
                            outcomes.push((intent, GcObjectOutcome::DeleteAcknowledged));
                        }
                        Err(e) => {
                            report.failed += 1;
                            // A timeout/error does not cancel a DELETE already
                            // accepted by the provider. Do not report exact
                            // reclaimed bytes.
                            outcomes.push((intent, GcObjectOutcome::Unknown));
                            tracing::warn!(
                                target: "fold_db::sync::backup",
                                chunk_sha256 = %sha,
                                error = %redact_sync_error_text(&e.to_string()),
                                "backup orphan chunk delete failed; continuing sweep"
                            );
                        }
                    }
                }
                // This fence covers only one live SyncEngine. A request timeout,
                // task cancellation, or process loss can release it while the
                // object store still accepts DELETE. A second process or device
                // does not share it. The current S3 API has no durable cancellation
                // or compare-and-delete token, so those remote protocol risks
                // remain outside this repair. A batch raises in-flight DELETEs
                // from 1 SHA to at most BACKUP_GC_DELETE_BATCH.
                drop(known_present);
                self.backup_gc_jobs.outcome_batch(outcomes)?;
            }
            drop(publish_turn);
            // Debit the storage meter for the reclaimed chunks. Best effort and
            // outside the publication fence: a metering round trip must not
            // delay snapshot CAS, and the daily storage audit heals a miss.
            let confirms = join_all(
                deleted_shas
                    .iter()
                    .map(|sha| self.auth.confirm_backup_chunk_delete(sha)),
            )
            .await;
            for (sha, result) in deleted_shas.iter().zip(confirms) {
                if let Err(e) = result {
                    tracing::warn!(
                        target: "fold_db::sync::backup",
                        chunk_sha256 = %sha,
                        error = %redact_sync_error_text(&e.to_string()),
                        "backup chunk delete metering confirm failed (non-fatal)"
                    );
                }
            }
            self.backup_gc_jobs
                .update(job_id, |job| job.report = Some(report.clone()))?;
            if report.superseded {
                break;
            }
        }
        if report.deleted > 0 {
            self.persist_backup_presence_cache().await;
        }
        tracing::info!(
            target: "fold_db::sync::backup",
            orphans_selected = report.orphans_selected,
            deleted = report.deleted,
            failed = report.failed,
            superseded = report.superseded,
            dry_run = false,
            cloud_chunks_listed = report.cloud_chunks_listed,
            live_referenced = report.live_referenced,
            published_referenced = report.published_referenced,
            in_flight_referenced = report.in_flight_referenced,
            "backup orphan GC report"
        );
        if report.failed > 0 {
            tracing::warn!(
                target: "fold_db::sync::backup",
                orphans_selected = report.orphans_selected,
                deleted = report.deleted,
                failed = report.failed,
                "backup orphan GC partial failure; local engine continues"
            );
        }
        Ok(Some(report))
    }
}
