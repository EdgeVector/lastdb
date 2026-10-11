use super::*;

impl SyncEngine {
    pub(super) fn gc_orphan_object_sizes(
        listed: &[crate::sync::auth::S3ObjectInfo],
    ) -> SyncResult<std::collections::HashMap<&str, u64>> {
        let mut sizes = std::collections::HashMap::new();
        for object in listed {
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
        Ok(sizes)
    }

    /// Delete orphans batch by batch, re-checking publication state per batch.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn gc_delete_orphan_batches(
        &self,
        job_id: &str,
        orphans: &[String],
        sizes: &std::collections::HashMap<&str, u64>,
        live_manifests: &[BackupManifest],
        keep_proof: BackupGcKeepProof,
        publication_state: &BackupGcPublicationState,
        report: &mut BackupOrphanGcReport,
    ) -> SyncResult<()> {
        for batch in orphans.chunks(BACKUP_GC_DELETE_BATCH) {
            // Re-check once per batch. Hold the turn across every DELETE in
            // the batch so this process cannot publish a new reference to
            // those SHAs while the HTTP calls are in flight.
            let publish_turn = self.backup_publish_turn.lock().await;
            let current_state = self
                .capture_backup_gc_publication_state_for_keep_set(live_manifests, keep_proof)
                .await;
            let mut now_keep: Option<std::collections::BTreeSet<String>> = None;
            let publication_changed = match &current_state {
                Ok(current) if current == publication_state => false,
                Ok(_) | Err(_) => true,
            };
            if publication_changed {
                report.superseded = true;
                let retargeted = self
                    .gc_retarget_batch_keep(
                        live_manifests,
                        keep_proof,
                        batch.len(),
                        report.deleted,
                        &current_state,
                    )
                    .await;
                let Some(keep) = retargeted else {
                    drop(publish_turn);
                    break;
                };
                now_keep = Some(keep);
            }

            let specs: Vec<(String, u64)> = batch
                .iter()
                .filter(|sha| {
                    !now_keep
                        .as_ref()
                        .is_some_and(|keep| keep.contains(sha.as_str()))
                })
                .map(|sha| (sha.clone(), sizes[sha.as_str()]))
                .collect();
            let deleted_shas = self
                .dispatch_backup_gc_deletes(job_id, &specs, report)
                .await?;
            drop(publish_turn);
            self.gc_confirm_chunk_deletes(&deleted_shas).await;
            self.backup_gc_jobs
                .update(job_id, |job| job.report = Some(report.clone()))?;
            if report.superseded {
                break;
            }
        }
        Ok(())
    }

    /// Publication changed before a batch: prove a fresh keep set for this
    /// batch. `None` means stop without DELETE.
    pub(super) async fn gc_retarget_batch_keep(
        &self,
        live_manifests: &[BackupManifest],
        keep_proof: BackupGcKeepProof,
        batch_size: usize,
        deleted_before_stop: usize,
        current_state: &SyncResult<BackupGcPublicationState>,
    ) -> Option<std::collections::BTreeSet<String>> {
        if let Err(e) = current_state {
            tracing::warn!(
                target: "fold_db::sync::backup",
                batch_size,
                deleted_before_stop,
                error = %redact_sync_error_text(&e.to_string()),
                "backup orphan GC publication revalidate failed before DELETE dispatch; retargeting this batch then stopping"
            );
        } else {
            tracing::info!(
                target: "fold_db::sync::backup",
                batch_size,
                deleted_before_stop,
                "backup orphan GC publication state changed before DELETE dispatch; retargeting this batch then stopping"
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
                return None;
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
                Some(parts.keep_union())
            }
            Err(e) => {
                tracing::warn!(
                    target: "fold_db::sync::backup",
                    error = %redact_sync_error_text(&e.to_string()),
                    "backup orphan GC could not prove a keep-set after publication change; stopping without DELETE"
                );
                None
            }
        }
    }

    /// Debit the storage meter for the reclaimed chunks. Best effort and
    /// outside the publication fence: a metering round trip must not
    /// delay snapshot CAS, and the daily storage audit heals a miss.
    pub(super) async fn gc_confirm_chunk_deletes(&self, deleted_shas: &[String]) {
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
    }

    pub(super) fn gc_log_execute_report(report: &BackupOrphanGcReport) {
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
    }
}
