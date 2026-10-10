use super::*;

impl SyncEngine {
    pub async fn laststore_cloud_snapshot(
        &self,
        previous_manifest: Option<&BackupManifest>,
    ) -> SyncResult<(BackupManifest, LastStoreCloudSnapshotReport)> {
        // Serialize against the continuous publisher before either caller can
        // select, drain, CAS, or retire the shared publish target.
        let _publish_turn = self.backup_publish_turn.lock().await;
        let current_previous = self.effective_previous_manifest(previous_manifest)?;
        // One automatic retry after store_uuid_mismatch (epoch rebind) or
        // stale_counter against a *different* cloud tip (counter observe).
        self.laststore_cloud_snapshot_once(current_previous.as_ref(), true, true, None)
            .await
    }

    /// Publish only the fenced cut. A source gap or CAS conflict leaves the
    /// resume marker to the caller and never creates an unfenced replacement.
    pub(crate) async fn reconcile_primary_resume_snapshot(
        &self,
        expected: &super::super::primary_resume::PrimaryResumeCutIdentity,
    ) -> SyncResult<LastStoreCloudSnapshotReport> {
        if !self.backup_only_mode.load(Ordering::Acquire)
            || self.primary_resume_frontier.lock().await.is_none()
            || !self.has_backup_publish_target().await
        {
            return Err(SyncError::Storage(
                "primary resume snapshot requires a fenced backup-only cut".into(),
            ));
        }
        let _publish_turn = self.backup_publish_turn.lock().await;
        let (_, report) = PAUSED_HOME_BACKUP_UPLOAD
            .scope(
                (),
                self.laststore_cloud_snapshot_once(None, false, false, Some(expected)),
            )
            .await?;
        Ok(report)
    }

    pub(super) async fn require_held_primary_resume_cut(
        &self,
        expected: &super::super::primary_resume::PrimaryResumeCutIdentity,
    ) -> SyncResult<()> {
        let target = self.backup_publish_target.lock().await;
        let manifest = &target
            .as_ref()
            .ok_or_else(missing_backup_publish_target_for_cas)?
            .manifest;
        require_primary_resume_manifest_identity(manifest, expected)
    }

    pub(super) async fn require_primary_resume_previous_latest(
        &self,
        expected: &super::super::primary_resume::PrimaryResumeCutIdentity,
    ) -> SyncResult<()> {
        if let Some(latest) = self.auth.backup_latest_get_optional().await? {
            latest.latest.require_supported_format()?;
            if !expected.matches_previous_latest(&latest.latest) {
                return Err(SyncError::Storage(
                    "primary resume normal backup/latest changed before CAS".into(),
                ));
            }
        } else if expected.previous_latest.is_some() {
            return Err(SyncError::Storage(
                "primary resume normal backup/latest disappeared before CAS".into(),
            ));
        }
        Ok(())
    }

    /// A manual owner repair can commit a newer tip while cloud is Off. Read
    /// its CAS-confirmed mirror inside the publication turn so a stale worker
    /// cannot carry retired body chunks into the next snapshot.
    pub(super) fn effective_previous_manifest(
        &self,
        previous: Option<&BackupManifest>,
    ) -> SyncResult<Option<BackupManifest>> {
        let Some(path) = self.backup_manifest_cache_path() else {
            return Ok(previous.cloned());
        };
        let Some(cached) = Self::read_mirrored_backup_keep_set(&path)? else {
            return Ok(previous.cloned());
        };
        if let Some(old) = previous {
            if cached.store_uuid != old.store_uuid {
                return Err(SyncError::Storage(
                    "backup predecessor cache source mismatch".into(),
                ));
            }
            let old_position = (old.epoch, old.counter);
            let cached_position = (cached.epoch, cached.counter);
            if cached_position < old_position {
                return Ok(Some(old.clone()));
            }
            if cached_position == old_position
                && manifest_sha256_hex(old)? != manifest_sha256_hex(&cached)?
            {
                return Err(SyncError::Storage(
                    "backup predecessor cache conflicts at same counter".into(),
                ));
            }
        }
        Ok(Some(cached))
    }

    /// Bounded read of the mirrored keep set at `path`. `Ok(None)` when the
    /// file does not exist; an unreadable, oversized, or invalid file is an
    /// error so a caller never treats a torn mirror as an empty keep set.
    pub(super) fn read_mirrored_backup_keep_set(
        path: &std::path::Path,
    ) -> SyncResult<Option<BackupManifest>> {
        let read_bounded = || -> std::io::Result<Vec<u8>> {
            use std::io::Read;
            let mut bytes = Vec::new();
            std::fs::File::open(path)?
                .take(16 * 1024 * 1024 + 1)
                .read_to_end(&mut bytes)?;
            Ok(bytes)
        };
        let bytes = match read_bounded() {
            Ok(bytes) if bytes.len() <= 16 * 1024 * 1024 => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            _ => {
                return Err(SyncError::Storage(
                    "backup predecessor cache unreadable or over budget".into(),
                ))
            }
        };
        let cached: BackupManifest = serde_json::from_slice(&bytes)
            .map_err(|_| SyncError::Storage("backup predecessor cache is invalid".into()))?;
        Ok(Some(cached))
    }

    // lint:fn-size-ok moved verbatim from backup_uploader.rs; splitting this function is separate work.
    pub(super) async fn laststore_cloud_snapshot_once(
        &self,
        previous_manifest: Option<&BackupManifest>,
        allow_rebind_retry: bool,
        allow_recut: bool,
        expected_primary_resume_cut: Option<
            &super::super::primary_resume::PrimaryResumeCutIdentity,
        >,
    ) -> SyncResult<(BackupManifest, LastStoreCloudSnapshotReport)> {
        if !self.cloud_plane_allows_upload().await {
            return Err(SyncError::Storage(
                "cloud plane Off: backup snapshot refused (lastdb cloud off)".into(),
            ));
        }
        if allow_rebind_retry {
            self.observe_cloud_latest_counter_before_cut().await;
        }
        if let Some(expected) = expected_primary_resume_cut {
            self.require_held_primary_resume_cut(expected).await?;
        } else {
            self.ensure_backup_publish_target(previous_manifest).await?;
        }
        let (mut upload_stats, mut generation) = self.drain_backup_publish_target(true).await?;

        // Same cloud-presence heal + reseal-bound abandon as the continuous
        // publisher. Operator/UDS snapshot path always allows re-cut.
        if upload_stats.source_missing > 0 {
            let heal = self
                .resolve_source_missing_via_cloud_presence(generation)
                .await;
            if heal.remaining < upload_stats.source_missing {
                upload_stats.chunks_present = self
                    .count_held_candidates_in_known_present()
                    .await
                    .unwrap_or(upload_stats.chunks_present);
            }
            upload_stats.source_missing = heal.remaining;
            if heal.probe_error && upload_stats.source_missing > 0 {
                // Could not confirm cloud presence for at least one digest
                // this cycle (transport/auth error) — fail closed, retry the
                // snapshot rather than abandoning on an unconfirmed residual.
                return Err(SyncError::Network(format!(
                    "backup snapshot could not confirm cloud presence for {} chunk(s) this \
                     cycle (transport/auth error); not abandoning — retry the snapshot",
                    upload_stats.source_missing
                )));
            }
        }
        while upload_stats.source_missing > 0 {
            if !allow_recut {
                return Err(SyncError::Storage(format!(
                    "primary resume cut has {} source-missing chunks; retry with a new fenced cut",
                    upload_stats.source_missing
                )));
            }
            let abandoned = self
                .handle_unpublishable_backup_cut(upload_stats.source_missing, generation)
                .await;
            if !abandoned {
                let consecutive = self.backup_consecutive_reseal_kills.load(Ordering::Relaxed);
                return Err(SyncError::Storage(format!(
                    "backup snapshot cannot complete ({} chunks not in cloud, of which {} have \
                     no local sealed file); automatic re-cut stopped after {consecutive} \
                     consecutive reseal-killed cuts (max={MAX_CONSECUTIVE_RESEAL_KILLED_CUTS}) \
                     — reseal rate is the defect (uploaded_this_drain={})",
                    upload_stats
                        .known_missing_chunks()
                        .unwrap_or(upload_stats.source_missing),
                    upload_stats.source_missing,
                    upload_stats.uploaded
                )));
            }
            self.ensure_backup_publish_target(previous_manifest).await?;
            let next = self.drain_backup_publish_target(true).await?;
            upload_stats = next.0;
            generation = next.1;
            if upload_stats.source_missing > 0 {
                let heal = self
                    .resolve_source_missing_via_cloud_presence(generation)
                    .await;
                if heal.remaining < upload_stats.source_missing {
                    upload_stats.chunks_present = self
                        .count_held_candidates_in_known_present()
                        .await
                        .unwrap_or(upload_stats.chunks_present);
                }
                upload_stats.source_missing = heal.remaining;
                if heal.probe_error && upload_stats.source_missing > 0 {
                    return Err(SyncError::Network(format!(
                        "backup snapshot could not confirm cloud presence for {} chunk(s) this \
                         cycle (transport/auth error); not abandoning — retry the snapshot",
                        upload_stats.source_missing
                    )));
                }
            }
        }

        // The drain already knows whether this cut can possibly be complete, and
        // it knows it locally. `try_verify_manifest_chunks_present` costs one
        // network round trip per chunk it has not already cached, sequentially —
        // so asking it about a cut with thousands of un-uploaded chunks spends
        // thousands of round trips to re-derive "incomplete", which the drain
        // just established for free.
        //
        // Short-circuit on local state only. This is strictly conservative: it
        // can only *skip* a verify that was going to fail, never admit a CAS the
        // verify would have blocked.
        // Operator snapshot must finish the held cut instead of 500-ing on the
        // first incomplete drain (a fresh home recut is tens of hash-group
        // segs; one cycle uploads a handful). Keep looping only while a cycle
        // actually uploaded; a no-progress cycle used to sleep-wait because
        // `will_finish` is true whenever sealed-home is not demoted. The
        // freeze+upload window is hours on a multi-GiB node (Tom 2026-08-19
        // packing-lock photograph): a 300s cap 500s a progressing drain.
        // Default 4h; override with LASTDB_OPERATOR_SNAPSHOT_DRAIN_SECS or
        // LASTDB_UDS_ADMIN_TIMEOUT_SECS (raise the daemon's admin timeout to
        // match, or the UDS handler 503s first).
        let drain_started = Instant::now();
        let drain_deadline = Self::operator_snapshot_drain_deadline();
        while let Some(missing) = upload_stats.known_missing_chunks() {
            let will_finish = self.continuous_held_cut_drain_active().await;
            if !Self::operator_snapshot_keep_draining(
                will_finish,
                upload_stats.uploaded,
                drain_started.elapsed(),
                drain_deadline,
            ) {
                return Err(Self::incomplete_cut_operator_error(
                    missing,
                    upload_stats.uploaded,
                    will_finish,
                ));
            }
            let next = self.drain_backup_publish_target(true).await?;
            upload_stats = next.0;
            let _ = next.1;
        }
        if let Some(expected) = expected_primary_resume_cut {
            self.require_held_primary_resume_cut(expected).await?;
        }
        let out = self
            .cas_backup_publish_target_inner(
                allow_rebind_retry,
                allow_recut,
                previous_manifest,
                expected_primary_resume_cut,
            )
            .await;
        if let Ok((manifest, _)) = &out {
            self.mirror_backup_keep_set(manifest);
        }
        out
    }
}
