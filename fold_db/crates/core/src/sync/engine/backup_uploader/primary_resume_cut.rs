use super::*;

pub(super) fn check_damage_mode(
    previous_manifest: Option<&BackupManifest>,
    primary_resume_root: bool,
    accept_local_damage: bool,
) -> SyncResult<()> {
    if accept_local_damage && (!primary_resume_root || previous_manifest.is_some()) {
        return Err(SyncError::Storage(
            "damage acceptance requires a fresh local root".into(),
        ));
    }
    Ok(())
}

impl SyncEngine {
    /// Create the one-writer resume cut while all serving mutations wait.
    /// The cloud presence list runs first; the fence covers the frontier read,
    /// persistence barrier, and manifest cut, but not the long upload.
    pub(crate) async fn prepare_primary_resume_snapshot_cut(
        &self,
        router: &crate::sync::capture::MutationLogCaptureRouter,
        old_cloud_frontier: u64,
        fresh_from_local: bool,
        accept_local_damage: bool,
    ) -> SyncResult<(u64, super::super::primary_resume::PrimaryResumeCutIdentity)> {
        if !self.backup_only_mode.load(Ordering::Acquire) {
            return Err(SyncError::Storage(
                "primary resume cut requires backup-only mode".into(),
            ));
        }
        if accept_local_damage && !fresh_from_local {
            return Err(SyncError::Storage(
                "local damage acceptance requires fresh mode".into(),
            ));
        }
        let (previous, latest, cloud_presence, fresh_proof) =
            self.primary_resume_cloud_inputs(fresh_from_local).await?;
        self.require_backup_file_pack_capability(previous.as_ref(), fresh_proof.is_some())
            .await?;
        let _publish_turn = self.backup_publish_turn.lock().await;
        let _mutation_fence = router.fence_mutations().await;
        if fresh_from_local {
            self.clear_backup_presence_cache_for_fresh_root().await?;
        }
        self.invoke_required_primary_resume_cut_barrier()
            .await
            .map_err(SyncError::Storage)?;
        let frontier = self
            .raise_primary_resume_writer_floor(old_cloud_frontier)
            .await?;
        self.retire_backup_publish_target().await;
        self.set_primary_resume_frontier(frontier).await?;
        self.ensure_backup_publish_target_with_presence(
            previous.as_ref(),
            Some(cloud_presence),
            true,
            accept_local_damage,
            fresh_proof.as_ref(),
            true,
        )
        .await?;
        let target = self.backup_publish_target.lock().await;
        let manifest = &target
            .as_ref()
            .ok_or_else(missing_backup_publish_target_for_cas)?
            .manifest;
        let identity = super::super::primary_resume::PrimaryResumeCutIdentity {
            store_uuid: manifest.store_uuid.clone(),
            epoch: manifest.epoch,
            counter: manifest.counter,
            manifest_sha256: manifest_sha256_hex(manifest).map_err(|error| {
                SyncError::Storage(format!("hash primary resume cut manifest: {error}"))
            })?,
            previous_latest: latest,
            local_missing_atom_groups: self
                .laststore_backup_source
                .as_ref()
                .unwrap()
                .report_committed_successor_history()
                .map_err(|error| SyncError::Storage(format!("report local atom groups: {error}")))?
                .groups_without_local_file,
        };
        Ok((frontier, identity))
    }

    async fn primary_resume_cloud_inputs(
        &self,
        fresh_from_local: bool,
    ) -> SyncResult<(
        Option<BackupManifest>,
        Option<BackupLatestPointer>,
        Option<CloudChunkPresence>,
        Option<FreshCloudProof>,
    )> {
        let (previous, latest, cloud_presence, fresh_proof) = if fresh_from_local {
            self.auth
                .require_backup_expected_absent_capability()
                .await?;
            if self.auth.backup_latest_get_optional().await?.is_some() {
                return Err(SyncError::Storage(
                    "fresh cloud backup requires absent backup/latest".into(),
                ));
            }
            let (presence, proof) = self.verified_fresh_cloud_presence().await?;
            (None, None, Some(presence), Some(proof))
        } else {
            let previous = self.effective_previous_manifest(None)?.ok_or_else(|| {
                SyncError::Storage(
                    "primary resume root cut requires the prior normal backup".into(),
                )
            })?;
            let previous_sha = manifest_sha256_hex(&previous).map_err(|error| {
                SyncError::Storage(format!("hash prior normal backup manifest: {error}"))
            })?;
            let latest = self.auth.backup_latest_get().await?;
            latest.latest.require_supported_format()?;
            if latest.latest.store_uuid != previous.store_uuid
                || latest.latest.epoch != previous.epoch
                || latest.latest.counter != previous.counter
                || !latest
                    .latest
                    .manifest_sha256
                    .eq_ignore_ascii_case(&previous_sha)
            {
                return Err(SyncError::Storage(
                    "primary resume root cut requires the exact current normal backup/latest"
                        .into(),
                ));
            }
            if !self
                .auth
                .require_backup_manifest_present(&previous_sha)
                .await?
            {
                return Err(SyncError::Storage(
                    "primary resume root cut predecessor manifest is absent from cloud".into(),
                ));
            }
            let presence = self.list_backup_chunk_presence_for_reconcile().await;
            if !presence.as_ref().is_some_and(|p| p.listing_complete) {
                return Err(SyncError::Storage(
                    "primary resume root cut requires a complete cloud chunk list".into(),
                ));
            }
            (Some(previous), Some(latest.latest), presence, None)
        };
        Ok((previous, latest, cloud_presence, fresh_proof))
    }

    pub(super) async fn cas_backup_latest_for_resume(
        &self,
        fresh_root: bool,
        manifest: &BackupManifest,
        manifest_sha256: &str,
    ) -> SyncResult<crate::sync::auth::ops::BackupLatestCasResponse> {
        if fresh_root {
            self.auth
                .backup_latest_cas_if_absent_for_format(
                    &manifest.store_uuid,
                    manifest.epoch,
                    manifest.counter,
                    manifest_sha256,
                    manifest.version,
                )
                .await
        } else {
            self.auth
                .backup_latest_cas_for_format(
                    &manifest.store_uuid,
                    manifest.epoch,
                    manifest.counter,
                    manifest_sha256,
                    manifest.version,
                )
                .await
        }
    }
}
