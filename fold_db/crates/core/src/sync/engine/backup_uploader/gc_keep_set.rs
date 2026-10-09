use super::*;

impl SyncEngine {
    pub(super) fn read_backup_gc_durable_tip(&self) -> SyncResult<(u64, Option<u64>, String)> {
        let store = self.laststore_backup_source.as_ref().ok_or_else(|| {
            SyncError::Storage(
                "backup orphan GC refused: no LastStore backup source for local tip proof".into(),
            )
        })?;
        let durability = store
            .backup_durability()
            .map_err(|e| {
                SyncError::Storage(format!(
                    "backup orphan GC refused: read local backup durability: {e}"
                ))
            })?
            .ok_or_else(|| {
                SyncError::Storage(
                    "backup orphan GC refused: no durable local backup marker".into(),
                )
            })?;
        let cloud_db_hash = store
            .cloud_db_hash()
            .map_err(|e| {
                SyncError::Storage(format!(
                    "backup orphan GC refused: read local cloud database identity: {e}"
                ))
            })?
            .ok_or_else(|| {
                SyncError::Storage(
                    "backup orphan GC refused: no durable local cloud database identity".into(),
                )
            })?;
        Ok((
            durability.backup_manifest_counter,
            durability.last_backup_commit_unix_secs,
            cloud_db_hash,
        ))
    }

    pub(super) fn validate_backup_gc_tip_identity(
        identity: Option<&BackupTipIdentity>,
        durable_counter: u64,
        last_commit_unix_secs: Option<u64>,
        cloud_db_hash: &str,
    ) -> SyncResult<()> {
        if durable_counter == 0 {
            if identity.is_some() {
                return Err(SyncError::Storage(
                    "backup orphan GC refused: process tip identity exists but durable counter is zero"
                        .into(),
                ));
            }
            return Err(SyncError::Storage(
                "backup orphan GC refused: no exact process identity at durable counter zero"
                    .into(),
            ));
        }
        if last_commit_unix_secs.is_none() {
            return Err(SyncError::Storage(format!(
                "backup orphan GC refused: durable counter {durable_counter} has no local commit stamp"
            )));
        }
        let identity = identity.ok_or_else(|| {
            SyncError::Storage(format!(
                "backup orphan GC refused: no exact process identity for durable tip counter {durable_counter}"
            ))
        })?;
        if identity.counter != durable_counter {
            return Err(SyncError::Storage(format!(
                "backup orphan GC refused: process tip counter {} differs from durable counter {durable_counter}",
                identity.counter
            )));
        }
        if cloud_db_hash_for_store_uuid(&identity.store_uuid) != cloud_db_hash {
            return Err(SyncError::Storage(
                "backup orphan GC refused: process tip belongs to a different local database"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Capture every local state component that can add a protected chunk.
    /// Caller holds `backup_publish_turn`.
    ///
    /// The durable high-water marker stores a counter and commit timestamp,
    /// but not the manifest digest or epoch. An observed foreign counter also
    /// preserves an older commit timestamp. Therefore neither the marker nor a
    /// caller-supplied manifest can establish exact tip identity after restart.
    ///
    /// [`BackupGcKeepProof::ExactProcessIdentity`] (post-CAS, and admin while
    /// this process still holds a CAS identity) refuses until this process
    /// records a successful CAS plus local commit.
    /// [`BackupGcKeepProof::VerifiedPublishedBody`] (quota hatch, and admin
    /// after restart) accepts a keep-set already resolved from last-committed
    /// or downloaded `backup/latest` when process identity is still `None`.
    /// After restart the body must also match the durable counter and live
    /// cloud `backup/latest`. A lagging sidecar is refuse, not a keep-set.
    pub(super) async fn capture_backup_gc_publication_state_for_keep_set(
        &self,
        live_manifests: &[BackupManifest],
        keep_proof: BackupGcKeepProof,
    ) -> SyncResult<BackupGcPublicationState> {
        self.backup_gc_jobs.require_current_engine()?;
        let (durable_counter, last_commit_unix_secs, cloud_db_hash) =
            self.read_backup_gc_durable_tip()?;
        let process_identity = self.backup_published_tip_identity.lock().await.clone();
        let mut cloud_latest = None;
        match keep_proof {
            BackupGcKeepProof::ExactProcessIdentity => {
                Self::validate_backup_gc_tip_identity(
                    process_identity.as_ref(),
                    durable_counter,
                    last_commit_unix_secs,
                    &cloud_db_hash,
                )?;
                Self::require_exact_keep_set_match(process_identity.as_ref(), live_manifests)?;
            }
            BackupGcKeepProof::VerifiedPublishedBody => {
                if live_manifests.is_empty() {
                    if durable_counter > 0 {
                        return Err(SyncError::Storage(
                            "backup orphan GC refused: known published tip but empty published keep \
                             (quota recovery will not DELETE with only the held cut)"
                                .into(),
                        ));
                    }
                    if process_identity.is_some() {
                        return Err(SyncError::Storage(
                            "backup orphan GC refused: process tip identity exists but published keep is empty"
                                .into(),
                        ));
                    }
                } else {
                    for manifest in live_manifests {
                        if cloud_db_hash_for_store_uuid(&manifest.store_uuid) != cloud_db_hash {
                            return Err(SyncError::Storage(
                                "backup orphan GC refused: published tip belongs to a different local database"
                                    .into(),
                            ));
                        }
                    }
                    if let Some(current) = process_identity.as_ref() {
                        Self::validate_backup_gc_tip_identity(
                            Some(current),
                            durable_counter,
                            last_commit_unix_secs,
                            &cloud_db_hash,
                        )?;
                        Self::require_exact_keep_set_match(Some(current), live_manifests)?;
                    } else if live_manifests.len() != 1 {
                        return Err(SyncError::Storage(format!(
                            "backup orphan GC refused: quota recovery expected one verified published tip, got {}",
                            live_manifests.len()
                        )));
                    } else {
                        let manifest = &live_manifests[0];
                        if manifest.counter != durable_counter {
                            return Err(SyncError::Storage(format!(
                                "backup orphan GC refused: published keep counter {} differs from durable counter {durable_counter}",
                                manifest.counter
                            )));
                        }
                        cloud_latest = Some(
                            self.require_published_body_matches_cloud_latest(manifest)
                                .await?,
                        );
                    }
                }
            }
        }
        let published_tip = match process_identity.as_ref() {
            Some(id) => Some(id.clone()),
            None => match live_manifests.first() {
                Some(manifest) => Some(BackupTipIdentity::from_manifest(manifest)?),
                None => None,
            },
        };
        let in_flight = self
            .backup_publish_target
            .lock()
            .await
            .as_ref()
            .map(|target| target.reachability_identity.clone());
        Ok(BackupGcPublicationState {
            post_cas_generation: self
                .post_cas_backup_gc_generation
                .load(std::sync::atomic::Ordering::SeqCst),
            published_tip,
            in_flight,
            cloud_latest,
        })
    }

    /// After restart, a supplied published body is keep-set only when it is
    /// the live cloud `backup/latest` pointer (same identity checks as
    /// [`Self::download_cloud_tip_manifest_body`]: uuid, epoch, counter, sha).
    pub(super) async fn require_published_body_matches_cloud_latest(
        &self,
        manifest: &BackupManifest,
    ) -> SyncResult<BackupTipIdentity> {
        let get = tokio::time::timeout(Duration::from_secs(2), self.auth.backup_latest_get())
            .await
            .map_err(|_| {
                SyncError::Storage(
                    "backup orphan GC refused: cloud backup/latest unreachable".into(),
                )
            })?
            .map_err(|e| {
                SyncError::Storage(format!(
                    "backup orphan GC refused: cloud backup/latest unreachable: {e}"
                ))
            })?;
        let latest = get.latest;
        latest.require_v1_format()?;
        let expected_sha = latest.manifest_sha256.trim();
        if expected_sha.is_empty() {
            return Err(SyncError::Storage(
                "backup orphan GC refused: cloud backup/latest has empty manifest_sha256".into(),
            ));
        }
        let body = BackupTipIdentity::from_manifest(manifest)?;
        if body.store_uuid != latest.store_uuid
            || body.epoch != latest.epoch
            || body.counter != latest.counter
            || !body.manifest_sha256.eq_ignore_ascii_case(expected_sha)
        {
            return Err(SyncError::Storage(
                "backup orphan GC refused: published keep-set is not live backup/latest".into(),
            ));
        }
        Ok(BackupTipIdentity {
            store_uuid: latest.store_uuid,
            epoch: latest.epoch,
            counter: latest.counter,
            manifest_sha256: expected_sha.to_string(),
        })
    }

    /// Admin `start_backup_gc` keep-set. A process that still holds a CAS
    /// identity uses the bounded sidecar reader. After restart the keep-set is
    /// live `backup/latest` (and the durable counter). A lagging sidecar is
    /// refuse, not a keep-set.
    pub(crate) async fn resolve_admin_backup_gc_keep_set(&self) -> SyncResult<Vec<BackupManifest>> {
        let has_identity = self.backup_published_tip_identity.lock().await.is_some();
        let sidecar = match self.backup_manifest_cache_path() {
            Some(path) => Self::read_mirrored_backup_keep_set(&path)?,
            None => None,
        };
        if has_identity {
            let manifest = sidecar.ok_or_else(|| {
                SyncError::Storage(
                    "backup orphan GC refused: no local backup manifest cache".into(),
                )
            })?;
            return Ok(vec![manifest]);
        }
        let (durable_counter, last_commit_unix_secs, cloud_db_hash) =
            self.read_backup_gc_durable_tip()?;
        if last_commit_unix_secs.is_none() {
            return Err(SyncError::Storage(format!(
                "backup orphan GC refused: durable counter {durable_counter} has no local commit stamp"
            )));
        }
        if durable_counter == 0 {
            return Err(SyncError::Storage(
                "backup orphan GC refused: no exact process identity at durable counter zero"
                    .into(),
            ));
        }
        // Cap the GET: a dead auth URL SYN-waits for minutes on some CI lanes.
        let get = tokio::time::timeout(Duration::from_secs(2), self.auth.backup_latest_get())
            .await
            .map_err(|_| {
                SyncError::Storage(
                    "backup orphan GC refused: cloud backup/latest unreachable".into(),
                )
            })?
            .map_err(|e| {
                SyncError::Storage(format!(
                    "backup orphan GC refused: cloud backup/latest unreachable: {e}"
                ))
            })?;
        let latest = get.latest;
        latest.require_v1_format()?;
        let expected_sha = latest.manifest_sha256.trim().to_string();
        if expected_sha.is_empty() {
            return Err(SyncError::Storage(
                "backup orphan GC refused: cloud backup/latest has empty manifest_sha256".into(),
            ));
        }
        if latest.counter != durable_counter {
            return Err(SyncError::Storage(format!(
                "backup orphan GC refused: cloud backup/latest counter {} differs from durable counter {durable_counter}",
                latest.counter
            )));
        }
        if cloud_db_hash_for_store_uuid(&latest.store_uuid) != cloud_db_hash {
            return Err(SyncError::Storage(
                "backup orphan GC refused: cloud backup/latest belongs to a different local database"
                    .into(),
            ));
        }
        if let Some(sidecar) = sidecar {
            let identity = BackupTipIdentity::from_manifest(&sidecar)?;
            if identity.store_uuid != latest.store_uuid
                || identity.epoch != latest.epoch
                || identity.counter != latest.counter
                || identity.counter != durable_counter
                || !identity.manifest_sha256.eq_ignore_ascii_case(&expected_sha)
            {
                return Err(SyncError::Storage(
                    "backup orphan GC refused: lagging sidecar keep-set (not live backup/latest)"
                        .into(),
                ));
            }
            return Ok(vec![sidecar]);
        }
        let body = self
            .download_cloud_tip_manifest_body(
                &latest.store_uuid,
                latest.epoch,
                latest.counter,
                &expected_sha,
            )
            .await?;
        Ok(vec![body])
    }

    /// Re-resolve a supplied keep set that predates the tip this process
    /// published. Caller holds `backup_publish_turn`.
    ///
    /// Manual GC reads the mirrored keep set before it waits for the publish
    /// turn. A CAS that lands during that wait (an operator snapshot drain
    /// holds the turn for the whole cut, 49 minutes on the 2026-09-12 primary)
    /// records a new process tip and mirrors it; the exact-copy check then
    /// refuses the old keep set with "0 exact copies of current local tip"
    /// although the caller could not have supplied anything newer. Adopt the
    /// mirror only when it is an exact copy of the current process tip. Any
    /// other shape — no identity, no mirror, a mirror at another tip, an
    /// unreadable mirror — returns `None` and the fail-closed check stands.
    pub(super) async fn refresh_keep_set_for_current_process_tip(
        &self,
        live_manifests: &[BackupManifest],
    ) -> SyncResult<Option<Vec<BackupManifest>>> {
        let current = match self.backup_published_tip_identity.lock().await.as_ref() {
            Some(identity) => identity.clone(),
            None => return Ok(None),
        };
        for manifest in live_manifests {
            if BackupTipIdentity::from_manifest(manifest)? == current {
                return Ok(None);
            }
        }
        let Some(path) = self.backup_manifest_cache_path() else {
            return Ok(None);
        };
        let mirrored = match Self::read_mirrored_backup_keep_set(&path) {
            Ok(Some(mirrored)) => mirrored,
            Ok(None) => return Ok(None),
            Err(error) => {
                tracing::warn!(
                    target: "fold_db::sync::backup",
                    error = %error,
                    counter = current.counter,
                    "mirrored keep set unreadable while the supplied keep set predates the process tip; keeping the supplied set"
                );
                return Ok(None);
            }
        };
        if BackupTipIdentity::from_manifest(&mirrored)? != current {
            return Ok(None);
        }
        let supplied_counters: Vec<u64> = live_manifests.iter().map(|m| m.counter).collect();
        tracing::info!(
            target: "fold_db::sync::backup",
            supplied_counters = ?supplied_counters,
            counter = current.counter,
            "supplied keep set predates the tip this process published; adopting the mirrored keep set"
        );
        Ok(Some(vec![mirrored]))
    }

    pub(super) fn require_exact_keep_set_match(
        identity: Option<&BackupTipIdentity>,
        live_manifests: &[BackupManifest],
    ) -> SyncResult<()> {
        let Some(current) = identity else {
            return Ok(());
        };
        let mut exact_matches = 0usize;
        for manifest in live_manifests {
            if BackupTipIdentity::from_manifest(manifest)? == *current {
                exact_matches += 1;
            }
        }
        if exact_matches != 1 {
            return Err(SyncError::Storage(format!(
                "backup orphan GC refused: supplied keep set contains {exact_matches} exact copies of current local tip counter {}",
                current.counter
            )));
        }
        Ok(())
    }
}
// lint:file-size-ok moved verbatim from backup_uploader.rs; cohesive unit, split further in a later pass
