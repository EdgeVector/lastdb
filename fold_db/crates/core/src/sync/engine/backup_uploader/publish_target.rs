use super::*;

impl SyncEngine {
    /// True when a cut is already in flight (so the caller must not cut another).
    pub(crate) async fn has_backup_publish_target(&self) -> bool {
        self.backup_publish_target.lock().await.is_some()
    }

    /// Serialize a physical rewrite against cutting or retiring a backup
    /// publish target. Holding this guard across compaction prevents the
    /// check-then-act race where a cut begins after an `is_some()` probe.
    pub(crate) async fn lock_backup_publish_target(
        &self,
    ) -> tokio::sync::MutexGuard<'_, Option<BackupPublishTarget>> {
        self.backup_publish_target.lock().await
    }

    /// Whether the continuous sealed-home uploader will keep draining a held cut.
    ///
    /// Used for honest incomplete-cut error copy: "will finish it" is only true
    /// when demotion does not strand the hold (or sealed-home is not demoted).
    pub(crate) async fn continuous_held_cut_drain_active(&self) -> bool {
        if self.pin_log.continuous_sealed_home_backup_demoted() {
            // Demoted exception: continuous only drains when a target is held.
            self.has_backup_publish_target().await
        } else {
            true
        }
    }

    /// Freeze+upload bound for `lastdb cloud snapshot`. Photograph packing
    /// lock stays held for this whole window (design: freeze-in-place).
    pub(super) fn operator_snapshot_drain_deadline() -> Duration {
        const DEFAULT_SECS: u64 = 14_400;
        env_flag::var_parsed::<u64>("LASTDB_OPERATOR_SNAPSHOT_DRAIN_SECS")
            .or_else(|| env_flag::var_parsed::<u64>("LASTDB_UDS_ADMIN_TIMEOUT_SECS"))
            .filter(|&n| n > 0)
            .map_or(Duration::from_secs(DEFAULT_SECS), Duration::from_secs)
    }

    /// Keep draining an operator snapshot only while the last cycle uploaded
    /// at least one chunk, the continuous hold will finish the cut, and the
    /// deadline has not elapsed.
    ///
    /// A 200ms×1500 sleep-wait (the first operator-drain loop) spun Mini's
    /// 20-minute job to timeout whenever `will_finish` was true and a cycle
    /// uploaded nothing — the common test / unreachable-auth case. Fail that
    /// cycle immediately; a real recut still loops because each cycle PUTs a
    /// handful. Progressing drains run until the freeze-window deadline
    /// (hours), not 300s.
    pub(super) fn operator_snapshot_keep_draining(
        will_finish: bool,
        uploaded_this_cycle: usize,
        elapsed: Duration,
        deadline: Duration,
    ) -> bool {
        will_finish && uploaded_this_cycle > 0 && elapsed < deadline
    }

    pub(super) fn incomplete_cut_operator_error(
        missing: usize,
        uploaded: usize,
        will_finish: bool,
    ) -> SyncError {
        if will_finish {
            SyncError::BackupSnapshotInProgress {
                missing_chunks: missing,
                uploaded_this_drain: uploaded,
            }
        } else {
            SyncError::Storage(format!(
                "backup snapshot incomplete ({missing} chunks not yet in cloud); \
                 continuous sealed-home backup is demoted under MutationLog — \
                 operator re-cut / bootstrap required (uploaded_this_drain={uploaded})"
            ))
        }
    }

    /// Cut a publish target if none is in flight. Idempotent.
    ///
    /// Freeze is a packing lock, not a clone: the lock is held before the
    /// sealed-file walk so compaction/reseal cannot rewrite candidates under
    /// the list, and upload reads those live paths in place. The walk is the
    /// expensive step (`walk_backup_chunks` over the whole store), which is
    /// why it must not run once per drain cycle: on the 2026-07-31 primary it
    /// dominated the ~33 s cycle time for 16.2 k chunks. Holding the result
    /// turns the drain into a countdown *and* takes the walk out of the hot
    /// loop.
    pub(crate) async fn ensure_backup_publish_target(
        &self,
        previous_manifest: Option<&BackupManifest>,
    ) -> SyncResult<()> {
        self.ensure_backup_publish_target_with_presence(
            previous_manifest,
            None,
            false,
            false,
            None,
            false,
        )
        .await
    }

    /// `prelisted_presence` lets a fenced cut do its cloud list before it
    /// stops new local mutations. The outer `Some` means the list was tried.
    // lint:fn-size-ok moved verbatim from backup_uploader.rs; splitting this function is separate work.
    pub(super) async fn ensure_backup_publish_target_with_presence(
        &self,
        previous_manifest: Option<&BackupManifest>,
        prelisted_presence: Option<Option<CloudChunkPresence>>,
        primary_resume_root: bool,
        accept_local_damage: bool,
        fresh_proof: Option<&FreshCloudProof>,
        file_pack_capability_prechecked: bool,
    ) -> SyncResult<()> {
        check_damage_mode(previous_manifest, primary_resume_root, accept_local_damage)?;
        if self.has_backup_publish_target().await {
            if primary_resume_root {
                return Err(SyncError::Storage(
                    "primary resume root cut found another held publish target".into(),
                ));
            }
            return Ok(());
        }
        let store = self.laststore_backup_source.as_ref().ok_or_else(|| {
            SyncError::Storage(
                "lastdb cloud snapshot requires a LastStore backup source".to_string(),
            )
        })?;
        if !file_pack_capability_prechecked {
            self.require_backup_file_pack_capability(previous_manifest, fresh_proof.is_some())
                .await?;
        }

        // When cutting against a previous manifest, try a complete object-store
        // listing so carried-forward atom refs that exist neither locally nor
        // in cloud can be retired with an authenticated receipt. Listing
        // failure → incomplete presence → historical carry-forward (safe).
        // Names already in the last finished cloud photograph count as present
        // (they do not need to be on disk). This listing does not walk live
        // sealed files, so it may run before the packing lock.
        let cloud_presence = match prelisted_presence {
            Some(presence) => presence,
            None if previous_manifest.is_some() => {
                self.list_backup_chunk_presence_for_reconcile().await
            }
            None => None,
        };
        if primary_resume_root
            && !cloud_presence.as_ref().is_some_and(|presence| {
                presence.listing_complete
                    && (previous_manifest.is_some()
                        || fresh_proof.is_some_and(|proof| proof.matches(presence)))
            })
        {
            return Err(SyncError::Storage(
                "primary resume root cut requires a complete cloud list and exact fresh proof"
                    .into(),
            ));
        }

        // Packing lock first: hold the slot before walking live sealed files so
        // unattended compact/reseal cannot rewrite them under the list. A
        // concurrent ensure that already filled the slot returns.
        let mut slot = self.backup_publish_target.lock().await;
        if slot.is_some() {
            if primary_resume_root {
                return Err(SyncError::Storage(
                    "primary resume root cut found another held publish target".into(),
                ));
            }
            return Ok(());
        }

        // This row becomes part of S0. Pin it before any file cut, after the
        // persistence barrier for the writer positions it covers. The held
        // target keeps the marker unchanged across upload retries.
        self.prepare_backup_restore_frontier().await?;

        let missing_atom_groups = match store.stamp_pending_committed_successor_history() {
            Ok(report) => {
                tracing::info!(
                    target: "fold_db::sync::backup",
                    retired = report.retired_shas.len(),
                    retired_bytes = report.retired_bytes,
                    scope_ok = report.scope_ok,
                    groups_without_local_file = report.groups_without_local_file,
                    unstamped_successor_history_refs = report.unstamped_successor_history_refs,
                    "stamped committed atom successor history before photograph cut"
                );
                if report.unstamped_successor_history_refs > 0 {
                    tracing::error!(
                        target: "fold_db::sync::backup",
                        unstamped_successor_history_refs = report.unstamped_successor_history_refs,
                        "atom_successor_history_stamp_miss"
                    );
                    return Err(SyncError::Storage(
                        "atom keep-set is not a copy of disk: successor-history stamp miss"
                            .to_string(),
                    ));
                }
                if report.groups_without_local_file > 0
                    && !self.backup_only_mode.load(Ordering::SeqCst)
                {
                    tracing::error!(
                        target: "fold_db::sync::backup",
                        groups_without_local_file = report.groups_without_local_file,
                        "atom_missing_successor_refs"
                    );
                    return Err(SyncError::Storage(
                        "atom keep-set is not a copy of disk: restore missing atom groups from cloud onto disk, then retry"
                            .to_string(),
                    ));
                }
                report.groups_without_local_file
            }
            Err(e) => {
                tracing::error!(
                    target: "fold_db::sync::backup",
                    error = %e,
                    "atom_successor_history_stamp_miss"
                );
                return Err(SyncError::Storage(format!(
                    "atom keep-set is not a copy of disk: successor-history stamp failed: {e}"
                )));
            }
        };

        let mut manifest = if self.backup_only_mode.load(Ordering::SeqCst) {
            store.cut_backup_manifest_with_cloud_presence_strict(
                previous_manifest,
                cloud_presence.as_ref(),
            )
        } else {
            store
                .cut_backup_manifest_with_cloud_presence(previous_manifest, cloud_presence.as_ref())
        }
        .map_err(|e| SyncError::Storage(format!("cut backup manifest failed: {e}")))?;
        if missing_atom_groups > 0 && !accept_local_damage {
            let proven = store
                .missing_committed_atom_groups_have_cloud_copies(
                    previous_manifest,
                    &manifest,
                    cloud_presence.as_ref(),
                    missing_atom_groups,
                )
                .map_err(|e| SyncError::Storage(format!("atom group cloud proof failed: {e}")))?;
            if !proven {
                return Err(SyncError::Storage(
                    "atom keep-set is not a copy of disk: missing atom group lacks exact prior manifest and cloud proof"
                        .to_string(),
                ));
            }
        } else if missing_atom_groups > 0 {
            tracing::warn!(
                target: "fold_db::sync::backup",
                missing_atom_groups,
                "owner accepted a fresh backup of local files with missing historical atom groups"
            );
        }
        let is_copy = if primary_resume_root && previous_manifest.is_none() {
            store.atom_photograph_is_disk_copy(&manifest)
        } else if self.backup_only_mode.load(Ordering::SeqCst) {
            store.atom_photograph_has_verified_cloud_copies(&manifest, cloud_presence.as_ref())
        } else {
            store.atom_photograph_is_disk_copy(&manifest)
        }
        .map_err(|e| SyncError::Storage(format!("atom keep-set copy check failed: {e}")))?;
        if !is_copy {
            tracing::error!(
                target: "fold_db::sync::backup",
                generation = manifest.counter,
                atom_chunks = manifest.atom_chunks.len(),
                "atom keep-set lacks a local file or a verified cloud copy; packing-lock cut refused"
            );
            return Err(SyncError::Storage(
                "atom keep-set lacks a local file or a verified cloud copy; packing-lock cut refused".to_string(),
            ));
        }
        if primary_resume_root {
            if let Some(previous) = previous_manifest {
                make_verified_primary_resume_root(previous, &mut manifest)?;
            } else {
                manifest.deletion_receipts.clear();
                validate_manifest_chain(None, &manifest).map_err(|error| {
                    SyncError::Storage(format!("fresh local root is invalid: {error}"))
                })?;
            }
        } else {
            validate_manifest_chain(previous_manifest, &manifest).map_err(|error| {
                SyncError::Storage(format!("validate backup manifest failed: {error}"))
            })?;
        }
        if !manifest.deletion_receipts.is_empty() {
            let retired: usize = manifest
                .deletion_receipts
                .iter()
                .map(|r| r.retired_atom_chunk_shas.len())
                .sum();
            tracing::info!(
                target: "fold_db::sync::backup",
                generation = manifest.counter,
                retired,
                receipts = manifest.deletion_receipts.len(),
                "retired unbackable carried-forward atom chunk refs at cut"
            );
        }

        // The held target is the denominator for progress and the source for
        // uploads. It must therefore be the same chunk universe CAS validates
        // through the manifest, not only the delta since `previous_manifest`.
        // Previous atom refs are copied into every later manifest; excluding
        // them here let a target report 100% present while CAS still rejected
        // the same cut for missing manifest chunks.
        let candidates = store
            .enumerate_backup_publish_target_candidates()
            .map_err(|e| SyncError::Storage(format!("enumerate backup chunks failed: {e}")))?;
        let candidates = bind_backup_candidates_to_manifest(&manifest, candidates);
        let candidates = self.prepare_backup_file_packs(
            store,
            &mut manifest,
            candidates,
            previous_manifest,
            cloud_presence.as_ref(),
            fresh_proof,
            primary_resume_root,
        )?;
        {
            let candidate_shas: std::collections::BTreeSet<_> =
                candidates.iter().map(|c| c.chunk.sha256.clone()).collect();
            let unbackable = unbackable_manifest_chunk_count(&manifest, &candidate_shas);
            if unbackable > 0 {
                tracing::warn!(
                    target: "fold_db::sync::backup",
                    generation = manifest.counter,
                    unbackable,
                    "manifest still names chunks outside the local candidate set (kept because still in cloud, or reconcile incomplete)"
                );
            }
        }

        let generation = manifest.counter;
        let total = candidates.len();
        // Classify the manifest/candidate gap NOW. Discovering it at CAS costs a
        // full drain first, and the drain reports "100% present" while it does,
        // so the shortfall reads as a transient network condition rather than a
        // structural one.
        let unbackable_manifest_chunks =
            unbackable_manifest_chunk_count(&manifest, &candidate_sha_set(&candidates));
        if unbackable_manifest_chunks > 0 {
            tracing::warn!(
                target: "fold_db::sync::backup",
                generation,
                chunks = total,
                unbackable_manifest_chunks,
                "cut names manifest chunks with no local candidate; if any of them \
                 is absent from cloud this cut cannot publish and must be re-cut"
            );
        }
        // Mirror onto the engine so the count survives this cut's abandonment:
        // the CAS path retires the target BEFORE the cycle ends, and the sample
        // is recorded after, so reading the target there reports 0 on precisely
        // the cycle that had something to say.
        self.backup_unbackable_manifest_chunks
            .store(unbackable_manifest_chunks as u64, Ordering::Relaxed);
        *slot = Some(BackupPublishTarget::new(
            manifest,
            candidates,
            unbackable_manifest_chunks,
        ));
        tracing::info!(
            target: "fold_db::sync::backup",
            generation,
            chunks = total,
            unbackable_manifest_chunks,
            "publish target established; packing lock held until it lands"
        );
        Ok(())
    }

    /// Drain the in-flight target's live sealed-file candidates under the put
    /// budget.
    ///
    /// Returns the stats plus the target generation. The candidate list is
    /// fixed for the target's lifetime (packing lock: sealed files stay put),
    /// so `chunks_present` cannot fall between cycles.
    ///
    /// When `allow_puts` is false, still walks the candidate list to refresh
    /// `chunks_present` / `bytes_remaining` (status honesty under drain-PUT
    /// backoff) but does not start any presign/PUT work.
    pub(crate) async fn drain_backup_publish_target(
        &self,
        allow_puts: bool,
    ) -> SyncResult<(LastStoreBackupUploadStats, Option<u64>)> {
        self.ensure_backup_presence_cache_loaded().await;
        let (candidates, generation) = {
            let target = self.backup_publish_target.lock().await;
            match target.as_ref() {
                Some(t) => (t.candidates.clone(), Some(t.generation)),
                None => (Vec::new(), None),
            }
        };
        if candidates.is_empty() {
            return Ok((LastStoreBackupUploadStats::default(), generation));
        }
        let _caps = self.refresh_upload_policy().await;
        self.seed_backup_presence_if_cold(&candidates).await;
        if allow_puts {
            self.reseed_backup_presence_on_shortfall(&candidates).await;
        }
        let stats = self
            .drain_backup_candidates(candidates, allow_puts, generation)
            .await?;
        Ok((stats, generation))
    }

    /// Abandon the in-flight target so the next cycle cuts a fresh one. Used
    /// after a successful CAS and on epoch rebind. Releases the packing lock.
    pub(crate) async fn retire_backup_publish_target(&self) {
        let _retired = self.backup_publish_target.lock().await.take();
        self.sweep_backup_freeze_dirs(None);
    }

    /// Persist that this home's sealed base was abandoned as unpublishable.
    ///
    /// Best-effort by design: the abandon itself already happened, and failing
    /// to write the marker must not turn a recovery step into a cycle error.
    /// A marker write that fails costs restart-visibility, not durability.
    pub(crate) fn record_sealed_base_abandoned_marker(&self) {
        let Some(store) = self.laststore_backup_source.as_ref() else {
            return;
        };
        if let Err(err) = store.record_sealed_base_abandoned() {
            tracing::warn!(
                target: "fold_db::sync::backup",
                error = %err,
                "could not persist the sealed-base abandon marker; status will report this home \
                 as backed up again after a restart"
            );
        }
    }

    /// Remove cut dirs after a target retires or before a new target starts.
    ///
    /// The v2 pack writer stores pack objects under the generation directory.
    /// Older binaries also stored sealed-file clones there.
    ///
    /// `None` means no held target owns a cut dir. `Some(keep)` preserves one
    /// generation while the caller removes older dirs.
    ///
    /// This deletes recursively, so it is deliberately narrow: only immediate
    /// children of `<sidecar>/backup-cut-freeze/` whose name parses as a
    /// manifest counter are ever removed. Anything else under that directory —
    /// now or after a future layout change — is left alone rather than trusted
    /// to be ours.
    pub(crate) fn sweep_backup_freeze_dirs(&self, keep_generation: Option<u64>) {
        let Some(store) = self.laststore_backup_source.as_ref() else {
            return;
        };
        // Any generation resolves the same parent; 0 is a probe for the path.
        let Some(probe) = store.backup_cut_freeze_dir(keep_generation.unwrap_or(0)) else {
            return;
        };
        let keep = keep_generation.and(Some(probe.clone()));
        let Some(parent) = probe.parent() else {
            return;
        };
        let Ok(entries) = std::fs::read_dir(parent) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if Some(&path) == keep.as_ref() || !path.is_dir() {
                continue;
            }
            // Name must be a manifest counter, or it is not a freeze dir.
            let is_freeze_dir = path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.parse::<u64>().is_ok());
            if !is_freeze_dir {
                continue;
            }
            if let Err(e) = std::fs::remove_dir_all(&path) {
                tracing::debug!(
                    target: "fold_db::sync::backup",
                    error = %e,
                    stale_freeze_dir = %path.display(),
                    "could not reclaim stale cut-freeze dir"
                );
            }
        }
    }

    /// Verify + CAS + commit the in-flight target, then retire it.
    pub(crate) async fn cas_backup_publish_target(
        &self,
        previous_manifest: Option<&BackupManifest>,
    ) -> SyncResult<(BackupManifest, LastStoreCloudSnapshotReport)> {
        let out = self
            .cas_backup_publish_target_inner(true, true, previous_manifest, None)
            .await;
        if let Ok((manifest, _)) = &out {
            self.mirror_backup_keep_set(manifest);
        }
        out
    }

    /// Mirror the just-committed manifest to the node's keep-set path.
    ///
    /// The keep set is what `backup-gc` diffs the cloud listing against; with
    /// no keep set it refuses, which is correct (an unknown live set must never
    /// be treated as an empty one) but leaves the orphan sweep unrunnable. The
    /// operator snapshot route used to be the only writer, so the file existed
    /// only if a caller happened to run that verb in the window after a cut
    /// landed. On a home where a cut takes hours and the continuous publisher
    /// commits them unattended, that window is simply never hit.
    ///
    /// Best-effort on purpose: the manifest is already durable in cloud at this
    /// point, so a local mirror failure must warn, not fail the publish. Written
    /// through a temp file + rename so a reader never sees a half-written keep
    /// set and drops live chunks from it.
    pub(crate) fn mirror_backup_keep_set(&self, manifest: &BackupManifest) {
        let Some(path) = self.backup_manifest_cache_path() else {
            return;
        };
        match Self::write_backup_keep_set_file(&path, manifest) {
            Ok(()) => tracing::info!(
                target: "fold_db::sync::snapshot_log",
                path = %path.display(),
                counter = manifest.counter,
                chunks = manifest.mutable_chunks.len() + manifest.atom_chunks.len(),
                "backup keep set mirrored after publish"
            ),
            Err(e) => tracing::warn!(
                target: "fold_db::sync::snapshot_log",
                path = %path.display(),
                error = %e,
                "could not mirror backup keep set after publish; backup-gc will refuse until it exists"
            ),
        }
    }

    /// Serialize + atomically place the keep set. Split out so a test can drive
    /// it without a cloud round trip.
    pub(crate) fn write_backup_keep_set_file(
        path: &std::path::Path,
        manifest: &BackupManifest,
    ) -> std::io::Result<()> {
        let encoded = serde_json::to_vec_pretty(manifest)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, encoded)?;
        std::fs::rename(&tmp, path)
    }
}
// lint:file-size-ok moved verbatim from backup_uploader.rs; cohesive unit, split further in a later pass
