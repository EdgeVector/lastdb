use super::*;

impl SyncEngine {
    /// Product path: list cloud backup chunks, keep digests referenced by the
    /// current (and optional retained) local manifests **and** any in-flight
    /// publish target, and delete the rest via presigned DELETE. Live-manifest
    /// and in-flight cut chunks are never selected.
    ///
    /// Pass `dry_run: true` to only compute orphans without deleting.
    ///
    /// Every execute sweep verifies the published keep-set and held-target
    /// state after each DELETE presign. The post-CAS auto path also supplies
    /// its queue generation so work that is already stale can stop before
    /// listing.
    ///
    /// After restart the in-process CAS identity is `None` even when durable
    /// tip counter N and a published body exist. Admin dry-run and execute
    /// then use [`BackupGcKeepProof::VerifiedPublishedBody`] so a keep-set
    /// can be resolved only when that body matches durable counter and live
    /// `backup/latest`. Empty published keep still fail-closes. Cloud Sync Off
    /// still refuses: Off can retire a held cut that cloud `latest` already
    /// names. Post-CAS auto GC keeps exact process identity.
    ///
    /// This is an in-process fence only. It cannot revoke a DELETE accepted
    /// after a timeout, cancellation, or process loss, and it does not exclude
    /// another process or device. Full protection needs a durable server-side
    /// delete intent coordinated with `backup_latest_cas`.
    pub async fn gc_orphan_backup_chunks(
        &self,
        live_manifests: &[BackupManifest],
        dry_run: bool,
    ) -> SyncResult<BackupOrphanGcReport> {
        let (job, _) = self.backup_gc_jobs.accept("manual", dry_run, None)?;
        let result = self
            .gc_orphan_backup_chunks_for_job(live_manifests, dry_run, &job.job_id)
            .await
            .map(Some);
        self.backup_gc_jobs.finish(&job.job_id, &result)?;
        result?.ok_or_else(|| SyncError::Storage("GC returned no report".into()))
    }

    pub(crate) async fn gc_orphan_backup_chunks_for_job(
        &self,
        live_manifests: &[BackupManifest],
        dry_run: bool,
        job_id: &str,
    ) -> SyncResult<BackupOrphanGcReport> {
        if live_manifests.is_empty() {
            return Err(SyncError::Storage(
                "backup orphan GC refused: empty published keep set".into(),
            ));
        }
        if self
            .backup_gc_identity_revoked
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(SyncError::Storage(
                "backup orphan GC refused: no exact process identity; cloud plane Off \
                 revoked GC proof until this process CASes after On"
                    .into(),
            ));
        }
        let has_identity = self.backup_published_tip_identity.lock().await.is_some();
        let keep_proof = if has_identity {
            BackupGcKeepProof::ExactProcessIdentity
        } else {
            let (durable_counter, last_commit_unix_secs, _) = self.read_backup_gc_durable_tip()?;
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
            BackupGcKeepProof::VerifiedPublishedBody
        };
        match self
            .execute_backup_gc_job(live_manifests, dry_run, None, keep_proof, job_id)
            .await?
        {
            Some(report) => Ok(report),
            // Unreachable without a generation guard; keep the type honest.
            None => Err(SyncError::Storage(
                "backup orphan GC aborted without a generation guard".into(),
            )),
        }
    }

    /// Reclaim unreachable cloud chunks when PUTs hit the typed quota gate.
    ///
    /// This deliberately runs before any successful CAS. Keep-set =
    /// **published tip(s)** (local `last_committed_manifest`, else cloud
    /// `backup/latest` body) **plus** the current held cut (via
    /// `gc_orphan_backup_chunks_with_generation_guard`). Passing an empty
    /// published slice while tip N is live would DELETE N-only chunks while
    /// `latest` still points at N — the empty-published-keep defect.
    ///
    /// Fail closed when a published tip is known but cannot be folded into the
    /// keep-set (refuse DELETE rather than GC with only the held cut). Errors
    /// stay on the cloud plane and never block local reads/writes; the normal
    /// drain retries after its existing backoff.
    pub(super) async fn maybe_run_quota_recovery_orphan_gc(
        &self,
        stats: &LastStoreBackupUploadStats,
        local_published: Option<&BackupManifest>,
    ) -> bool {
        if !stats.quota_exceeded {
            return false;
        }
        let job = match self.backup_gc_jobs.accept("quota_recovery", false, None) {
            Ok((job, _)) => job,
            Err(error) => {
                tracing::warn!(target: "fold_db::sync::backup", error = %error, "quota GC could not persist job; no DELETE");
                return true;
            }
        };
        let live_manifests = match self
            .resolve_published_manifests_for_quota_gc(local_published)
            .await
        {
            Ok(m) => m,
            Err(error) => {
                let _ = self
                    .backup_gc_jobs
                    .finish(&job.job_id, &Err(SyncError::Storage(error.to_string())));
                tracing::warn!(
                    target: "fold_db::sync::backup",
                    error = %redact_sync_error_text(&error.to_string()),
                    "quota-exceeded orphan reclaim refused: cannot resolve published tip keep-set"
                );
                return true;
            }
        };
        let result = self
            .execute_backup_gc_job(
                &live_manifests,
                false,
                None,
                BackupGcKeepProof::VerifiedPublishedBody,
                &job.job_id,
            )
            .await;
        if let Err(error) = self.backup_gc_jobs.finish(&job.job_id, &result) {
            tracing::error!(target: "fold_db::sync::backup", error = %error, "quota GC receipt failed");
        }
        match result {
            Ok(Some(report)) => tracing::info!(
                target: "fold_db::sync::backup",
                deleted = report.deleted,
                failed = report.failed,
                superseded = report.superseded,
                orphans_selected = report.orphans_selected,
                live_referenced = report.live_referenced,
                published_tips = live_manifests.len(),
                "quota-exceeded backup PUT triggered automatic orphan reclaim"
            ),
            Ok(None) => tracing::info!(
                target: "fold_db::sync::backup",
                published_tips = live_manifests.len(),
                "quota-exceeded orphan reclaim aborted; newer keep-set supersedes"
            ),
            Err(error) => tracing::warn!(
                target: "fold_db::sync::backup",
                error = %redact_sync_error_text(&error.to_string()),
                "quota-exceeded automatic orphan reclaim failed; drain will retry later"
            ),
        }
        true
    }

    /// Resolve published tip(s) for quota-recovery orphan GC keep-set.
    ///
    /// Prefer the continuous uploader's in-memory last committed tip. When the
    /// process has not yet landed a tip this lifetime (or restarted mid-drain),
    /// download the cloud `backup/latest` tip body so the keep-set still
    /// protects the currently published restore target.
    ///
    /// Fail closed when a published tip is known to exist (cloud latest pointer
    /// with non-empty sha, or local high-water counter > 0) but its body cannot
    /// be folded into the keep-set. First-ever backup (no local commit, no
    /// cloud latest) may return an empty vec — in-flight alone is then the keep.
    pub(super) async fn resolve_published_manifests_for_quota_gc(
        &self,
        local_published: Option<&BackupManifest>,
    ) -> SyncResult<Vec<BackupManifest>> {
        if let Some(m) = local_published {
            return Ok(vec![m.clone()]);
        }
        match self.auth.backup_latest_get().await {
            Ok(get) => {
                let sha = get.latest.manifest_sha256.trim();
                if sha.is_empty() {
                    return Ok(Vec::new());
                }
                match self
                    .download_cloud_tip_manifest_body(
                        &get.latest.store_uuid,
                        get.latest.epoch,
                        get.latest.counter,
                        &get.latest.manifest_sha256,
                    )
                    .await
                {
                    Ok(m) => Ok(vec![m]),
                    Err(err) => Err(SyncError::Storage(format!(
                        "quota orphan GC refused: cloud latest counter={} tip body unloadable \
                         (empty published keep would exclude live tip): {err}",
                        get.latest.counter
                    ))),
                }
            }
            Err(err) => {
                // Auth/network blip: only refuse when local durability proves a
                // tip was previously committed (counter > 0). True first backup
                // (counter 0 / no high-water) may proceed with in-flight keep.
                if self.local_backup_manifest_counter().unwrap_or(0) > 0 {
                    return Err(SyncError::Storage(format!(
                        "quota orphan GC refused: local backup counter>0 but cloud latest \
                         unreachable (cannot risk empty published keep): {err}"
                    )));
                }
                Ok(Vec::new())
            }
        }
    }

    /// Highest committed backup manifest counter on the local LastStore, if known.
    pub(super) fn local_backup_manifest_counter(&self) -> Option<u64> {
        let store = self.laststore_backup_source.as_ref()?;
        store
            .backup_durability()
            .ok()
            .flatten()
            .map(|d| d.backup_manifest_counter)
    }

    /// Download one tip body for a known `backup/latest` pointer.
    pub(super) async fn download_cloud_tip_manifest_body(
        &self,
        store_uuid: &str,
        epoch: u64,
        counter: u64,
        manifest_sha256: &str,
    ) -> SyncResult<BackupManifest> {
        let expected = manifest_sha256.trim().to_string();
        if expected.is_empty() {
            return Err(SyncError::Storage(
                "backup latest pointer has empty manifest_sha256".into(),
            ));
        }
        let presigned = self
            .auth
            .presign_backup_manifest_download(&expected)
            .await?;
        let bytes = self
            .s3
            .download_limited(&presigned, Some(16 * 1024 * 1024))
            .await?
            .ok_or_else(|| {
                SyncError::Storage(format!("backup published tip manifest {expected} missing"))
            })?;
        let actual = {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(&bytes);
            format!("{:x}", hasher.finalize())
        };
        if !actual.eq_ignore_ascii_case(&expected) {
            return Err(SyncError::Crypto(format!(
                "backup published tip sha256 mismatch: expected {expected}, got {actual}"
            )));
        }
        let manifest: BackupManifest = serde_json::from_slice(&bytes).map_err(|e| {
            SyncError::Storage(format!("decode published tip backup manifest: {e}"))
        })?;
        let canonical = manifest_sha256_hex(&manifest)
            .map_err(|e| SyncError::Storage(format!("hash published tip manifest: {e}")))?;
        if !canonical.eq_ignore_ascii_case(&expected) {
            return Err(SyncError::Crypto(format!(
                "backup published tip canonical hash mismatch: expected {expected}, got {canonical}"
            )));
        }
        if store_uuid != manifest.store_uuid
            || epoch != manifest.epoch
            || counter != manifest.counter
        {
            return Err(SyncError::Storage(
                "backup latest pointer does not match published tip manifest body".into(),
            ));
        }
        Ok(manifest)
    }

    /// Remember the just-landed tip for post-CAS auto orphan GC.
    ///
    /// Default retention is **latest tip only**: a newer CAS replaces any
    /// still-pending tip so GC always keeps the most recent cut. Each enqueue
    /// also bumps [`Self::post_cas_backup_gc_generation`] so an in-flight GC
    /// that already took an older tip can abort before DELETEs.
    pub(super) async fn enqueue_post_cas_backup_orphan_gc(&self, latest_tip: BackupManifest) {
        let cas_counter = latest_tip.counter;
        let cut_csn = latest_tip.cut_csn;
        // Generation first, then the tip slot, so a concurrent take that reads
        // generation under the same tip lock cannot observe a new gen with an
        // old tip still sitting in the slot (or vice versa without pairing).
        let generation = self
            .post_cas_backup_gc_generation
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            .saturating_add(1);
        let mut slot = self.post_cas_backup_gc_tip.lock().await;
        if let Some(previous) = slot.take() {
            if let Err(error) = self.backup_gc_jobs.finish(&previous.job_id, &Ok(None)) {
                tracing::error!(target: "fold_db::sync::backup", error = %error, "superseded queued GC receipt failed");
            }
        }
        match self.backup_gc_jobs.accept("post_publication", false, None) {
            Ok((job, _)) => {
                *slot = Some(PendingBackupGc {
                    manifest: latest_tip,
                    generation,
                    job_id: job.job_id,
                });
            }
            Err(error) => {
                tracing::error!(target: "fold_db::sync::backup", error = %error, "post-publication GC admission failed; no DELETE");
            }
        }
        tracing::debug!(
            target: "fold_db::sync::backup",
            cas_counter,
            cut_csn,
            generation,
            "queued post-CAS backup orphan GC (latest tip only)"
        );
    }

    /// True when a newer tip has been enqueued after `my_generation`.
    pub(super) fn post_cas_backup_gc_superseded(&self, my_generation: u64) -> bool {
        self.post_cas_backup_gc_generation
            .load(std::sync::atomic::Ordering::SeqCst)
            > my_generation
    }

    /// Fire-and-forget post-CAS orphan GC on the current Tokio runtime.
    ///
    /// Safe to call from the continuous uploader loop after a successful CAS.
    /// Local Mini R/W never waits on this task.
    pub(crate) fn schedule_post_cas_backup_orphan_gc(self: &Arc<Self>) {
        let engine = Arc::clone(self);
        // lint:spawn-bare-ok process-lifetime post-CAS backup orphan GC — not
        // request-scoped; must not block the uploader drain cycle or local R/W.
        tokio::spawn(async move {
            engine.run_pending_post_cas_backup_orphan_gc().await;
        });
    }

    /// Drain the queued latest-tip keep-set and run orphan GC (execute, not dry-run).
    ///
    /// No-op when nothing is queued. Failures are logged and never surface as
    /// local-engine errors (preference: cloud plane never blocks local R/W).
    ///
    /// **Generation supersede:** if a newer CAS enqueues tip N+1 after this
    /// call has already taken tip N, GC aborts before any DELETE so chunks
    /// exclusive to N+1 cannot be reaped under keep-set N.
    pub async fn run_pending_post_cas_backup_orphan_gc(&self) {
        let taken = self.post_cas_backup_gc_tip.lock().await.take();
        let Some(pending) = taken else {
            return;
        };
        self.run_post_cas_backup_orphan_gc_for_tip(
            pending.manifest,
            pending.generation,
            &pending.job_id,
        )
        .await;
    }

    /// Run orphan GC for a tip already taken from the queue, aborting if a
    /// newer generation has been enqueued in the meantime.
    pub(super) async fn run_post_cas_backup_orphan_gc_for_tip(
        &self,
        manifest: BackupManifest,
        my_generation: u64,
        job_id: &str,
    ) {
        let cas_counter = manifest.counter;
        let cut_csn = manifest.cut_csn;
        if self.post_cas_backup_gc_superseded(my_generation) {
            let _ = self.backup_gc_jobs.finish(job_id, &Ok(None));
            tracing::info!(
                target: "fold_db::sync::backup",
                cas_counter,
                cut_csn,
                my_generation,
                current_generation = self
                    .post_cas_backup_gc_generation
                    .load(std::sync::atomic::Ordering::SeqCst),
                "post-CAS backup orphan GC superseded by newer tip; skipping deletes"
            );
            return;
        }
        let result = self
            .execute_backup_gc_job(
                std::slice::from_ref(&manifest),
                false,
                Some(my_generation),
                BackupGcKeepProof::ExactProcessIdentity,
                job_id,
            )
            .await;
        if let Err(error) = self.backup_gc_jobs.finish(job_id, &result) {
            tracing::error!(target: "fold_db::sync::backup", error = %error, "post-publication GC receipt failed");
        }
        match result {
            Ok(None) => {
                tracing::info!(
                    target: "fold_db::sync::backup",
                    cas_counter,
                    cut_csn,
                    my_generation,
                    current_generation = self
                        .post_cas_backup_gc_generation
                        .load(std::sync::atomic::Ordering::SeqCst),
                    "post-CAS backup orphan GC aborted mid-run; newer tip keep-set supersedes"
                );
            }
            Ok(Some(report)) => {
                tracing::info!(
                    target: "fold_db::sync::backup",
                    orphans_selected = report.orphans_selected,
                    deleted = report.deleted,
                    failed = report.failed,
                    superseded = report.superseded,
                    cloud_chunks_listed = report.cloud_chunks_listed,
                    live_referenced = report.live_referenced,
                    cas_counter,
                    cut_csn,
                    my_generation,
                    "post-CAS backup orphan GC finished"
                );
            }
            Err(e) => {
                tracing::warn!(
                    target: "fold_db::sync::backup",
                    error = %redact_sync_error_text(&e.to_string()),
                    cas_counter,
                    cut_csn,
                    my_generation,
                    "post-CAS backup orphan GC failed; local engine continues"
                );
            }
        }
    }
}
// lint:file-size-ok moved verbatim from backup_uploader.rs; cohesive unit, split further in a later pass
