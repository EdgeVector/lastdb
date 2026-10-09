use super::*;

impl SyncEngine {
    pub(super) fn record_backup_progress_cycle(
        &self,
        report: &SnapshotLogPublishCycleReport,
        cycle_duration: Duration,
    ) {
        let phase = progress_phase_for(&report.publish_phase);
        let sample = crate::backup_progress::BackupCycleSample {
            chunks_total: report.chunks.candidates as u64,
            chunks_present: report.chunks.chunks_present as u64,
            uploaded: report.chunks.uploaded as u64,
            already_present_walked: report.chunks.already_present as u64,
            bytes_uploaded: report.chunks.bytes_uploaded,
            failed: report.chunks.failed as u64,
            failure_error: report.chunks.first_error.clone(),
            quota_exceeded: report.chunks.quota_exceeded,
            source_missing: report.chunks.source_missing as u64,
            unbackable_manifest_chunks: self
                .backup_unbackable_manifest_chunks
                .load(Ordering::Relaxed),
            bytes_remaining: Some(report.chunks.bytes_remaining),
            cycle_duration,
            transfer_secs: report.chunks.transfer_secs,
            cas_counter: report.cas_counter,
            phase,
            target_generation: report.target_generation,
        };
        if let Ok(mut tracker) = self.backup_progress.lock() {
            tracker.observe(&sample);
        }
    }

    /// Stamp the moment a cut actually landed in cloud (successful CAS).
    pub(super) fn record_backup_publish_landed(&self) {
        // A successful CAS proves the home can outrun reseal for at least one
        // cut; clear the re-cut bound streak so a later isolated kill does not
        // inherit consecutive kills from before the land.
        self.backup_consecutive_reseal_kills
            .store(0, Ordering::Relaxed);
        // A CAS only lands when every manifest chunk verified present, so the
        // unbackable population of the cut that just landed was empty. Clear the
        // mirror rather than leaving a stale count to outlive its cut.
        self.backup_unbackable_manifest_chunks
            .store(0, Ordering::Relaxed);
        if let Ok(mut tracker) = self.backup_progress.lock() {
            tracker.observe_publish_landed();
        }
    }

    /// Promote source_missing digests that are already in cloud (or already in
    /// `backup_known_present`) so a held cut can still CAS without a local
    /// sealed file. Returns residual digests still absent from both local and
    /// cloud — those are truly unpublishable on this generation.
    ///
    /// Local-file absence only means "cannot re-upload this sha"; it does not
    /// mean the object store lacks it. Skipping this heal pinned residual sets
    /// (e.g. 215) as FAILING forever even when prior generations had uploaded
    /// the digests (2026-08-14 primary durability DEGRADED 6d+).
    ///
    /// `probe_error` is set when at least one presence check could not be
    /// completed (transport/auth failure). A flaky HEAD/presign call is not
    /// evidence the digest is absent from cloud, so callers must not treat a
    /// residual count produced under `probe_error` as a server-confirmed
    /// absence — retiring or punching a named hole on an unconfirmed answer
    /// can drop a digest that is actually still held in cloud.
    pub(super) async fn resolve_source_missing_via_cloud_presence(
        &self,
        generation: Option<u64>,
    ) -> CloudPresenceHeal {
        let missing_shas: Vec<String> = {
            let mut guard = self.backup_unresolvable.lock().await;
            let set = guard.for_generation(generation);
            set.iter().cloned().collect()
        };
        if missing_shas.is_empty() {
            return CloudPresenceHeal::default();
        }
        let mut still: HashSet<String> = HashSet::new();
        let mut healed = 0usize;
        let mut probe_error = false;
        for sha in missing_shas {
            let already_known = self.backup_known_present.lock().await.contains(&sha);
            if already_known {
                healed = healed.saturating_add(1);
                continue;
            }
            match self.auth.require_backup_chunk_present(&sha).await {
                Ok(true) => {
                    self.backup_known_present.lock().await.insert(sha);
                    healed = healed.saturating_add(1);
                }
                Ok(false) => {
                    still.insert(sha);
                }
                Err(err) => {
                    probe_error = true;
                    tracing::warn!(
                        target: "fold_db::sync::backup",
                        sha = %sha,
                        error = %err,
                        generation = ?generation,
                        "backup cloud-presence heal probe failed (transport/auth); \
                         leaving digest unresolved for retry, not a confirmed absence"
                    );
                    still.insert(sha);
                }
            }
        }
        if healed > 0 {
            self.persist_backup_presence_cache().await;
            tracing::info!(
                target: "fold_db::sync::backup",
                healed,
                residual = still.len(),
                generation = ?generation,
                "source_missing digests already present in cloud (or known-present cache); \
                 promoted so the held cut can still CAS"
            );
        }
        {
            let mut guard = self.backup_unresolvable.lock().await;
            let set = guard.for_generation(generation);
            *set = still.clone();
        }
        CloudPresenceHeal {
            remaining: still.len(),
            probe_error,
        }
    }

    /// Count held-cut candidates currently in `backup_known_present`.
    ///
    /// Used after cloud-presence heal so `chunks_present` / `known_missing_chunks`
    /// reflect digests promoted without a local re-upload.
    pub(super) async fn count_held_candidates_in_known_present(&self) -> Option<usize> {
        let candidates = {
            let guard = self.backup_publish_target.lock().await;
            guard.as_ref().map(|t| {
                t.candidates
                    .iter()
                    .map(|c| c.chunk.sha256.clone())
                    .collect::<Vec<_>>()
            })
        }?;
        let known = self.backup_known_present.lock().await;
        Some(
            candidates
                .iter()
                .filter(|sha| known.contains(sha.as_str()))
                .count(),
        )
    }

    /// Proven unpublishability (`source_missing > 0`): abandon the pinned cut
    /// for a replacement, unless consecutive reseal-killed cuts hit the bound.
    ///
    /// Returns `true` when the cut was abandoned (caller should re-cut against
    /// the current sealed set). Returns `false` when the bound fired — leave
    /// the dead cut in place and report failing so an operator sees the
    /// reseal-rate defect rather than an unbounded re-cut loop.
    pub(super) async fn handle_unpublishable_backup_cut(
        &self,
        source_missing: usize,
        generation: Option<u64>,
    ) -> bool {
        debug_assert!(
            source_missing > 0,
            "handle_unpublishable_backup_cut requires source_missing > 0"
        );
        let so_far = self.backup_consecutive_reseal_kills.load(Ordering::Relaxed) as u32;
        match decide_unpublishable_cut_action(so_far, MAX_CONSECUTIVE_RESEAL_KILLED_CUTS) {
            UnpublishableCutAction::AbandonAndRecut { consecutive_after } => {
                self.backup_consecutive_reseal_kills
                    .store(u64::from(consecutive_after), Ordering::Relaxed);
                tracing::warn!(
                    target: "fold_db::sync::backup",
                    source_missing,
                    generation = ?generation,
                    consecutive_reseal_kills = consecutive_after,
                    max = MAX_CONSECUTIVE_RESEAL_KILLED_CUTS,
                    "abandoning provably unpublishable backup cut for a fresh cut \
                     (source_missing > 0); replacement names the post-reseal sealed set"
                );
                self.retire_backup_publish_target().await;
                true
            }
            UnpublishableCutAction::StopRecutting { consecutive } => {
                self.backup_consecutive_reseal_kills
                    .store(u64::from(consecutive), Ordering::Relaxed);
                tracing::error!(
                    target: "fold_db::sync::backup",
                    source_missing,
                    generation = ?generation,
                    consecutive_reseal_kills = consecutive,
                    max = MAX_CONSECUTIVE_RESEAL_KILLED_CUTS,
                    "backup cut repeatedly dies to reseal; stopping automatic re-cut — \
                     reseal rate is the defect, not cut policy"
                );
                self.record_backup_progress_failure(&format!(
                    "backup cut abandoned {consecutive} times due to resealed-away chunks \
                     (source_missing={source_missing}); automatic re-cut stopped — \
                     reseal rate must fall before a cut can land"
                ));
                false
            }
        }
    }

    /// Record a publish cycle that failed before producing any sample.
    pub(super) fn record_backup_progress_failure(&self, error: &str) {
        if let Ok(mut tracker) = self.backup_progress.lock() {
            tracker.observe_failure(error);
        }
    }

    /// Snapshot sealed-chunk backup progress for `/api/status` / `lastdb status`.
    pub fn backup_progress_snapshot(&self) -> crate::backup_progress::BackupProgressSnapshot {
        let mut snap = match self.backup_progress.lock() {
            Ok(t) => t.snapshot(),
            Err(_) => crate::backup_progress::BackupProgressSnapshot {
                // Unknown state is not a complete backup. Only a home with no
                // uploader at all can honestly claim "nothing outstanding".
                enabled: self.laststore_backup_source.is_some(),
                complete: self.laststore_backup_source.is_none(),
                percent: None,
                chunks_total: 0,
                chunks_present: 0,
                chunks_remaining: 0,
                bytes_remaining: None,
                elapsed_secs: None,
                eta_secs: None,
                ewma_upload_bps: None,
                ewma_link_bps: None,
                ewma_overhead_secs: None,
                last_cycle_uploaded: 0,
                last_cycle_already_present: 0,
                last_cycle_bytes_uploaded: 0,
                last_cycle_failed: 0,
                chunks_source_missing: 0,
                chunks_unbackable_manifest: 0,
                cas_counter: None,
                phase: "unavailable".into(),
                show_progress: false,
                last_success_unix: None,
                last_success_age_secs: None,
                consecutive_failures: 0,
                last_error: None,
                chunks_gained: 0,
                chunks_erased: 0,
                net_progressing: false,
                recent_gained: 0,
                recent_erased: 0,
                net_progress_window_cycles: 0,
                cycles_since_net_gain: 0,
                target_generation: None,
                last_publish_unix: None,
                last_publish_age_secs: None,
            },
        };
        // Demoted continuous plane must not read as idle `never_completed` with
        // zero counters while a cut is mid-drain. Surface the held generation so
        // status shows work in flight (samples fill remaining once a cycle runs).
        if self.pin_log.continuous_sealed_home_backup_demoted() {
            if let Ok(guard) = self.backup_publish_target.try_lock() {
                if let Some(target) = guard.as_ref() {
                    if snap.target_generation.is_none() {
                        snap.target_generation = Some(target.generation);
                    }
                    if snap.phase == "never_completed" || snap.phase == "idle" {
                        snap.phase = "draining".into();
                        snap.show_progress = true;
                        if snap.chunks_total == 0 {
                            let total = target.total() as u64;
                            snap.chunks_total = total;
                            // Without a drain sample we cannot know present count;
                            // leave remaining as total so the bar is non-zero idle.
                            snap.chunks_remaining = total;
                        }
                    }
                }
            }
        }
        snap
    }
}
