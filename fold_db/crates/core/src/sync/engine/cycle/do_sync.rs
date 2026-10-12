//! One sync cycle: `do_sync` orchestrates the phase methods in the child modules.

use super::*;

mod compact;
mod download;
mod mutation_log;
mod upload;

/// Mutable state threaded through the phases of one sync cycle.
#[derive(Default)]
struct CycleState {
    uploaded: usize,
    downloaded: u64,
    first_transfer_error: Option<SyncError>,
    mutation_log_backlog_ns: u64,
    mutation_log_upload_held: bool,
    /// Prefixes for which this cycle positively proved the key opens the
    /// existing cloud data.
    proven_prefixes: std::collections::HashSet<String>,
}

impl SyncEngine {
    pub(crate) async fn do_sync(&self) -> SyncResult<bool> {
        // A latched bootstrap block means the pre-upload proof cannot be
        // satisfied by anything this cycle does, so every byte of upload
        // preparation is waste. Suppress the UPLOAD side — capture tick, outbox
        // scheduling, mutation-log segment upload, and batch selection — before
        // any of it runs. The proof gate sits *after* entry selection and
        // partitioning, so without this the engine rebuilds the full upload set
        // every cycle only to have the gate refuse it. That restaging is what
        // walked RSS 6.4 -> 10.6 GiB in ~10 minutes on 2026-08-09, for uploads
        // that could never happen.
        //
        // Downloads deliberately still run. The block is recorded against ONE
        // target; stopping the whole cycle would also stop this node receiving
        // share/org data on targets that are perfectly healthy, which is a
        // bigger regression than the waste being removed.
        //
        // The block is still surfaced as the cycle's error (via
        // `first_transfer_error` below, after downloads) so the degraded/streak
        // machinery keeps firing — suppressing the work must not launder the
        // failure into a healthy cycle.
        //
        // The latch clears on a daemon restart, on `cloud on`, and the moment
        // the prefix proves — a brake, never a permanent stop.
        let backup_blocked = self.backup_blocker.lock().await.clone();
        if let Some(ref blocker) = backup_blocked {
            log_backup_blocked(blocker);
        }

        // Per-phase wall time for this pass, logged once when the pass ends
        // (any exit path). A 7-minute hole between the mutation-log upload and
        // the download phase on the primary (2026-10-05) left no log line to
        // name the slow step; this line names it.
        let mut phases = SyncPhaseTimer::start();
        let (targets, partitioner) = self.target_config_snapshot().await;
        // The permanent marker precedes upload preparation and any peer replay.
        // A held account keeps the existing outbox and capture queues intact.
        let targets = self.preflight_cloud_peer_targets(&targets).await?;
        phases.mark("cloud_generation");
        let mut state = CycleState::default();

        tracing::info!(
            target: "fold_db::sync::memory",
            target_count = targets.len(),
            target_labels = %targets
                .iter()
                .map(|t| t.label.as_str())
                .collect::<Vec<_>>()
                .join(","),
            "do_sync starting"
        );

        let upload_caps = self.run_pre_upload_maintenance(&mut phases).await;

        self.stage_and_upload_pending(backup_blocked.is_some(), &mut state, &mut phases)
            .await?;
        phases.mark("mutation_log_upload");

        // Peer writer-scoped mutation-log apply. Independent of S0 and of the
        // classic `log/{seq}.enc` download: those never list `log/{writer}/`
        // keys. Vector F from `published_f_by_writer` (not scalar 0) so a peer
        // whose through_id is <= this node's published scalar is still fetched.
        // Local writer streams are skipped inside the helper (self-echo).
        // Runs even when backup bootstrap is blocked: downloads still run.
        //
        // While this node still has unpublished local log, listing/downloading
        // peers can take longer than the upload batch (primary, 2026-10-05:
        // peer_apply ~170 s, upload ~80 s). Skip those cycles except once per
        // `mutation_log_peer_apply_min_interval_ms` so drain is not gated on
        // peer apply. Frontier logic is unchanged when the cycle does run.
        if matches!(self.config.capture_mode, CaptureMode::MutationLog) {
            self.apply_peer_mutation_logs(&mut state).await;
        }
        phases.mark("peer_apply");

        let scoped_idxs = self
            .download_upload_download_phases(
                backup_blocked.is_some(),
                &upload_caps,
                &targets,
                &partitioner,
                &mut state,
                &mut phases,
            )
            .await?;
        let scoped_total = scoped_idxs.len();

        self.compact_scoped_targets(&targets, &scoped_idxs, scoped_total, &state.proven_prefixes)
            .await;
        phases.mark("scoped_compact");

        self.compact_personal_log_with_backoff(&state.proven_prefixes)
            .await;
        phases.mark("personal_compact");

        // Outbox overflow valve: after this cycle's upload drain attempt,
        // escalate oversized / stale staging via snapshot+clear (never clears
        // if the snapshot fails). Run this even when a post-upload transfer
        // failed: a large orphaned backlog can otherwise keep retrying the same
        // poisoned upload head and never reach the snapshot heal path.
        //
        // Important: reducing outbox depth must NOT launder an unrelated
        // transfer failure into a healthy cycle. `overflow_healed` only means
        // staging shrank (possibly via a time-based snapshot for a different
        // reason). Returning Ok here used to flow into `record_cycle_outcome(true)`
        // → `record_sync_success()`, which clears `last_error`, the failure
        // streak, `replay_blocker`, and `backlog_alerts` — masking a real,
        // ongoing failure on another target. Always surface `state.first_transfer_error`
        // so the degraded/backlog machinery keeps firing until the transfer
        // itself recovers. The heal still ran above; the next clean cycle can
        // retire the streak honestly.
        let (overflow_before, overflow_after, overflow_healed) =
            self.run_outbox_overflow_valve().await;
        phases.mark("outbox_overflow");

        finish_cycle(
            state,
            backup_blocked.as_ref(),
            overflow_before,
            overflow_after,
            overflow_healed,
        )
    }

    /// Select the upload batch, then run personal download, legacy upload and
    /// scoped downloads in key-safe order. Returns the scoped target indices
    /// so the caller can compact them afterwards.
    async fn download_upload_download_phases(
        &self,
        backup_blocked: bool,
        upload_caps: &UploadPolicySnapshot,
        targets: &[SyncTarget],
        partitioner: &Option<SyncPartitioner>,
        state: &mut CycleState,
        phases: &mut SyncPhaseTimer,
    ) -> SyncResult<Vec<usize>> {
        // Upload pending entries, partitioned across targets. Cap what this
        // cycle will seal/PUT so a multi-thousand outbox cannot monopolise RAM
        // (re-enable thrash 2026-07-14: download=0, pending≈3757, swap 3→26GB).
        //
        // Size is measured once here and reused for stats/logs — never call
        // `serialized_len()` again on the selected batch (full JSON re-encode
        // of a multi-MB BatchPut was enough to stall the cycle under swap).
        let (entries, bytes_selected) = self
            .select_and_record_upload_batch(backup_blocked, upload_caps)
            .await;

        // Download before upload for key safety — but **personal first**, then at
        // most one scoped (org/share) target per cycle.
        //
        // F1: default sync/bootstrap/device-join transfer only log and snapshot
        // objects. Content-addressed file blobs (`cas/sha256/*`) are opened by
        // explicit file-blob APIs, never by ordinary replay of DB pointers.
        //
        // Re-enable thrash 2026-07-14 (root cause, after upload caps):
        // `target_count=22` (personal + ~20 distinct org_hash rows all labeled
        // `org:edgevector` + upgrade-probe). do_sync downloaded every target
        // before any upload; the first org:edgevector download alone pushed
        // RSS 4→10 GiB, and a ready 8 KiB personal upload never ran. Scoped
        // downloads must not monopolise the cycle or block personal catch-up.
        //
        // Prefixes for which this download cycle succeeded: personal (empty
        // prefix) always decrypts `log_index.enc` or runs list+prove before
        // returning Ok — so Ok alone is a positive key proof even when 0 new
        // entries are replayed. Org targets only mark proven when at least one
        // entry was replayed; otherwise the pre-upload proof runs (lean path).
        // Personal-first split is the thrash fix (helpers are unit-tested):
        // never walk scoped targets before personal upload.
        let (personal_idxs, scoped_idxs) =
            split_personal_and_scoped_indices(targets, |t| t.prefix.is_empty());
        let scoped_total = scoped_idxs.len();

        self.download_personal_targets(targets, &personal_idxs, state)
            .await;
        phases.mark("personal_download");

        // Personal download failure blocks the whole cycle (key-safety gate).
        if let Some(e) = state.first_transfer_error.take() {
            return Err(e);
        }

        let scoped_downloads_ran_before_upload = personal_idxs.is_empty();
        if scoped_downloads_ran_before_upload {
            // No personal target: scoped downloads are the key-safety gate for
            // this cycle, so failures must block upload (propagate_errors=true).
            self.run_scoped_download_pass(targets, &scoped_idxs, scoped_total, true, state)
                .await?;
            phases.mark("scoped_download");
        }

        log_personal_download_complete(
            targets.len(),
            scoped_total,
            state.downloaded,
            entries.len(),
            bytes_selected,
        );

        if !entries.is_empty() {
            self.upload_partitioned_entries(
                &entries,
                bytes_selected,
                targets,
                partitioner,
                &personal_idxs,
                state,
            )
            .await?;
        }
        phases.mark("legacy_upload");

        // Phase: scoped downloads AFTER personal upload so org catch-up cannot
        // starve personal outbox drain (re-enable thrash: 21 org targets before
        // any upload). Cap via [`scoped_downloads_per_cycle`] (at most 1 per
        // cycle, any nonzero count — round-robin below still rotates through
        // every registered target over successive cycles instead of
        // disabling downloads once the registry passes 4).
        // Failures are non-fatal here (propagate_errors=false): personal upload
        // already ran, so org catch-up must not reverse personal progress.
        if !scoped_downloads_ran_before_upload {
            self.run_scoped_download_pass(targets, &scoped_idxs, scoped_total, false, state)
                .await?;
            phases.mark("scoped_download");
        }

        Ok(scoped_idxs)
    }

    /// Refresh upload caps and run the local compaction passes before staging uploads.
    async fn run_pre_upload_maintenance(
        &self,
        phases: &mut SyncPhaseTimer,
    ) -> UploadPolicySnapshot {
        // Adaptive (or fixed) upload caps from RAM headroom / EWMA / env.
        // Must run before schedule_outbox_entries so the in-memory queue depth
        // matches what this cycle will actually select and PUT.
        let upload_caps = self.refresh_upload_policy().await;
        phases.mark("upload_policy");

        // D3: compact-if-dirty on the ~6h photograph cadence, 5-minute budget.
        // Independent of the 120s snapshot+log publisher. A held packing lock
        // skips this pass so an in-flight photograph is not delayed.
        self.maybe_photograph_aligned_compact_if_dirty().await;
        phases.mark("photograph_compact");
        self.compact_local_planes().await;
        phases.mark("plane_compact");
        upload_caps
    }

    /// Run one scoped-download pass, threading the cycle state fields it updates.
    async fn run_scoped_download_pass(
        &self,
        targets: &[SyncTarget],
        scoped_idxs: &[usize],
        scoped_total: usize,
        propagate_errors: bool,
        state: &mut CycleState,
    ) -> SyncResult<()> {
        self.run_scoped_downloads(ScopedDownloadPass {
            targets,
            scoped_idxs,
            scoped_total,
            propagate_errors,
            downloaded: &mut state.downloaded,
            proven_prefixes: &mut state.proven_prefixes,
            first_transfer_error: &mut state.first_transfer_error,
        })
        .await
    }

    /// Outbox overflow valve. Returns `(before, after, healed)` outbox depth.
    async fn run_outbox_overflow_valve(&self) -> (usize, usize, bool) {
        let overflow_before = self.outbox_count().await.unwrap_or(0);
        if let Err(e) = self.maybe_force_snapshot_for_outbox_overflow().await {
            tracing::warn!(error = %e, "outbox overflow snapshot step failed (non-fatal)");
        }
        let overflow_after = self.outbox_count().await.unwrap_or(overflow_before);
        let overflow_healed = overflow_before > 0 && overflow_after < overflow_before;
        (overflow_before, overflow_after, overflow_healed)
    }
}

/// Log that a latched bootstrap block suppresses upload staging this cycle.
fn log_backup_blocked(blocker: &SyncBackupBlocker) {
    tracing::warn!(
        target: "fold_db::sync",
        sync_target = %blocker.target,
        head_seq = ?blocker.head_seq,
        blocked_since = blocker.blocked_since,
        "backup bootstrap blocked: suppressing upload staging for this cycle (downloads still run)"
    );
}

/// Turn the finished cycle state into the cycle result, surfacing any block or transfer error.
fn log_personal_download_complete(
    target_count: usize,
    scoped_total: usize,
    downloaded: u64,
    selected: usize,
    bytes_selected: u64,
) {
    tracing::info!(
        target: "fold_db::sync::memory",
        target_count,
        scoped_total,
        downloaded,
        selected,
        bytes_selected,
        "personal download complete; evaluating upload before scoped downloads"
    );
}

fn finish_cycle(
    mut state: CycleState,
    backup_blocked: Option<&SyncBackupBlocker>,
    overflow_before: usize,
    overflow_after: usize,
    overflow_healed: bool,
) -> SyncResult<bool> {
    // Suppressing the upload work must not launder the block into a healthy
    // cycle: with no entries selected there is no transfer to fail, so
    // without this the cycle would return Ok and `record_sync_success()`
    // would clear the failure streak and `last_error` on every tick while
    // backup remained impossible. Same reasoning as the overflow-heal note
    // above. A real transfer error from the download side still wins.
    if let (None, Some(blocker)) = (&state.first_transfer_error, &backup_blocked) {
        state.first_transfer_error = Some(SyncError::BackupBootstrapBlocked {
            target: blocker.target.clone(),
            reason: blocker.reason.clone(),
        });
    }

    if let Some(e) = state.first_transfer_error {
        if overflow_healed {
            tracing::warn!(
                error = %redact_sync_error_text(&e.to_string()),
                before = overflow_before,
                after = overflow_after,
                "outbox overflow snapshot reduced staging after transfer error; still returning transfer failure (heal must not launder failure streak)"
            );
        }
        return Err(e);
    }

    Ok(state.uploaded > 0 || state.downloaded > 0 || overflow_healed)
}
