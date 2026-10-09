use super::*;

impl SyncEngine {
    /// Download every personal target. A failure is recorded in the cycle
    /// state and blocks the rest of the cycle (key-safety gate).
    pub(super) async fn download_personal_targets(
        &self,
        targets: &[SyncTarget],
        personal_idxs: &[usize],
        state: &mut CycleState,
    ) {
        for &idx in personal_idxs {
            let target = &targets[idx];
            tracing::info!(
                target: "fold_db::sync::memory",
                target_label = %target.label,
                "download phase: starting personal target"
            );
            match self.download_with_auth_retry(target).await {
                Ok(n) => {
                    state.downloaded += n;
                    // Empty personal prefix: Ok alone is a positive key proof.
                    state.proven_prefixes.insert(target.prefix.clone());
                    tracing::info!(
                        target: "fold_db::sync::memory",
                        target_label = %target.label,
                        downloaded = n,
                        "download phase: personal target complete"
                    );
                }
                Err(e) => {
                    self.record_cloud_sync_transfer_failure("download", &target.label, &e)
                        .await;
                    tracing::warn!(
                        "download from '{}' failed: {}; personal upload for this sync cycle is blocked until replay succeeds",
                        target.label,
                        redact_sync_error_text(&e.to_string())
                    );
                    state.first_transfer_error = Some(select_more_severe_transfer_error(
                        state.first_transfer_error.take(),
                        e,
                    ));
                }
            }
        }
    }

    /// Shared scoped (org/share) download pass used before and after personal
    /// upload. Cap via [`scoped_downloads_per_cycle`]; when `propagate_errors`
    /// is true (pre-upload, no personal target) failures block the cycle and
    /// become the key-safety gate; when false (post-upload) they are non-fatal.
    ///
    /// Arguments are bundled in [`ScopedDownloadPass`] so the helper stays under
    /// the workspace `clippy::too_many_arguments` deny (self + 7 free args = 8).
    pub(super) async fn run_scoped_downloads(
        &self,
        pass: ScopedDownloadPass<'_>,
    ) -> SyncResult<()> {
        let ScopedDownloadPass {
            targets,
            scoped_idxs,
            scoped_total,
            propagate_errors,
            downloaded,
            proven_prefixes,
            first_transfer_error,
        } = pass;
        let max_scoped = scoped_downloads_per_cycle(scoped_total);
        if scoped_total > 4 {
            tracing::info!(
                target: "fold_db::sync::memory",
                scoped_total,
                "large scoped-target registry (likely leaked test orgs): capping scoped \
                 downloads to 1 per cycle, rotating round-robin through all targets instead \
                 of downloading every one"
            );
        }
        if max_scoped == 0 {
            return Ok(());
        }
        let rr = crate::clock::unix_secs() as usize;
        let picked = pick_scoped_round_robin(scoped_idxs, max_scoped, rr);
        for (pick_i, &idx) in picked.iter().enumerate() {
            let target = &targets[idx];
            if propagate_errors {
                tracing::info!(
                    target: "fold_db::sync::memory",
                    target_label = %target.label,
                    scoped_index = pick_i,
                    scoped_total,
                    "download phase: starting scoped target before upload (no personal target)"
                );
            } else {
                tracing::info!(
                    target: "fold_db::sync::memory",
                    target_label = %target.label,
                    scoped_index = pick_i,
                    scoped_total,
                    "post-upload scoped download: starting (capped per cycle)"
                );
            }
            match self.download_with_auth_retry(target).await {
                Ok(n) => {
                    *downloaded += n;
                    if n > 0 {
                        proven_prefixes.insert(target.prefix.clone());
                    }
                    if propagate_errors {
                        tracing::info!(
                            target: "fold_db::sync::memory",
                            target_label = %target.label,
                            downloaded = n,
                            "download phase: scoped target complete"
                        );
                    } else {
                        tracing::info!(
                            target: "fold_db::sync::memory",
                            target_label = %target.label,
                            downloaded = n,
                            "post-upload scoped download: complete"
                        );
                    }
                }
                Err(e) => {
                    self.record_cloud_sync_transfer_failure("download", &target.label, &e)
                        .await;
                    if propagate_errors {
                        tracing::warn!(
                            "download from '{}' failed: {}; upload for this sync cycle is blocked until replay succeeds",
                            target.label,
                            redact_sync_error_text(&e.to_string())
                        );
                        *first_transfer_error = Some(select_more_severe_transfer_error(
                            first_transfer_error.take(),
                            e,
                        ));
                    } else {
                        tracing::warn!(
                            "scoped download from '{}' failed (non-fatal; personal upload already attempted): {}",
                            target.label,
                            redact_sync_error_text(&e.to_string())
                        );
                    }
                }
            }
        }
        if propagate_errors {
            if let Some(e) = first_transfer_error.take() {
                return Err(e);
            }
        }
        Ok(())
    }
}
