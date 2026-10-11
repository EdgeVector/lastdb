//! Held-cut verification and unbackable-shortfall resolution for the CAS publish.

use super::*;

impl SyncEngine {
    /// Verify the held cut's chunks are present; on a shortfall with
    /// unbackable chunks retire, hole, or report pending.
    pub(super) async fn cas_ensure_cut_verified(
        &self,
        allow_retirement: bool,
        previous_manifest: Option<&BackupManifest>,
    ) -> SyncResult<()> {
        let (manifest, unbackable_manifest_chunks) = {
            let target = self.backup_publish_target.lock().await;
            let target = target
                .as_ref()
                .ok_or_else(missing_backup_publish_target_for_cas)?;
            (target.manifest.clone(), target.unbackable_manifest_chunks)
        };

        if let Err(missing) = self.try_verify_manifest_chunks_present(&manifest).await {
            // "will finish it" is only true when every missing digest is a
            // candidate. Carried-forward atom refs that reseal removed from disk
            // are not, so no amount of draining can close a shortfall that lands
            // on them — the same distinction `source_missing` already draws for
            // a candidate whose live sealed file vanished.
            if unbackable_manifest_chunks > 0 && allow_retirement {
                self.cas_resolve_unbackable_shortfall(
                    &manifest,
                    missing,
                    unbackable_manifest_chunks,
                    previous_manifest,
                )
                .await?;
                // Fall through with refreshed held manifest.
            } else {
                return Err(SyncError::BackupSnapshotVerifyPending {
                    missing_chunks: missing.len(),
                });
            }
        }

        Ok(())
    }

    pub(super) fn cas_build_report(
        manifest: &BackupManifest,
        manifest_sha256: String,
        manifest_key: String,
        latest_key: String,
        chunks_in_target: usize,
        gc_eligible_log_segments: usize,
    ) -> LastStoreCloudSnapshotReport {
        let chunks_referenced = manifest.mutable_chunks.len() + manifest.atom_chunks.len();
        LastStoreCloudSnapshotReport {
            manifest_sha256,
            manifest_key,
            latest_key,
            counter: manifest.counter,
            cut_csn: manifest.cut_csn,
            chunks_referenced,
            // The drain that filled this cut ran across many cycles, so a
            // single cycle's put count is not the number that landed it. The
            // honest figure at CAS time is the size of the cut itself.
            chunks_uploaded: 0,
            chunks_already_present: chunks_in_target,
            bytes_uploaded: 0,
            frontier_through: manifest.cut_csn,
            cas_counter: manifest.counter,
            gc_eligible_log_segments,
        }
    }

    /// Clone of the held target's manifest, or `fallback` when no target is held.
    pub(super) async fn cas_held_manifest_or(&self, fallback: &BackupManifest) -> BackupManifest {
        let guard = self.backup_publish_target.lock().await;
        guard
            .as_ref()
            .map_or_else(|| fallback.clone(), |t| t.manifest.clone())
    }

    /// Verify shortfall with unbackable manifest chunks: retire, hole, or
    /// classify the remainder. `Ok(())` means the cut now verifies.
    pub(super) async fn cas_resolve_unbackable_shortfall(
        &self,
        manifest: &BackupManifest,
        missing: ManifestPresenceProbe,
        unbackable_manifest_chunks: usize,
        previous_manifest: Option<&BackupManifest>,
    ) -> SyncResult<()> {
        // 2026-08-06 primary: sticky gen 502 looped forever with
        // missing == unbackable (776) and zero retirement receipts.
        // Listing at cut time kept the ghosts (or reconcile was
        // incomplete); HEAD after reseed is the same truth CAS uses —
        // retire those digests on the held cut and retry verify so we
        // do not livelock saying "must re-cut" while holding the same
        // generation.
        //
        // 2026-08-19: do **not** abandon after a partial retirement.
        // Abandon discards in-memory receipts and the next cut
        // re-imports the same ghosts from the last published manifest
        // (papercut-lastdb-unbackable-retirement-is-discarded-when-
        // the-held-cut-is-abandoned). Re-verify can also surface
        // previously cache-positive ghosts as newly missing — retire
        // those on further passes against the same held generation.
        let missing_before = missing.len();
        let (verified_ok, outstanding, passes) = self
            .cas_run_retirement_passes(manifest, previous_manifest, missing)
            .await;
        if verified_ok {
            return Ok(());
        }
        // Slice 4: leftover names that exist nowhere (any role,
        // not only carried-forward atoms) become named holes so
        // the stamp can publish. Inherited cloud-present names
        // are not holes — they never appear in `outstanding`. Only
        // HEAD-confirmed absences may be holed; an unconfirmed
        // (errored) probe must not punch a hole for a digest that
        // might still be sitting in cloud.
        let (verified_ok, outstanding) = self
            .cas_record_named_holes_pass(manifest, previous_manifest, outstanding)
            .await;
        if verified_ok {
            return Ok(());
        }
        self.cas_classify_remaining_shortfall(
            outstanding,
            &passes,
            missing_before,
            unbackable_manifest_chunks,
        )
        .await
    }

    /// Retire CAS-proven unbackable digests on the held cut, re-verifying
    /// after each pass. Returns (verified, outstanding, passes).
    pub(super) async fn cas_run_retirement_passes(
        &self,
        manifest: &BackupManifest,
        previous_manifest: Option<&BackupManifest>,
        missing: ManifestPresenceProbe,
    ) -> (bool, ManifestPresenceProbe, Vec<UnbackableRetirement>) {
        let mut outstanding = missing;
        let mut passes: Vec<UnbackableRetirement> = Vec::new();
        let mut verified_ok = false;
        for pass in 1..=MAX_UNBACKABLE_RETIREMENT_PASSES {
            // Retirement is HEAD-proven-absent evidence only — never
            // hand it a digest whose presence check merely errored.
            let Some(retirement) = self
                .try_retire_cas_proven_unbackable_on_held_target(
                    previous_manifest,
                    &outstanding.confirmed_missing,
                )
                .await
            else {
                break;
            };
            passes.push(retirement);
            let healed_manifest = self.cas_held_manifest_or(manifest).await;
            match self
                .try_verify_manifest_chunks_present(&healed_manifest)
                .await
            {
                Ok(()) => {
                    verified_ok = true;
                    tracing::info!(
                        target: "fold_db::sync::backup",
                        pass,
                        retired_this_pass = retirement.retired,
                        retired_total = passes.iter().map(|p| p.retired).sum::<usize>(),
                        "CAS verify clean after unbackable retirement on held cut"
                    );
                    break;
                }
                Err(still_missing) => {
                    tracing::info!(
                        target: "fold_db::sync::backup",
                        pass,
                        missing_before = outstanding.len(),
                        missing_after = still_missing.len(),
                        retired_this_pass = retirement.retired,
                        unbackable_after = retirement.unbackable_after,
                        "CAS shortfall remains after unbackable retirement pass; \
                         retaining held cut for another pass or next cycle"
                    );
                    outstanding = still_missing;
                }
            }
        }
        (verified_ok, outstanding, passes)
    }

    /// Record named holes for HEAD-confirmed absences, then re-verify.
    /// Returns (verified, outstanding).
    pub(super) async fn cas_record_named_holes_pass(
        &self,
        manifest: &BackupManifest,
        previous_manifest: Option<&BackupManifest>,
        mut outstanding: ManifestPresenceProbe,
    ) -> (bool, ManifestPresenceProbe) {
        let mut verified_ok = false;
        if let Some(holed) = self
            .try_record_named_holes_on_held_target(
                previous_manifest,
                &outstanding.confirmed_missing,
            )
            .await
        {
            let healed_manifest = self.cas_held_manifest_or(manifest).await;
            match self
                .try_verify_manifest_chunks_present(&healed_manifest)
                .await
            {
                Ok(()) => {
                    verified_ok = true;
                    tracing::info!(
                        target: "fold_db::sync::backup",
                        named_holes = holed,
                        "CAS verify clean after recording named holes on held cut"
                    );
                }
                Err(still_missing) => {
                    outstanding = still_missing;
                    tracing::info!(
                        target: "fold_db::sync::backup",
                        named_holes = holed,
                        missing_after = outstanding.len(),
                        "named holes recorded; remaining shortfall is still-uploading candidates"
                    );
                }
            }
        }
        (verified_ok, outstanding)
    }

    /// Shortfall remains after retirement and holes: pending (uploadable),
    /// closed (empty), or a retained-cut storage error.
    pub(super) async fn cas_classify_remaining_shortfall(
        &self,
        outstanding: ManifestPresenceProbe,
        passes: &[UnbackableRetirement],
        missing_before: usize,
        unbackable_manifest_chunks: usize,
    ) -> SyncResult<()> {
        let candidate_shas = {
            let guard = self.backup_publish_target.lock().await;
            guard.as_ref().map(|t| candidate_sha_set(&t.candidates))
        };
        let source_missing = {
            let generation = self
                .backup_publish_target
                .lock()
                .await
                .as_ref()
                .map(|t| t.generation);
            let mut missing = self.backup_unresolvable.lock().await;
            missing
                .for_generation(generation)
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
        };
        let remaining_are_uploadable = !outstanding.is_empty()
            && candidate_shas.as_ref().is_some_and(|cands| {
                outstanding
                    .iter()
                    .all(|sha| cands.contains(sha) && !source_missing.contains(sha))
            });
        if outstanding.is_empty() {
            // Holes closed the shortfall; fall through and stamp.
            return Ok(());
        }
        if remaining_are_uploadable {
            return Err(SyncError::BackupSnapshotVerifyPending {
                missing_chunks: outstanding.len(),
            });
        }
        let generation = self
            .backup_publish_target
            .lock()
            .await
            .as_ref()
            .map(|t| t.generation);
        let missing_after = outstanding.len();
        if passes.is_empty() {
            tracing::warn!(
                target: "fold_db::sync::backup",
                missing = missing_before,
                unbackable_manifest_chunks,
                generation = ?generation,
                "CAS shortfall with unbackable manifest chunks and no \
                 CAS-proven ghosts to retire; retaining held cut \
                 (automatic abandon suppressed)"
            );
            return Err(SyncError::Storage(retain_no_ghosts_message(
                missing_before,
                unbackable_manifest_chunks,
            )));
        }
        let folded = fold_unbackable_retirements(passes);
        tracing::warn!(
            target: "fold_db::sync::backup",
            missing_before,
            missing_after,
            retired = folded.retired,
            unbackable_before = unbackable_manifest_chunks,
            unbackable_after = folded.unbackable_after,
            passes = passes.len(),
            generation = ?generation,
            "CAS shortfall remains after unbackable retirement; \
             retaining held cut (automatic abandon suppressed)"
        );
        Err(SyncError::Storage(retain_after_retirement_message(
            missing_before,
            folded,
            unbackable_manifest_chunks,
            missing_after,
        )))
    }
}
