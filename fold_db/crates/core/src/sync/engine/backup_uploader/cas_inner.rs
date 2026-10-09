use super::*;

impl SyncEngine {
    // lint:fn-size-ok moved verbatim from backup_uploader.rs; splitting this function is separate work.
    pub(super) async fn cas_backup_publish_target_inner(
        &self,
        allow_rebind_retry: bool,
        allow_retirement: bool,
        previous_manifest: Option<&BackupManifest>,
        expected_primary_resume_cut: Option<
            &super::super::primary_resume::PrimaryResumeCutIdentity,
        >,
    ) -> SyncResult<(BackupManifest, LastStoreCloudSnapshotReport)> {
        let store = self.laststore_backup_source.as_ref().ok_or_else(|| {
            SyncError::Storage(
                "lastdb cloud snapshot requires a LastStore backup source".to_string(),
            )
        })?;
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
                    let healed_manifest = {
                        let guard = self.backup_publish_target.lock().await;
                        guard
                            .as_ref()
                            .map_or_else(|| manifest.clone(), |t| t.manifest.clone())
                    };
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

                if !verified_ok {
                    // Slice 4: leftover names that exist nowhere (any role,
                    // not only carried-forward atoms) become named holes so
                    // the stamp can publish. Inherited cloud-present names
                    // are not holes — they never appear in `outstanding`. Only
                    // HEAD-confirmed absences may be holed; an unconfirmed
                    // (errored) probe must not punch a hole for a digest that
                    // might still be sitting in cloud.
                    if let Some(holed) = self
                        .try_record_named_holes_on_held_target(
                            previous_manifest,
                            &outstanding.confirmed_missing,
                        )
                        .await
                    {
                        let healed_manifest = {
                            let guard = self.backup_publish_target.lock().await;
                            guard
                                .as_ref()
                                .map_or_else(|| manifest.clone(), |t| t.manifest.clone())
                        };
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
                }
                if !verified_ok {
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
                    } else if remaining_are_uploadable {
                        return Err(SyncError::BackupSnapshotVerifyPending {
                            missing_chunks: outstanding.len(),
                        });
                    } else {
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
                        let folded = fold_unbackable_retirements(&passes);
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
                        return Err(SyncError::Storage(retain_after_retirement_message(
                            missing_before,
                            folded,
                            unbackable_manifest_chunks,
                            missing_after,
                        )));
                    }
                }
                // Fall through with refreshed held manifest.
            } else {
                return Err(SyncError::BackupSnapshotVerifyPending {
                    missing_chunks: missing.len(),
                });
            }
        }

        // Re-read the held target after a possible mid-CAS retirement heal so
        // the bytes we publish match the (possibly narrowed) cut.
        let (manifest, chunks_in_target) = {
            let target = self.backup_publish_target.lock().await;
            let target = target
                .as_ref()
                .ok_or_else(missing_backup_publish_target_for_cas)?;
            (target.manifest.clone(), target.total())
        };

        // Retirement and hole exclusion can change a cut after its initial
        // validation. Validate the exact final bytes' predecessor before any
        // manifest upload or latest CAS. Never publish an unrestorable step.
        validate_manifest_chain(previous_manifest, &manifest)
            .map_err(|e| SyncError::Storage(format!("final backup manifest chain invalid: {e}")))?;
        let manifest_bytes = serde_json::to_vec(&manifest)
            .map_err(|e| SyncError::Storage(format!("encode backup manifest failed: {e}")))?;
        let manifest_sha256 = manifest_sha256_hex(&manifest)
            .map_err(|e| SyncError::Storage(format!("hash backup manifest failed: {e}")))?;
        if let Some(expected) = expected_primary_resume_cut {
            require_primary_resume_manifest_identity(&manifest, expected)?;
        }
        let manifest_presign = self
            .auth
            .presign_backup_manifest_upload(&manifest_sha256, manifest_bytes.len() as u64)
            .await?;
        if !manifest_presign.already_present {
            let url = manifest_presign.url.ok_or_else(|| {
                SyncError::Auth("backup manifest upload presign returned no URL".to_string())
            })?;
            self.s3.upload_snapshot(&url, manifest_bytes).await?;
            self.auth
                .confirm_backup_manifest_upload(&manifest_sha256)
                .await?;
        }
        if !self
            .auth
            .require_backup_manifest_present(&manifest_sha256)
            .await?
        {
            return Err(SyncError::Storage(format!(
                "backup manifest {manifest_sha256} is missing from cloud backup storage"
            )));
        }

        // A lost source home also loses its random store UUID and physical
        // layout. Publish both under the account root before the latest CAS.
        // An orphan descriptor from a failed CAS cannot pass the restore's
        // exact latest-pointer check.
        let recovery_key = self.enc_key.as_ref().ok_or_else(|| {
            SyncError::Storage("backup recovery descriptor needs the account E2E key".into())
        })?;
        let db_hash = cloud_db_hash_for_store_uuid(&manifest.store_uuid);
        if self.auth.db_hash_scope() != Some(db_hash.as_str()) {
            return Err(SyncError::Storage(
                "backup recovery descriptor database scope does not match the publisher".into(),
            ));
        }
        let options = store.options();
        let layout = laststore::LayoutDescriptor {
            layout_mode: options.layout_mode,
            shard_bits: options.shard_bits,
            hash_group_bits: options.hash_group_bits,
            hash_algo: options.hash_algo,
            hash_group_key: options.hash_group_key,
            hash_group_partition_fanout: options.hash_group_partition_fanout,
            layout_epoch: options.layout_epoch,
            packaging: options.packaging,
        };
        let descriptor = RecoveryDescriptorV1::new_normal(
            &manifest.store_uuid,
            &db_hash,
            layout,
            &manifest_sha256,
            manifest.counter,
            manifest.epoch,
        )
        .map_err(SyncError::Storage)?;
        let (descriptor_name, ciphertext) =
            descriptor.seal(recovery_key).map_err(SyncError::Storage)?;
        let descriptor_sha256 = crate::hex::sha256_hex(&ciphertext);
        self.auth
            .backup_recovery_descriptor_put(crate::sync::auth::ops::BackupRecoveryDescriptorPut {
                db_hash: &db_hash,
                store_uuid: &manifest.store_uuid,
                epoch: manifest.epoch,
                counter: manifest.counter,
                manifest_sha256: &manifest_sha256,
                descriptor_name: &descriptor_name,
                descriptor_sha256: &descriptor_sha256,
                ciphertext: &ciphertext,
            })
            .await?;

        // Cloud CAS is the durable source of truth. Local
        // `commit_backup_manifest` runs *after* CAS and can fail (ENOSPC,
        // permissions). When that happens the held target stays in memory, the
        // next cycle re-issues the same counter, and the server rejects it as
        // `stale_counter` forever — a livelock. Heal by treating cloud identity
        // match as "already landed" and finishing local bookkeeping only.
        if let Some(expected) = expected_primary_resume_cut {
            self.require_primary_resume_previous_latest(expected)
                .await?;
        }
        let cas = self
            .cas_backup_latest_for_resume(
                expected_primary_resume_cut
                    .is_some_and(|expected| expected.previous_latest.is_none()),
                &manifest,
                &manifest_sha256,
            )
            .await;
        let latest_key = match cas {
            Ok(latest) => latest.key,
            Err(err) if allow_rebind_retry && is_store_uuid_mismatch_error(&err) => {
                // Cloud `latest` is owned by a different store_uuid under the
                // same/lower epoch. Bump local publisher epoch above cloud's
                // and re-cut once so CAS can rebind (server requires
                // candidate.epoch > current.epoch for store_uuid change).
                let cloud_epoch = match self.auth.backup_latest_get().await {
                    Ok(get) => get.latest.epoch,
                    Err(get_err) => {
                        tracing::warn!(
                            target: "fold_db::sync::snapshot_log",
                            error = %redact_sync_error_text(&get_err.to_string()),
                            "backup_latest_get after store_uuid_mismatch failed; using local epoch+1"
                        );
                        manifest.epoch
                    }
                };
                let min_epoch = cloud_epoch
                    .saturating_add(1)
                    .max(manifest.epoch.saturating_add(1));
                let new_epoch = store.ensure_backup_epoch_at_least(min_epoch).map_err(|e| {
                    SyncError::Storage(format!(
                        "backup epoch rebind after store_uuid_mismatch failed: {e}"
                    ))
                })?;
                tracing::warn!(
                    target: "fold_db::sync::snapshot_log",
                    local_store_uuid = %manifest.store_uuid,
                    local_epoch = manifest.epoch,
                    cloud_epoch,
                    rebind_epoch = new_epoch,
                    "store_uuid_mismatch on CAS; rebinding publisher epoch and retrying once"
                );
                // The rebind changes the epoch this cut must carry, so the held
                // target is stale — drop it and let the retry cut a fresh one.
                self.retire_backup_publish_target().await;
                return Box::pin(self.laststore_cloud_snapshot_once(
                    previous_manifest,
                    false,
                    allow_retirement,
                    None,
                ))
                .await;
            }
            Err(err) if is_stale_counter_error(&err) => {
                match self
                    .heal_already_cas_landed_held_cut(&manifest, &manifest_sha256)
                    .await
                {
                    Ok(Some(key)) => key,
                    Ok(None)
                        if allow_rebind_retry
                            && self
                                .bump_local_counter_after_foreign_stale_cas(
                                    &manifest,
                                    &manifest_sha256,
                                )
                                .await =>
                    {
                        self.retire_backup_publish_target().await;
                        return Box::pin(self.laststore_cloud_snapshot_once(
                            previous_manifest,
                            false,
                            allow_retirement,
                            None,
                        ))
                        .await;
                    }
                    Ok(None) => return Err(err),
                    // Cloud `latest` names a format this publisher does not
                    // read. Surface that by name instead of the stale_counter
                    // it hid behind; no recut can land against it.
                    Err(unsupported) => return Err(unsupported),
                }
            }
            Err(err) => return Err(err),
        };

        store
            .commit_backup_manifest(&manifest)
            .map_err(|e| SyncError::Storage(format!("commit backup manifest failed: {e}")))?;
        let backup_only = self.backup_only_mode.load(Ordering::SeqCst);
        if backup_only {
            // A paused-home backup can preserve an incomplete local store.
            // Retain every older cloud chunk until read integrity and peer
            // reconciliation have their own proof.
            *self.backup_published_tip_identity.lock().await = None;
            self.backup_gc_identity_revoked
                .store(true, std::sync::atomic::Ordering::SeqCst);
        } else {
            *self.backup_published_tip_identity.lock().await = Some(BackupTipIdentity {
                store_uuid: manifest.store_uuid.clone(),
                epoch: manifest.epoch,
                counter: manifest.counter,
                manifest_sha256: manifest_sha256.clone(),
            });
            self.backup_gc_identity_revoked
                .store(false, std::sync::atomic::Ordering::SeqCst);
        }
        // Landed. Release the packing lock so the next cycle cuts a fresh one
        // that includes everything written while this one was draining.
        self.retire_backup_publish_target().await;
        self.record_backup_publish_landed();
        // Queue post-CAS orphan GC with keep-set = this latest tip only.
        // Continuous uploader drains it on a detached task so local R/W and the
        // drain loop never wait on cloud DELETEs. Manual `lastdb cloud backup-gc`
        // remains the operator override path.
        if !backup_only {
            self.enqueue_post_cas_backup_orphan_gc(manifest.clone())
                .await;
        }

        let chunks_referenced = manifest.mutable_chunks.len() + manifest.atom_chunks.len();
        // Object model: F = cut_csn; mutation-log collection chunks with end_csn ≤ F
        // are GC-eligible only after this successful CAS (never before).
        let log_through_ids: Vec<u64> = manifest
            .mutable_chunks
            .iter()
            .filter(|c| c.collection == "log")
            .map(|c| c.end_csn)
            .collect();
        let gc_eligible_log_segments =
            Self::count_gc_eligible_log_segments_after_cas(manifest.cut_csn, log_through_ids);
        let cas_payload = LatestCasPayload::from_backup_cut(
            &manifest_sha256,
            manifest.cut_csn,
            manifest.counter,
            &manifest.store_uuid,
            manifest.epoch,
        );
        debug_assert!(
            LatestCasPayload::cas_allows_replace(None, &cas_payload) || cas_payload.counter >= 1,
            "CAS payload must be valid for first publish"
        );
        let report = LastStoreCloudSnapshotReport {
            manifest_sha256,
            manifest_key: manifest_presign.key.unwrap_or_default(),
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
        };
        Ok((manifest, report))
    }

    /// After `stale_counter` on CAS: if cloud `latest` already *is* this held
    /// cut, a prior cycle CASed successfully and only local commit failed.
    /// Return the cloud latest key so the caller can finish bookkeeping.
    ///
    /// `Ok(None)` is "not our cut, or could not read"; `Err` is only the
    /// named [`SyncError::UnsupportedBackupFormat`] for a pointer whose
    /// format this publisher does not read.
    pub(super) async fn heal_already_cas_landed_held_cut(
        &self,
        manifest: &BackupManifest,
        manifest_sha256: &str,
    ) -> SyncResult<Option<String>> {
        let get = match self.auth.backup_latest_get().await {
            Ok(get) => get,
            Err(get_err) => {
                tracing::warn!(
                    target: "fold_db::sync::snapshot_log",
                    error = %redact_sync_error_text(&get_err.to_string()),
                    "backup_latest_get after stale_counter failed; cannot self-heal post-CAS commit gap"
                );
                return Ok(None);
            }
        };
        if !backup_cloud_latest_held_cut_match(&get.latest, manifest, manifest_sha256)? {
            return Ok(None);
        }
        tracing::warn!(
            target: "fold_db::sync::snapshot_log",
            store_uuid = %manifest.store_uuid,
            epoch = manifest.epoch,
            counter = manifest.counter,
            "cloud latest already matches held cut after stale_counter; finishing local commit (prior CAS landed, local bookkeeping lagging)"
        );
        Ok(Some(get.key))
    }

    /// Best-effort: if cloud `latest` is already ahead of local high-water,
    /// raise the local counter so the next reserve is strictly above it.
    ///
    /// A CoW of prod last-publish N pointed at DEV that already holds N+1
    /// (a different tip) otherwise cuts N+1 and CAS-fails `stale_counter`.
    /// GET failure is a first-fill / offline — leave local high-water alone.
    pub(super) async fn observe_cloud_latest_counter_before_cut(&self) {
        let Some(store) = self.laststore_backup_source.as_ref() else {
            return;
        };
        let get = match self.auth.backup_latest_get().await {
            Ok(get) => get,
            Err(err) => {
                tracing::debug!(
                    target: "fold_db::sync::backup",
                    error = %redact_sync_error_text(&err.to_string()),
                    "no cloud backup/latest before cut; leaving local high-water"
                );
                return;
            }
        };
        let local = store
            .backup_durability()
            .ok()
            .flatten()
            .map_or(0, |d| d.backup_manifest_counter);
        let Some(observe) = cloud_counter_to_observe_before_cut(local, get.latest.counter) else {
            return;
        };
        match store.observe_cloud_backup_counter(observe) {
            Ok(new) => {
                tracing::warn!(
                    target: "fold_db::sync::backup",
                    local_counter = local,
                    cloud_counter = get.latest.counter,
                    observed = new,
                    "cloud latest is ahead of local high-water; next cut will reserve above it"
                );
            }
            Err(err) => {
                tracing::warn!(
                    target: "fold_db::sync::backup",
                    error = %redact_sync_error_text(&err.to_string()),
                    "failed to observe cloud backup counter before cut"
                );
            }
        }
    }

    /// After `stale_counter` on a *different* cloud tip: persist cloud.counter
    /// as local high-water so the retry cuts strictly ahead. Returns whether
    /// the caller should retire the held cut and recut.
    pub(super) async fn bump_local_counter_after_foreign_stale_cas(
        &self,
        manifest: &BackupManifest,
        manifest_sha256: &str,
    ) -> bool {
        let Some(store) = self.laststore_backup_source.as_ref() else {
            return false;
        };
        let Ok(get) = self.auth.backup_latest_get().await else {
            return false;
        };
        let matches =
            match backup_cloud_latest_held_cut_match(&get.latest, manifest, manifest_sha256) {
                Ok(matches) => matches,
                Err(unsupported) => {
                    // A recut cannot land against a format this publisher does
                    // not write; do not move local high-water toward it.
                    tracing::warn!(
                        target: "fold_db::sync::backup",
                        error = %unsupported,
                        "cloud latest names an unsupported backup format; not observing its counter"
                    );
                    return false;
                }
            };
        let Some(observe) =
            cloud_counter_to_observe_after_stale_cas(manifest.counter, get.latest.counter, matches)
        else {
            return false;
        };
        match store.observe_cloud_backup_counter(observe) {
            Ok(new) => {
                tracing::warn!(
                    target: "fold_db::sync::backup",
                    held_counter = manifest.counter,
                    cloud_counter = get.latest.counter,
                    observed = new,
                    "stale_counter against a different cloud tip; observing cloud counter and recutting"
                );
                true
            }
            Err(_) => false,
        }
    }
}
// lint:file-size-ok moved verbatim from backup_uploader.rs; cohesive unit, split further in a later pass
