//! Post-CAS bookkeeping and stale-counter healing.

use super::*;

impl SyncEngine {
    /// After a landed CAS: commit locally, update tip identity, release the
    /// held target, queue orphan GC. Returns the GC-eligible log segment count.
    pub(super) async fn cas_finalize_landed(
        &self,
        store: &crate::storage::LastStoreNamespacedStore,
        manifest: &BackupManifest,
        manifest_sha256: &str,
    ) -> SyncResult<usize> {
        store
            .commit_backup_manifest(manifest)
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
                manifest_sha256: manifest_sha256.to_string(),
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
            manifest_sha256,
            manifest.cut_csn,
            manifest.counter,
            &manifest.store_uuid,
            manifest.epoch,
        );
        debug_assert!(
            LatestCasPayload::cas_allows_replace(None, &cas_payload) || cas_payload.counter >= 1,
            "CAS payload must be valid for first publish"
        );
        Ok(gc_eligible_log_segments)
    }

    /// After `stale_counter` on CAS: if cloud `latest` already *is* this held
    /// cut, a prior cycle CASed successfully and only local commit failed.
    /// Return the cloud latest key so the caller can finish bookkeeping.
    ///
    /// `Ok(None)` is "not our cut, or could not read"; `Err` is only the
    /// named [`SyncError::UnsupportedBackupFormat`] for a pointer whose
    /// format this publisher does not read.
    pub(in super::super) async fn heal_already_cas_landed_held_cut(
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
    pub(in super::super) async fn observe_cloud_latest_counter_before_cut(&self) {
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
    pub(in super::super) async fn bump_local_counter_after_foreign_stale_cas(
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
