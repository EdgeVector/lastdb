//! Final manifest upload, recovery descriptor and latest-CAS outcome handling.

use super::*;

impl SyncEngine {
    /// Validate, hash, upload and confirm the final manifest bytes. Returns
    /// (manifest_sha256, manifest_key).
    pub(super) async fn cas_upload_final_manifest(
        &self,
        manifest: &BackupManifest,
        previous_manifest: Option<&BackupManifest>,
        expected_primary_resume_cut: Option<
            &super::super::primary_resume::PrimaryResumeCutIdentity,
        >,
    ) -> SyncResult<(String, String)> {
        // Retirement and hole exclusion can change a cut after its initial
        // validation. Validate the exact final bytes' predecessor before any
        // manifest upload or latest CAS. Never publish an unrestorable step.
        validate_manifest_chain(previous_manifest, manifest)
            .map_err(|e| SyncError::Storage(format!("final backup manifest chain invalid: {e}")))?;
        let manifest_bytes = serde_json::to_vec(manifest)
            .map_err(|e| SyncError::Storage(format!("encode backup manifest failed: {e}")))?;
        let manifest_sha256 = manifest_sha256_hex(manifest)
            .map_err(|e| SyncError::Storage(format!("hash backup manifest failed: {e}")))?;
        if let Some(expected) = expected_primary_resume_cut {
            require_primary_resume_manifest_identity(manifest, expected)?;
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
        Ok((manifest_sha256, manifest_presign.key.unwrap_or_default()))
    }

    /// A lost source home also loses its random store UUID and physical
    /// layout. Publish both under the account root before the latest CAS.
    /// An orphan descriptor from a failed CAS cannot pass the restore's
    /// exact latest-pointer check.
    pub(super) async fn cas_publish_recovery_descriptor(
        &self,
        store: &crate::storage::LastStoreNamespacedStore,
        manifest: &BackupManifest,
        manifest_sha256: &str,
    ) -> SyncResult<()> {
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
            manifest_sha256,
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
                manifest_sha256,
                descriptor_name: &descriptor_name,
                descriptor_sha256: &descriptor_sha256,
                ciphertext: &ciphertext,
            })
            .await?;
        Ok(())
    }

    /// Map the latest-CAS result to the landed latest key, or to a recut
    /// result (`Ok(Err(..))`) after an epoch rebind or counter bump.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn cas_resolve_latest_outcome(
        &self,
        cas: SyncResult<crate::sync::auth::ops::BackupLatestCasResponse>,
        store: &crate::storage::LastStoreNamespacedStore,
        allow_rebind_retry: bool,
        allow_retirement: bool,
        previous_manifest: Option<&BackupManifest>,
        manifest: &BackupManifest,
        manifest_sha256: &str,
    ) -> SyncResult<Result<String, (BackupManifest, LastStoreCloudSnapshotReport)>> {
        match cas {
            Ok(latest) => Ok(Ok(latest.key)),
            Err(err) if allow_rebind_retry && is_store_uuid_mismatch_error(&err) => {
                self.cas_rebind_epoch_for_store_uuid_mismatch(store, manifest)
                    .await?;
                // The rebind changes the epoch this cut must carry, so the held
                // target is stale — drop it and let the retry cut a fresh one.
                self.retire_backup_publish_target().await;
                let recut = Box::pin(self.laststore_cloud_snapshot_once(
                    previous_manifest,
                    false,
                    allow_retirement,
                    None,
                ))
                .await?;
                Ok(Err(recut))
            }
            Err(err) if is_stale_counter_error(&err) => {
                match self
                    .heal_already_cas_landed_held_cut(manifest, manifest_sha256)
                    .await
                {
                    Ok(Some(key)) => Ok(Ok(key)),
                    Ok(None)
                        if allow_rebind_retry
                            && self
                                .bump_local_counter_after_foreign_stale_cas(
                                    manifest,
                                    manifest_sha256,
                                )
                                .await =>
                    {
                        self.retire_backup_publish_target().await;
                        let recut = Box::pin(self.laststore_cloud_snapshot_once(
                            previous_manifest,
                            false,
                            allow_retirement,
                            None,
                        ))
                        .await?;
                        Ok(Err(recut))
                    }
                    Ok(None) => Err(err),
                    // Cloud `latest` names a format this publisher does not
                    // read. Surface that by name instead of the stale_counter
                    // it hid behind; no recut can land against it.
                    Err(unsupported) => Err(unsupported),
                }
            }
            Err(err) => Err(err),
        }
    }

    /// Cloud `latest` is owned by a different store_uuid under the same/lower
    /// epoch. Bump local publisher epoch above cloud's so CAS can rebind
    /// (server requires candidate.epoch > current.epoch for store_uuid change).
    pub(super) async fn cas_rebind_epoch_for_store_uuid_mismatch(
        &self,
        store: &crate::storage::LastStoreNamespacedStore,
        manifest: &BackupManifest,
    ) -> SyncResult<()> {
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
        Ok(())
    }
}
