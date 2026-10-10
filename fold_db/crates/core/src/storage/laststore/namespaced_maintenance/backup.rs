//! Backup manifest, chunk and restore entry points on [`LastStoreNamespacedStore`].

use super::*;

impl LastStoreNamespacedStore {
    /// Directory for small durable sidecar files (derived from the high-water
    /// marker location). `None` for stores opened without one.
    pub fn durable_sidecar_dir(&self) -> Option<std::path::PathBuf> {
        self.high_water
            .as_ref()
            .and_then(|hw| hw.sidecar_dir())
            .map(std::path::Path::to_path_buf)
    }

    /// Durable local evidence that a snapshot manifest reached cloud CAS.
    ///
    /// Mutation-log-first sync uses this to require an S0 before publishing
    /// post-base log segments. `None` means this store was opened without the
    /// high-water sidecar and therefore cannot prove a committed base.
    pub fn backup_durability(&self) -> StorageResult<Option<BackupDurability>> {
        self.high_water
            .as_ref()
            .map(|high_water| high_water.backup_durability())
            .transpose()
    }

    /// Stable cloud object root for this database.
    ///
    /// LastStore's high-water `store_uuid` is the durable local database
    /// identity used by backup manifests. Cloud object roots need a 64-hex
    /// value, so derive a namespaced SHA-256 from that UUID instead of using
    /// the authenticated user or org principal as the storage root.
    pub fn cloud_db_hash(&self) -> StorageResult<Option<String>> {
        let Some(high_water) = self.high_water.as_ref() else {
            return Ok(None);
        };
        let state = high_water.load_or_init()?;
        Ok(Some(cloud_db_hash_for_store_uuid(&state.store_uuid)))
    }

    pub fn verify_integrity(&self) -> StorageResult<()> {
        self.store
            .verify_integrity()
            .map_err(LastStoreKvStore::map_error)
    }

    /// Force-seal a local Last Store snapshot and build the authenticated cloud
    /// backup manifest for it, without uploading anything.
    ///
    /// The caller supplies the prior manifest, if any, so atom chunks can remain
    /// a delta/superset list (`N+1 = N + newly sealed`) while mutable planes are
    /// listed in full for this snapshot generation.
    pub fn cut_backup_manifest(
        &self,
        previous_manifest: Option<&BackupManifest>,
    ) -> StorageResult<BackupManifest> {
        backup_manifest::cut_backup_manifest(self, previous_manifest)
    }

    /// Cut with optional complete object-store presence so carried-forward
    /// atom refs that exist neither locally nor in cloud can be retired with
    /// an authenticated unbackable deletion receipt.
    pub fn cut_backup_manifest_with_cloud_presence(
        &self,
        previous_manifest: Option<&BackupManifest>,
        cloud: Option<&CloudChunkPresence>,
    ) -> StorageResult<BackupManifest> {
        backup_manifest::cut_backup_manifest_with_cloud_presence(self, previous_manifest, cloud)
    }

    /// Refuse a backup cut if the cold-load cap left any unsealed group out.
    pub fn cut_backup_manifest_with_cloud_presence_strict(
        &self,
        previous_manifest: Option<&BackupManifest>,
        cloud: Option<&CloudChunkPresence>,
    ) -> StorageResult<BackupManifest> {
        backup_manifest::cut_backup_manifest_with_cloud_presence_strict(
            self,
            previous_manifest,
            cloud,
        )
    }

    /// Stamp committed successor-history SHAs into pending purged retirements.
    /// Takes `atom_retirement_lock`. Refuses if compaction is in progress.
    pub fn stamp_pending_committed_successor_history(
        &self,
    ) -> StorageResult<StampCommittedSuccessorHistoryReport> {
        backup_manifest::stamp_pending_committed_successor_history(self)
    }

    /// Report-only successor-history stamp. Does not write the sidecar.
    pub fn report_committed_successor_history(
        &self,
    ) -> StorageResult<StampCommittedSuccessorHistoryReport> {
        backup_manifest::report_committed_successor_history(self)
    }

    /// True when every atom ref is an exact verified prefix at its local address.
    pub fn atom_photograph_is_disk_copy(&self, manifest: &BackupManifest) -> StorageResult<bool> {
        Ok(self
            .atom_photograph_copy_report(manifest, None, true)?
            .is_complete())
    }

    /// Backup-only may retain a prior atom file that exists only in cloud.
    /// Require a complete cloud list for every such file. The CAS path later
    /// checks each file again before it commits the new manifest.
    pub fn atom_photograph_has_verified_cloud_copies(
        &self,
        manifest: &BackupManifest,
        cloud: Option<&CloudChunkPresence>,
    ) -> StorageResult<bool> {
        Ok(self
            .atom_photograph_copy_report(manifest, cloud, false)?
            .is_complete())
    }

    pub fn atom_photograph_copy_report(
        &self,
        manifest: &BackupManifest,
        cloud: Option<&CloudChunkPresence>,
        allow_append_growth: bool,
    ) -> StorageResult<AtomPhotographCopyReport> {
        backup_manifest::atom_photograph_copy_report(self, manifest, cloud, allow_append_growth)
    }

    pub fn missing_committed_atom_groups_have_cloud_copies(
        &self,
        previous: Option<&BackupManifest>,
        current: &BackupManifest,
        cloud: Option<&CloudChunkPresence>,
        expected_missing_groups: u64,
    ) -> StorageResult<bool> {
        backup_manifest::missing_committed_atom_groups_have_cloud_copies(
            self,
            previous,
            current,
            cloud,
            expected_missing_groups,
        )
    }

    pub fn commit_backup_manifest(&self, manifest: &BackupManifest) -> StorageResult<()> {
        backup_manifest::commit_backup_manifest(self, manifest)
    }

    /// Record that the held sealed-home cut was abandoned as unpublishable.
    ///
    /// Durable because the abandon is a one-shot in-process event: the demoted
    /// publisher releases the hold and every later cycle idles without a
    /// progress sample, so a restart has only this marker to learn from.
    pub fn record_sealed_base_abandoned(&self) -> StorageResult<()> {
        let high_water = self.high_water.as_ref().ok_or_else(|| {
            StorageError::BackendError(
                "laststore sealed-base abandon marker requires a high-water marker".to_string(),
            )
        })?;
        high_water.record_sealed_base_abandoned()?;
        Ok(())
    }

    /// Raise the local backup counter to at least cloud `latest.counter`
    /// without stamping a commit time (the observed tip is not ours).
    pub fn observe_cloud_backup_counter(&self, counter: u64) -> StorageResult<u64> {
        let high_water = self.high_water.as_ref().ok_or_else(|| {
            StorageError::BackendError(
                "laststore backup counter observe requires a high-water marker".to_string(),
            )
        })?;
        Ok(high_water
            .observe_cloud_backup_counter(counter)?
            .backup_manifest_counter)
    }

    /// Persist a higher backup publisher epoch so the next cut can rebind
    /// cloud `latest` after a `store_uuid_mismatch` (epoch must exceed cloud).
    pub fn ensure_backup_epoch_at_least(&self, min_epoch: u64) -> StorageResult<u64> {
        let high_water = self.high_water.as_ref().ok_or_else(|| {
            StorageError::BackendError(
                "laststore backup epoch rebind requires a high-water marker".to_string(),
            )
        })?;
        Ok(high_water
            .ensure_backup_epoch_at_least(min_epoch)?
            .backup_epoch)
    }

    pub fn validate_restore_candidate(&self, manifest: &BackupManifest) -> StorageResult<()> {
        let high_water = self.high_water.as_ref().ok_or_else(|| {
            StorageError::BackendError("laststore restore requires a high-water marker".to_string())
        })?;
        high_water.validate_restore_candidate(
            &manifest.store_uuid,
            manifest.epoch,
            manifest.counter,
            manifest.cut_csn,
        )?;
        Ok(())
    }

    pub fn install_backup_chunk(&self, chunk: &BackupChunkRef, bytes: &[u8]) -> StorageResult<()> {
        let actual = sha256_hex(bytes);
        if actual != chunk.sha256 {
            return Err(StorageError::BackendError(format!(
                "backup chunk {} sha256 mismatch",
                chunk.sha256
            )));
        }
        let chunk_uuid = Uuid::parse_str(&chunk.chunk_uuid).map_err(|e| {
            StorageError::BackendError(format!(
                "backup chunk {} has invalid UUID: {e}",
                chunk.chunk_uuid
            ))
        })?;
        let meta = self
            .store
            .install_manifest_chunk(
                &chunk.collection,
                chunk.shard,
                chunk.group_id,
                chunk_uuid,
                bytes,
            )
            .map_err(LastStoreKvStore::map_error)?;
        if meta.collection != chunk.collection
            || meta.shard != chunk.shard
            || meta.group_id != chunk.group_id
        {
            return Err(StorageError::BackendError(format!(
                "backup chunk {} installed at unexpected collection/shard/group",
                chunk.chunk_uuid
            )));
        }
        if meta.end_csn != chunk.end_csn {
            return Err(StorageError::BackendError(format!(
                "backup chunk {} end_csn mismatch: manifest={} installed={}",
                chunk.chunk_uuid, chunk.end_csn, meta.end_csn
            )));
        }
        Ok(())
    }

    pub fn commit_restored_backup_manifest(&self, manifest: &BackupManifest) -> StorageResult<u64> {
        let high_water = self.high_water.as_ref().ok_or_else(|| {
            StorageError::BackendError("laststore restore requires a high-water marker".to_string())
        })?;
        let state = high_water.record_restored_backup_manifest(
            &manifest.store_uuid,
            manifest.epoch,
            manifest.counter,
            manifest.cut_csn,
        )?;
        Ok(state.backup_epoch)
    }

    /// Enumerate sealed chunks eligible for cloud backup upload, without
    /// forcing a snapshot cut or mutating high-water state.
    pub fn enumerate_backup_chunk_candidates(
        &self,
        previous_manifest: Option<&BackupManifest>,
    ) -> StorageResult<Vec<BackupChunkUploadCandidate>> {
        backup_manifest::enumerate_backup_chunk_candidates(self, previous_manifest)
    }

    /// Enumerate the full local chunk set for a held publish target.
    ///
    /// The target denominator must match the chunks CAS can require from the
    /// manifest. Delta-filtering by the previous manifest can exclude atom refs
    /// that are still authenticated by the new manifest, making status read
    /// complete while CAS still rejects the cut.
    pub fn enumerate_backup_publish_target_candidates(
        &self,
    ) -> StorageResult<Vec<BackupChunkUploadCandidate>> {
        backup_manifest::enumerate_backup_publish_target_candidates(self)
    }

    /// Directory for immutable pack sidecars during a held backup cut.
    /// Older binaries used this path for sealed-file clones.
    /// `None` when no high-water marker is configured.
    #[must_use]
    pub fn backup_cut_freeze_dir(&self, manifest_counter: u64) -> Option<std::path::PathBuf> {
        self.high_water.as_ref().and_then(|hw| {
            hw.sidecar_dir().map(|dir| {
                dir.join("backup-cut-freeze")
                    .join(manifest_counter.to_string())
            })
        })
    }

    /// As [`Self::enumerate_backup_chunk_candidates`], but also reports the
    /// chunks that could not be verified instead of discarding that signal.
    pub fn scan_backup_chunks(
        &self,
        previous_manifest: Option<&BackupManifest>,
    ) -> StorageResult<BackupChunkScan> {
        backup_manifest::scan_backup_chunks(self, previous_manifest)
    }
}
