//! Cut steps of establishing the held backup publish target.

use super::*;

impl SyncEngine {
    /// Stamp committed atom successor history before the photograph cut.
    /// Returns the count of atom groups with no local file.
    pub(super) fn stamp_successor_history_for_cut(
        &self,
        store: &crate::storage::LastStoreNamespacedStore,
    ) -> SyncResult<u64> {
        match store.stamp_pending_committed_successor_history() {
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
                Ok(report.groups_without_local_file)
            }
            Err(e) => {
                tracing::error!(
                    target: "fold_db::sync::backup",
                    error = %e,
                    "atom_successor_history_stamp_miss"
                );
                Err(SyncError::Storage(format!(
                    "atom keep-set is not a copy of disk: successor-history stamp failed: {e}"
                )))
            }
        }
    }

    /// Cut and validate the manifest the held target will publish.
    pub(super) fn cut_publish_target_manifest(
        &self,
        store: &crate::storage::LastStoreNamespacedStore,
        previous_manifest: Option<&BackupManifest>,
        cloud_presence: Option<&CloudChunkPresence>,
        primary_resume_root: bool,
        accept_local_damage: bool,
        missing_atom_groups: u64,
    ) -> SyncResult<BackupManifest> {
        let mut manifest = if self.backup_only_mode.load(Ordering::SeqCst) {
            store.cut_backup_manifest_with_cloud_presence_strict(previous_manifest, cloud_presence)
        } else if primary_resume_root {
            store.cut_backup_manifest_with_cloud_presence(previous_manifest, cloud_presence)
        } else {
            store.cut_normal_backup_manifest_with_cloud_presence(previous_manifest, cloud_presence)
        }
        .map_err(|e| SyncError::Storage(format!("cut backup manifest failed: {e}")))?;
        if missing_atom_groups > 0 && !accept_local_damage {
            let proven = store
                .missing_committed_atom_groups_have_cloud_copies(
                    previous_manifest,
                    &manifest,
                    cloud_presence,
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
        require_atom_photograph_copy(
            store,
            &manifest,
            cloud_presence,
            primary_resume_root && previous_manifest.is_none(),
            self.backup_only_mode.load(Ordering::SeqCst),
        )?;
        if primary_resume_root {
            if let Some(previous) = previous_manifest {
                make_verified_primary_resume_root(previous, &mut manifest)?;
            } else {
                manifest.deletion_receipts.clear();
                validate_manifest_chain_before_packing(None, &manifest).map_err(|error| {
                    SyncError::Storage(format!("fresh local root is invalid: {error}"))
                })?;
            }
        } else {
            validate_manifest_chain_before_packing(previous_manifest, &manifest).map_err(
                |error| SyncError::Storage(format!("validate backup manifest failed: {error}")),
            )?;
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
        Ok(manifest)
    }

    /// Enumerate and pack the candidates for the held target.
    pub(super) fn publish_target_candidates(
        &self,
        store: &crate::storage::LastStoreNamespacedStore,
        manifest: &mut BackupManifest,
        previous_manifest: Option<&BackupManifest>,
        cloud_presence: Option<&CloudChunkPresence>,
        primary_resume_root: bool,
    ) -> SyncResult<Vec<BackupChunkUploadCandidate>> {
        let candidates = store
            .enumerate_backup_publish_target_candidates()
            .map_err(|e| SyncError::Storage(format!("enumerate backup chunks failed: {e}")))?;
        let candidates = bind_backup_candidates_to_manifest(manifest, candidates);
        let candidates = self.prepare_backup_file_packs(
            store,
            manifest,
            candidates,
            previous_manifest,
            cloud_presence,
            primary_resume_root,
        )?;
        {
            let candidate_shas: std::collections::BTreeSet<_> =
                candidates.iter().map(|c| c.chunk.sha256.clone()).collect();
            let unbackable = unbackable_manifest_chunk_count(manifest, &candidate_shas);
            if unbackable > 0 {
                tracing::warn!(
                    target: "fold_db::sync::backup",
                    generation = manifest.counter,
                    unbackable,
                    "manifest still names chunks outside the local candidate set (kept because still in cloud, or reconcile incomplete)"
                );
            }
        }
        Ok(candidates)
    }

    /// Store the held target in the locked slot and record the cut gap.
    pub(super) fn install_publish_target(
        &self,
        slot: &mut Option<BackupPublishTarget>,
        manifest: BackupManifest,
        candidates: Vec<BackupChunkUploadCandidate>,
    ) {
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
    }
}
