//! S0 install and mutation-log tail replay.

use super::*;

use fold_db::sync::engine::{BackupRestoreMode, LastStoreCloudRestoreReport, RestoreProgress};

/// Everything the async install and replay steps read.
pub(crate) struct Apply<'a> {
    pub(crate) flavor: &'a Flavor<'a>,
    pub(crate) homes: &'a Homes,
    pub(crate) source: &'a Source,
    pub(crate) remote_descriptor: Option<&'a RemoteRecovery>,
    pub(crate) source_db_hash: &'a str,
    pub(crate) identity: &'a Identity,
    pub(crate) clients: &'a CloudClients,
    pub(crate) store: &'a std::sync::Arc<fold_db::storage::LastStoreNamespacedStore>,
    pub(crate) engine: &'a Engine,
    pub(crate) data_path: &'a Path,
    pub(crate) progress: Option<&'a RestoreProgress>,
}

pub(crate) struct Applied {
    pub(crate) report: LastStoreCloudRestoreReport,
    pub(crate) mode: BackupRestoreMode,
}

impl Apply<'_> {
    /// Install S0, then replay the mutation-log tail when the marker asks for it.
    pub(crate) fn run(self, resume_report: Option<LastStoreCloudRestoreReport>) -> Phase<Applied> {
        self.clients.runtime.block_on(async {
            // Restore calls explicit cloud read methods. Arm the upload interlock
            // before the first one. The later FoldDB shutdown still runs its normal
            // final sync, but that cycle exits before lock, register, PUT, CAS, or
            // DELETE. Normal daemon engines keep their default Cloud On state.
            self.engine.engine.set_cloud_sync_disabled(true).await;
            self.verify_remote_pointers().await?;
            self.verify_paused_latest().await?;
            let mut report = self.install_s0(resume_report).await?;
            self.verify_installed_scope(&report)?;
            let (frontier, stored_mode) = self
                .engine
                .engine
                .restored_backup_marker_and_frontier()
                .await
                .map_err(|e| {
                    RestoreFailure::from_sync(
                        Stage::RestoreTail,
                        "read restored snapshot writer frontier and restore mode",
                        &e,
                    )
                })?;
            report.mutation_log_snapshot_frontier = Some(frontier.clone());
            let mode = self.resolve_mode(stored_mode, &report)?;
            if mode == BackupRestoreMode::S0Only {
                report.remote_read_only = true;
                return Ok(Applied { report, mode });
            }
            self.check_replay_allowed()?;
            self.replay_tail(&mut report, &frontier).await?;
            report.remote_read_only = true;
            Ok(Applied { report, mode })
        })
    }

    async fn verify_remote_pointers(&self) -> Phase<()> {
        let Some(recovery) = self.remote_descriptor else {
            return Ok(());
        };
        let auth = &self.clients.auth;
        if let Some(expected) = &recovery.latest {
            let current = auth.backup_latest_get().await.map_err(|error| {
                RestoreFailure::from_sync(
                    Stage::RestoreS0LatestPointer,
                    "read normal latest before restore",
                    &error,
                )
            })?;
            if current.latest != expected.latest || current.key != expected.key {
                return Err(scope_failure("normal backup latest changed before restore"));
            }
        }
        if let Some(expected_rescue) = &recovery.rescue {
            let unscoped = auth.clone().with_db_hash(None).without_db_auto_claim();
            let rescue = unscoped
                .rescue_s0_get(&expected_rescue.manifest_sha256)
                .await
                .map_err(|error| {
                    RestoreFailure::from_sync(
                        Stage::RestoreS0LatestPointer,
                        "read S0 rescue cut before restore",
                        &error,
                    )
                })?;
            if rescue != *expected_rescue {
                return Err(scope_failure("S0 rescue cut changed before restore"));
            }
        }
        Ok(())
    }

    async fn verify_paused_latest(&self) -> Phase<()> {
        let Some(receipt) = &self.source.paused_receipt else {
            return Ok(());
        };
        let latest = self
            .clients
            .auth
            .backup_latest_get()
            .await
            .map_err(|error| {
                RestoreFailure::from_sync(
                    Stage::SourceScope,
                    "read latest backup for paused source",
                    &error,
                )
            })?;
        if latest.latest.manifest_sha256 != receipt.manifest_sha256
            || latest.latest.counter != receipt.manifest_counter
        {
            return Err(scope_failure(
                "paused source receipt does not match the latest cloud backup",
            ));
        }
        Ok(())
    }

    async fn install_s0(
        &self,
        resume_report: Option<LastStoreCloudRestoreReport>,
    ) -> Phase<LastStoreCloudRestoreReport> {
        if let Some(report) = resume_report {
            self.store.verify_integrity().map_err(|error| {
                preflight_failure(format!(
                    "restore checkpoint integrity check failed: {error}"
                ))
            })?;
            return Ok(report);
        }
        let auth = &self.clients.auth;
        let s3 = &self.clients.s3;
        let store = self.store.as_ref();
        let progress = self.progress;
        let report = if let Some(recovery) = self.remote_descriptor {
            if let Some(rescue) = &recovery.rescue {
                fold_db::sync::engine::restore_laststore_cloud_backup_from_rescue_with_cache(
                    auth, s3, store, rescue, progress,
                )
                .await
            } else {
                fold_db::sync::engine::restore_laststore_cloud_backup_from_latest_pointer(
                    auth,
                    s3,
                    store,
                    recovery.latest.as_ref().expect("normal latest was checked"),
                    progress,
                )
                .await
            }
        } else {
            fold_db::sync::engine::restore_laststore_cloud_backup_with_cache(
                auth,
                s3,
                store,
                progress,
                self.homes.cache.as_ref(),
            )
            .await
        }
        .map_err(|e| RestoreFailure::from_s0("restore LastStore S0 backup", &e))?;
        if !self.flavor.remote_only() {
            let files = restore_checkpoint::capture_files(
                &self.homes.target,
                store,
                report.chunks_installed,
            )
            .map_err(|detail| io_failure(Stage::CompletionMarker, detail))?;
            restore_checkpoint::save(&self.homes.target, self.source_db_hash, &report, files)
                .map_err(|detail| io_failure(Stage::CompletionMarker, detail))?;
        }
        Ok(report)
    }

    fn verify_installed_scope(&self, report: &LastStoreCloudRestoreReport) -> Phase<()> {
        if let Some(recovery) = self.remote_descriptor {
            if report.manifest_sha256 != recovery.descriptor.manifest_sha256
                || report.counter != recovery.descriptor.counter
            {
                return Err(scope_failure(
                    "installed backup does not match the recovery descriptor",
                ));
            }
        }
        if let Some(receipt) = &self.source.paused_receipt {
            if report.manifest_sha256 != receipt.manifest_sha256
                || report.counter != receipt.manifest_counter
            {
                return Err(scope_failure(
                    "installed backup does not match the paused source receipt",
                ));
            }
        }
        Ok(())
    }

    fn resolve_mode(
        &self,
        stored_mode: Option<BackupRestoreMode>,
        report: &LastStoreCloudRestoreReport,
    ) -> Phase<BackupRestoreMode> {
        if self.flavor.remote_latest && stored_mode != Some(BackupRestoreMode::ReplayTail) {
            return Err(tail_failure(
                "normal remote backup needs the authenticated replay-tail restore marker",
            ));
        }
        // An offline rescue published before the in-store marker existed still
        // has an authenticated S0-only descriptor and an exact immutable root
        // pointer. The remote restore has already verified all chunks and
        // committed the destination. Only an absent marker may use that proof.
        Ok(match stored_mode {
            Some(mode) => mode,
            None if self.flavor.remote_s0_only
                && self.remote_descriptor.is_some()
                && report.source_scope_verified
                && report.manifests_walked == 1 =>
            {
                BackupRestoreMode::S0Only
            }
            None => BackupRestoreMode::ReplayTail,
        })
    }

    fn check_replay_allowed(&self) -> Phase<()> {
        if self.flavor.remote_s0_only {
            return Err(tail_failure(
                "remote recovery requires the authenticated v2 S0-only restore marker",
            ));
        }
        if self.source.paused_source && !self.flavor.remote_latest {
            return Err(tail_failure(
                "paused source backup lacks the S0-only restore marker",
            ));
        }
        Ok(())
    }

    async fn replay_tail(
        &self,
        report: &mut LastStoreCloudRestoreReport,
        frontier: &fold_db::sync::snapshot_log::Frontier,
    ) -> Phase<()> {
        // Restore is a one-shot CLI. Plist LASTDB_RESIDENT_MODE=write parks
        // replayed molecules in the resident graph; without a long-lived
        // persist worker they never land in atoms/tips (2026-08-21: 715
        // records_applied, dest HashRangeRange empty, only sync_pin_log
        // files were new). Force LastStore puts + per-batch flush.
        std::env::set_var("LASTDB_RESIDENT_MODE", "off");
        std::env::set_var("LASTDB_MUTATION_SYNC_FLUSH", "1");
        if let Some(progress) = self.progress {
            progress.phase(fold_db::sync::engine::RestorePhase::OpenDatabase);
        }
        let data_path_str = self.data_path.to_str().ok_or_else(|| {
            runtime_failure(format!(
                "restore data path is not UTF-8: {}",
                self.data_path.display()
            ))
        })?;
        let namespaced: std::sync::Arc<dyn fold_db::storage::NamespacedStore> =
            std::sync::Arc::clone(self.store) as _;
        let fold_db = fold_db::FoldDB::for_restore(
            namespaced,
            data_path_str,
            std::sync::Arc::clone(&self.engine.signer),
            &self.identity.e2e,
        )
        .await
        .map_err(|e| {
            RestoreFailure::new(
                Stage::OpenRestoreDatabase,
                Code::OperationFailed,
                format!("FoldDB for restore apply: {e}"),
            )
        })?;
        fold_db
            .set_sync_engine(std::sync::Arc::clone(&self.engine.engine))
            .await;
        let replay = self
            .engine
            .engine
            .restore_mutation_log_after_s0_with_progress(frontier, self.progress)
            .await
            .map_err(|e| {
                RestoreFailure::from_sync(Stage::RestoreTail, "restore mutation log after S0", &e)
            })?;
        report.mutation_log_replay = Some(replay);
        // Memory-first mutations + resident persist sit in RAM until shutdown.
        if let Some(progress) = self.progress {
            progress.phase(fold_db::sync::engine::RestorePhase::Flush);
        }
        fold_db.shutdown().await.map_err(|e| {
            RestoreFailure::new(
                Stage::FlushDestination,
                Code::OperationFailed,
                format!("shutdown dest after mutation-log apply: {e}"),
            )
        })?;
        Ok(())
    }
}
