use super::*;

mod landed;
mod publish;
use publish::CasRetry;
mod shortfall;

impl SyncEngine {
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
        self.cas_ensure_cut_verified(allow_retirement, previous_manifest)
            .await?;

        // Re-read the held target after a possible mid-CAS retirement heal so
        // the bytes we publish match the (possibly narrowed) cut.
        let (manifest, chunks_in_target) = {
            let target = self.backup_publish_target.lock().await;
            let target = target
                .as_ref()
                .ok_or_else(missing_backup_publish_target_for_cas)?;
            (target.manifest.clone(), target.total())
        };

        let (manifest_sha256, manifest_key) = self
            .cas_upload_final_manifest(&manifest, previous_manifest, expected_primary_resume_cut)
            .await?;
        self.cas_publish_recovery_descriptor(store, &manifest, &manifest_sha256)
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
        let latest_key = match self
            .cas_resolve_latest_outcome(
                cas,
                store,
                CasRetry {
                    allow_rebind_retry,
                    allow_retirement,
                },
                previous_manifest,
                &manifest,
                &manifest_sha256,
            )
            .await?
        {
            Ok(key) => key,
            Err(recut) => return Ok(recut),
        };

        let gc_eligible_log_segments = self
            .cas_finalize_landed(store, &manifest, &manifest_sha256)
            .await?;

        let report = Self::cas_build_report(
            &manifest,
            manifest_sha256,
            manifest_key,
            latest_key,
            chunks_in_target,
            gc_eligible_log_segments,
        );
        Ok((manifest, report))
    }
}
