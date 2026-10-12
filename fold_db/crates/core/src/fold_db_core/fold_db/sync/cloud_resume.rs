use super::super::FoldDB;

impl FoldDB {
    /// Inspect the old cloud logs, then cut the live store under the mutation
    /// fence. The caller must store this intent durably before upload.
    pub async fn prepare_primary_authoritative_cloud_resume(
        &self,
        fresh_from_local: bool,
        accept_local_damage: bool,
    ) -> Result<crate::sync::engine::PrimaryResumeCut, String> {
        let engine = self
            .sync_engine()
            .ok_or_else(|| "primary resume requires a paused sync engine".to_string())?;
        let router = self
            .mutation_log_capture
            .as_ref()
            .ok_or_else(|| "primary resume requires a serving capture router".to_string())?;
        if !engine.is_backup_only_mode() {
            return Err("primary resume requires backup-only mode".into());
        }
        if fresh_from_local {
            engine
                .register_fresh_primary_resume_owner()
                .await
                .map_err(|error| error.to_string())?;
        }
        let before = engine
            .inspect_primary_resume_logs()
            .await
            .map_err(|error| error.to_string())?;
        if fresh_from_local && before.cloud_log_objects != 0 {
            return Err("fresh cloud backup requires an empty cloud log".into());
        }
        // The backup-only boot already attached capture and its cut barrier.
        // No cloud worker exists until the separate finish action succeeds.
        let (writer_frontier, manifest) = engine
            .prepare_primary_resume_snapshot_cut(
                router,
                before.cloud_writer_frontier,
                fresh_from_local,
                accept_local_damage,
            )
            .await
            .map_err(|error| error.to_string())?;
        Ok(crate::sync::engine::PrimaryResumeCut {
            before,
            writer_frontier,
            manifest,
        })
    }

    /// Publish the exact saved cut. A failed upload leaves the durable resume
    /// marker in place, so a later job can retry from its recorded intent.
    pub async fn publish_primary_authoritative_cloud_resume(
        &self,
        cut: &crate::sync::engine::PrimaryResumeCut,
    ) -> Result<
        (
            crate::sync::engine::LastStoreCloudSnapshotReport,
            crate::sync::engine::PrimaryResumeLogInventory,
        ),
        String,
    > {
        let engine = self
            .sync_engine()
            .ok_or_else(|| "primary resume requires a sync engine".to_string())?;
        let report = engine
            .reconcile_primary_resume_snapshot(&cut.manifest)
            .await
            .map_err(|error| error.to_string())?;
        if report.manifest_sha256 != cut.manifest.manifest_sha256
            || report.counter != cut.manifest.counter
        {
            return Err("primary resume published a different cut than its durable intent".into());
        }
        let after = engine
            .confirm_primary_resume_snapshot(&cut.before, cut.writer_frontier)
            .await
            .map_err(|error| error.to_string())?;
        Ok((report, after))
    }

    /// Recover an exact CAS result after a process restart. The boot already
    /// attached local capture but kept every cloud worker Off.
    pub async fn recover_primary_authoritative_cloud_resume(
        &self,
        cut: &crate::sync::engine::PrimaryResumeCut,
    ) -> Result<crate::sync::engine::PrimaryResumeLogInventory, String> {
        let engine = self
            .sync_engine()
            .ok_or_else(|| "primary resume requires a sync engine".to_string())?;
        engine
            .recover_primary_resume_snapshot(cut)
            .await
            .map_err(|error| error.to_string())
    }

    /// Start normal workers after the node commits cloud-on intent and clears
    /// the durable resume marker.
    pub async fn finish_primary_authoritative_cloud_resume(&self) -> Result<(), String> {
        let engine = self
            .sync_engine()
            .ok_or_else(|| "primary resume has no sync engine".to_string())?;
        engine.finish_primary_resume().await;
        engine.start_laststore_backup_uploader();
        self.start_sync(engine.config.sync_interval_ms);
        Ok(())
    }
}
