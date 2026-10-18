//! Source-only engine uses the serving store without enabling cloud workers.

use super::*;

impl FoldDB {
    /// Build a source-only engine over the already-open serving store while
    /// Cloud Sync is off. No engine slot, capture route, or worker is replaced.
    pub async fn cloud_purge_source_engine(
        &self,
        api_url: &str,
        api_key: &str,
        data_dir: &str,
        e2e_keys: &crate::crypto::E2eKeys,
    ) -> crate::error::FoldDbResult<Arc<crate::sync::SyncEngine>> {
        self.require_cloud_sync_supported()
            .map_err(crate::error::FoldDbError::Config)?;
        if let Some(engine) = self.sync_engine() {
            return Ok(engine);
        }
        let serving = self.db_ops.namespaced_store();
        let source = serving.laststore_backup_source().ok_or_else(|| {
            crate::error::FoldDbError::Config(
                "cloud purge source requires the serving LastStore backup facade".into(),
            )
        })?;
        let router = self.mutation_log_capture.as_ref().cloned().ok_or_else(|| {
            crate::error::FoldDbError::Config(
                "cloud purge source requires the serving mutation fence".into(),
            )
        })?;
        let setup = crate::sync::SyncSetup::from_exemem(api_url, api_key, data_dir);
        let mut config = setup.config.unwrap_or_default();
        config.legacy_personal_cloud_sync = false;
        config.capture_mode = crate::sync::engine::CaptureMode::MutationLog;
        let http = Arc::new(crate::sync::build_shared_http_client().map_err(|_| {
            crate::error::FoldDbError::Config("cloud HTTPS transport unavailable".into())
        })?);
        let s3 = crate::sync::s3::S3Client::new(Arc::clone(&http));
        let db_hash = source
            .cloud_db_hash()
            .map_err(|error| crate::error::FoldDbError::Database(error.to_string()))?;
        let auth = crate::sync::auth::AuthClient::new(http, setup.auth_url, setup.auth)
            .with_db_hash(db_hash);
        let (packing, disabled_at) = self.sync_coordinator.packing_slots();
        let raw: Arc<dyn crate::storage::traits::NamespacedStore> = source.clone();
        let mut engine = crate::sync::SyncEngine::new_with_shared_packing(
            setup.device_id,
            Arc::new(crate::crypto::LocalCryptoProvider::from_key(
                e2e_keys.encryption_key(),
            )),
            s3,
            auth,
            serving,
            config,
            Arc::clone(&self.signer),
            Some(source),
            Some(packing),
            Some(disabled_at),
        );
        engine.set_at_rest_key(e2e_keys.encryption_key());
        engine.set_cursor_store(raw);
        engine.set_backup_only_mode().await;
        engine
            .set_photograph_mutation_router(Arc::clone(&router))
            .await;
        let pending = Arc::clone(&self.pending_tasks);
        let db_ops = Arc::clone(&self.db_ops);
        engine
            .set_photograph_cut_barrier(Arc::new(move || {
                let pending = Arc::clone(&pending);
                let db_ops = Arc::clone(&db_ops);
                let router = Arc::clone(&router);
                Box::pin(async move {
                    prepare_photograph_cut_components(&pending, &db_ops, Some(router)).await
                })
            }))
            .await;
        Ok(Arc::new(engine))
    }
}

pub(super) async fn prepare_photograph_cut_components(
    pending_tasks: &crate::fold_db_core::pending_task_tracker::PendingTaskTracker,
    db_ops: &crate::db_operations::DbOperations,
    capture_router: Option<Arc<crate::sync::capture::MutationLogCaptureRouter>>,
) -> Result<(), String> {
    if !pending_tasks
        .wait_for_completion(std::time::Duration::from_secs(30))
        .await
    {
        return Err(format!(
            "photograph cut barrier timed out with {} local task(s) still pending",
            pending_tasks.count()
        ));
    }
    // Each accepted mutation owns a pending task until its schema lane makes
    // the complete envelope durable. The wait above is the photograph cut.
    if let Some(router) = capture_router {
        if !router
            .wait_for_completion(std::time::Duration::from_secs(30))
            .await
        {
            return Err("photograph cut barrier timed out with local capture work pending".into());
        }
    }
    db_ops
        .flush()
        .await
        .map_err(|error| format!("photograph storage flush failed: {error}"))
}
