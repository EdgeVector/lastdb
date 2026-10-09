use std::sync::Arc;

use crate::fold_db_core::sync_coordinator::SyncCoordinator;
#[cfg(feature = "cloud-sync")]
use crate::schema::types::field::HashRangeFilter;
#[cfg(feature = "cloud-sync")]
use crate::schema::types::operations::MutationType;
#[cfg(feature = "cloud-sync")]
use crate::schema::types::{Mutation, Query};
#[cfg(feature = "cloud-sync")]
use std::collections::HashMap;

use super::FoldDB;

async fn prepare_photograph_cut_components(
    pending_tasks: &crate::fold_db_core::pending_task_tracker::PendingTaskTracker,
    db_ops: &crate::db_operations::DbOperations,
    #[cfg(feature = "cloud-sync")] capture_router: Option<
        Arc<crate::sync::capture::MutationLogCaptureRouter>,
    >,
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
    #[cfg(feature = "cloud-sync")]
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

impl FoldDB {
    /// Returns a reference to the sync coordinator.
    pub fn sync_coordinator(&self) -> &SyncCoordinator {
        &self.sync_coordinator
    }

    /// Set the sync engine (called by the factory when sync is configured).
    /// Also registers the schema reloader callback so SchemaCore's in-memory
    /// cache is refreshed after sync replays schema entries.
    ///
    /// Registration lives here because it needs access to FoldDB-owned
    /// components (SchemaCore); engine storage is then delegated to the
    /// SyncCoordinator. In-process native index reloading was retired;
    /// Search app owns semantic recall.
    pub async fn set_sync_engine(&self, engine: Arc<crate::sync::SyncEngine>) {
        self.set_sync_engine_with_capture(engine, true).await;
    }

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

    async fn set_sync_engine_with_capture(
        &self,
        engine: Arc<crate::sync::SyncEngine>,
        capture_enabled: bool,
    ) {
        let schema_mgr = Arc::clone(&self.schema_manager);
        engine
            .set_schema_reloader(Arc::new(move || {
                let mgr = Arc::clone(&schema_mgr);
                Box::pin(async move {
                    mgr.reload_from_store()
                        .await
                        .map_err(|e| format!("SchemaCore reload failed: {e}"))
                })
            }))
            .await;

        if capture_enabled {
            if let Some(capture) = &self.mutation_log_capture {
                capture
                    .set_engine_after_mutations(Arc::clone(&engine), || {
                        // New writes must see the atom-retention engine slot
                        // before they can enter the shared serving router.
                        self.mutation_manager
                            .set_capture_engine_metadata(Arc::clone(&engine));
                    })
                    .await;
            } else {
                self.mutation_manager
                    .set_capture_engine(Arc::clone(&engine));
            }
        }
        let db_ops = Arc::clone(&self.db_ops);
        let materializer: crate::sync::engine::MutationIntentMaterializer =
            Arc::new(move |envelopes| {
                let db_ops = Arc::clone(&db_ops);
                Box::pin(async move {
                    crate::sync::mutation_intent::materialize_field_values(
                        envelopes,
                        db_ops.atoms(),
                    )
                    .await
                })
            });
        engine
            .set_mutation_intent_materializer(Arc::clone(&materializer))
            .await;
        engine
            .set_automatic_gc_atom_store(self.db_ops.atoms().clone())
            .await;
        let pending_tasks = Arc::clone(&self.pending_tasks);
        let cut_db_ops = Arc::clone(&self.db_ops);
        let cut_capture_router = self.mutation_log_capture.clone();
        let cut_barrier: crate::sync::engine::PhotographCutBarrier = Arc::new(move || {
            let pending_tasks = Arc::clone(&pending_tasks);
            let cut_db_ops = Arc::clone(&cut_db_ops);
            let cut_capture_router = cut_capture_router.clone();
            Box::pin(async move {
                prepare_photograph_cut_components(
                    &pending_tasks,
                    &cut_db_ops,
                    #[cfg(feature = "cloud-sync")]
                    cut_capture_router.clone(),
                )
                .await
            })
        });
        engine.set_photograph_cut_barrier(cut_barrier).await;
        let restore_db_ops = Arc::clone(&self.db_ops);
        let restore_barrier: crate::sync::engine::PhotographCutBarrier = Arc::new(move || {
            let restore_db_ops = Arc::clone(&restore_db_ops);
            Box::pin(async move {
                restore_db_ops
                    .atoms()
                    .refresh_boot_encoding_after_restore()
                    .await
                    .map_err(|error| error.to_string())
            })
        });
        engine.set_photograph_restore_barrier(restore_barrier).await;
        let mutation_manager = Arc::clone(&self.mutation_manager);
        let applier: crate::sync::engine::MutationIntentApplier = Arc::new(move |envelopes| {
            let mutation_manager = Arc::clone(&mutation_manager);
            let materializer = Arc::clone(&materializer);
            Box::pin(async move {
                let envelopes = materializer(envelopes).await?;
                let (mutations, prefix) =
                    crate::sync::mutation_intent::decode_mutations(&envelopes);
                mutation_manager
                    .apply_replayed_mutations(mutations, prefix.as_deref())
                    .await
                    .map(|_| ())
                    .map_err(|error| error.to_string())
            })
        });
        engine.set_mutation_intent_applier(applier).await;
        self.sync_coordinator.set_engine(engine);
    }

    /// Start the background sync timer. Delegates to the coordinator.
    ///
    /// Automatic gc-atoms is not started here. FoldDB init starts that
    /// local cadence even when no sync engine is configured.
    pub fn start_sync(&self, interval_ms: u64) {
        self.sync_coordinator.start_background_sync(interval_ms);
    }

    /// Force an immediate sync (e.g. on shutdown).
    pub async fn force_sync(&self) -> Result<(), crate::sync::SyncError> {
        self.sync_coordinator.force_sync().await
    }

    /// **Live** cloud snapshot + upload-staging heal (never stops Mini).
    ///
    /// Uploads `latest.enc` from the open store, resets capture watermark, and
    /// clears durable upload staging **only if** the snapshot succeeds.
    /// Concurrent local R/W continues; this is the product path for operators
    /// (sled-era offline exclusive snapshot is gone with the sled engine).
    pub async fn heal_cloud_staging(
        &self,
    ) -> Result<crate::sync::engine::HealStagingReport, crate::error::FoldDbError> {
        let engine = self.sync_engine().ok_or_else(|| {
            crate::error::FoldDbError::Database(
                "cloud sync is not configured (no cloud_sync.json / sync engine)".into(),
            )
        })?;
        engine
            .heal_staging_via_snapshot()
            .await
            .map_err(crate::error::FoldDbError::Database)
    }

    pub async fn laststore_cloud_snapshot(
        &self,
        previous_manifest: Option<&crate::storage::laststore::BackupManifest>,
    ) -> Result<
        (
            crate::storage::laststore::BackupManifest,
            crate::sync::engine::LastStoreCloudSnapshotReport,
        ),
        crate::error::FoldDbError,
    > {
        let engine = self.sync_engine().ok_or_else(|| {
            crate::error::FoldDbError::Database(
                "cloud sync is not configured (no cloud_sync.json / sync engine)".into(),
            )
        })?;
        engine
            .laststore_cloud_snapshot(previous_manifest)
            .await
            .map_err(|e| {
                crate::error::fold_db_error_from_sync("laststore cloud snapshot failed", e)
            })
    }

    /// Product path: GC unreferenced cloud backup chunks (orphan sweep).
    ///
    /// Keeps digests referenced by `live_manifests` (pass current + retained
    /// cuts). `dry_run: true` only reports orphans; `false` presign-DELETEs them.
    pub async fn gc_orphan_backup_chunks(
        &self,
        live_manifests: &[crate::storage::laststore::BackupManifest],
        dry_run: bool,
    ) -> Result<crate::sync::engine::BackupOrphanGcReport, crate::error::FoldDbError> {
        let engine = self.sync_engine().ok_or_else(|| {
            crate::error::FoldDbError::Database(
                "cloud sync is not configured (no cloud_sync.json / sync engine)".into(),
            )
        })?;
        engine
            .gc_orphan_backup_chunks(live_manifests, dry_run)
            .await
            .map_err(|e| crate::error::FoldDbError::Database(e.to_string()))
    }

    /// Product path: read-only R2 prefix/category size inventory for the
    /// connected cloud account. List-only — never deletes, never fetches an
    /// object body. See [`crate::sync::engine::PrefixInventoryReport`].
    pub async fn prefix_inventory(
        &self,
    ) -> Result<crate::sync::engine::PrefixInventoryReport, crate::error::FoldDbError> {
        let engine = self.sync_engine().ok_or_else(|| {
            crate::error::FoldDbError::Database(
                "cloud sync is not configured (no cloud_sync.json / sync engine)".into(),
            )
        })?;
        engine
            .prefix_inventory()
            .await
            .map_err(|e| crate::error::FoldDbError::Database(e.to_string()))
    }

    /// Stop the background sync timer and run a final sync.
    pub async fn stop_sync(&self) -> Result<(), crate::sync::SyncError> {
        self.sync_coordinator.stop().await
    }

    /// Get the sync engine state, if sync is configured.
    pub async fn sync_state(&self) -> Option<crate::sync::SyncState> {
        self.sync_coordinator.state().await
    }

    /// Get a full sync status snapshot, if sync is configured.
    pub async fn sync_status(&self) -> Option<crate::sync::SyncStatus> {
        self.sync_coordinator.status().await
    }

    /// Intentionally pause cloud mutation staging on the live engine (grace clock).
    ///
    /// Does **not** remove credentials; pair with renaming `cloud_sync.json` for
    /// durable reboot pause (`lastdb cloud off`). No-op-ish if no engine.
    pub async fn set_cloud_sync_disabled_live(&self, disabled: bool) -> Result<(), String> {
        let engine = self.sync_engine().ok_or_else(|| {
            "cloud sync engine not running (daemon booted without cloud_sync.json)".to_string()
        })?;
        engine.set_cloud_sync_disabled(disabled).await;
        Ok(())
    }

    /// Re-enable after intentional pause: pull→snapshot when past grace, then resume staging.
    pub async fn reenable_cloud_sync_live(
        &self,
    ) -> Result<crate::sync::CloudSyncReenableOutcome, String> {
        let engine = self.sync_engine().ok_or_else(|| {
            "cloud sync engine not running (daemon booted without cloud_sync.json)".to_string()
        })?;
        Ok(engine.reenable_cloud_sync().await)
    }

    /// Last-fire automatic plane-compaction reports, on any node shape.
    ///
    /// `sync_status()` is `None` without `cloud_sync.json`, so reading these
    /// off the sync snapshot hid the whole cadence on exactly the node that
    /// most needs to prove it runs. This reads the compactor the local cadence
    /// steps, or the engine's when cloud sync is on — the same object either
    /// way. `None` means no cadence started in this process.
    pub async fn automatic_compaction_status(
        &self,
    ) -> Option<std::collections::BTreeMap<String, crate::sync::engine::AutomaticCompactionStatus>>
    {
        self.sync_coordinator.automatic_compaction_status().await
    }

    /// Returns true if the sync engine is configured.
    pub fn is_sync_enabled(&self) -> bool {
        self.sync_coordinator.is_enabled()
    }

    /// Returns a clone of the sync engine Arc, if configured.
    ///
    /// Used by callers to configure scoped sync targets after startup.
    pub fn sync_engine(&self) -> Option<Arc<crate::sync::SyncEngine>> {
        self.sync_coordinator.engine()
    }

    /// True while a backup cut is held in-process and must not be deleted out
    /// from under. Local-only boots with no sync engine are never held.
    pub async fn backup_publish_target_is_held(&self) -> bool {
        match self.sync_engine() {
            Some(engine) => engine.has_backup_publish_target().await,
            None => false,
        }
    }

    /// Run `op` only when no backup cut is held, keeping the photograph packing
    /// lock across `op` so a cut cannot start mid-rewrite.
    ///
    /// Returns `None` when a cut is already in the slot (caller must defer).
    /// Local-only boots with no sync engine always run `op`.
    pub async fn run_unless_backup_publish_target_held<T, F, Fut>(&self, op: F) -> Option<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        match self.sync_engine() {
            Some(engine) => {
                let guard = engine.lock_backup_publish_target().await;
                if guard.is_some() {
                    None
                } else {
                    let result = op().await;
                    drop(guard);
                    Some(result)
                }
            }
            None => Some(op().await),
        }
    }

    /// Register an org cloud-sync target (org_hash prefix + shared E2E key).
    ///
    /// Persists to the primary storage backend and reconfigures the live sync
    /// engine when cloud sync is enabled. When sync is dormant (no
    /// `cloud_sync.json`), the target is still stored so the next boot with
    /// sync can pick it up.
    ///
    /// `e2e_key_b64` is the base64 32-byte org AES key (from LastSecrets).
    pub async fn register_org_sync_target(
        &self,
        org_hash: &str,
        e2e_key_b64: &str,
        slug: &str,
    ) -> Result<crate::sharing::OrgSyncTarget, crate::error::FoldDbError> {
        let target = crate::sharing::upsert_org_sync_target_in_ops(
            self.db_ops(),
            org_hash,
            e2e_key_b64,
            slug,
        )
        .await?;
        self.apply_org_sync_targets_from_store().await?;
        Ok(target)
    }

    /// Register an org cloud target for one explicit local database prefix.
    pub async fn register_org_sync_target_for_storage_prefix(
        &self,
        org_hash: &str,
        storage_prefix: &str,
        e2e_key_b64: &str,
        slug: &str,
    ) -> Result<crate::sharing::OrgSyncTarget, crate::error::FoldDbError> {
        let target = crate::sharing::upsert_org_sync_target_for_storage_prefix_in_ops(
            self.db_ops(),
            org_hash,
            storage_prefix,
            e2e_key_b64,
            slug,
        )
        .await?;
        self.apply_org_sync_targets_from_store().await?;
        Ok(target)
    }

    /// Register one schema from the legacy unprefixed personal instance.
    pub async fn register_org_sync_target_for_storage_prefix_and_schema(
        &self,
        org_hash: &str,
        storage_prefix: &str,
        schema_name: &str,
        e2e_key_b64: &str,
        slug: &str,
    ) -> Result<crate::sharing::OrgSyncTarget, crate::error::FoldDbError> {
        let target = crate::sharing::upsert_org_sync_target_for_storage_prefix_and_schema_in_ops(
            self.db_ops(),
            org_hash,
            storage_prefix,
            schema_name,
            e2e_key_b64,
            slug,
        )
        .await?;
        self.apply_org_sync_targets_from_store().await?;
        Ok(target)
    }

    /// Deactivate the active org sync targets that match `org_hash` and/or
    /// `slug`, then rebuild the sync engine target set so the next cycle no
    /// longer pulls from or pushes to them. Local only: no cloud registry
    /// call. Re-arm with `register_org_sync_target*` for the same org.
    pub async fn deactivate_org_sync_targets(
        &self,
        org_hash: Option<&str>,
        slug: Option<&str>,
        dry_run: bool,
    ) -> Result<Vec<crate::sharing::OrgSyncTarget>, crate::error::FoldDbError> {
        let changed = crate::sharing::deactivate_org_sync_targets_matching_in_ops(
            self.db_ops(),
            org_hash,
            slug,
            dry_run,
        )
        .await?;
        if !dry_run && !changed.is_empty() {
            self.apply_org_sync_targets_from_store().await?;
        }
        Ok(changed)
    }

    /// List registered org cloud-sync targets.
    pub async fn list_org_sync_targets(
        &self,
    ) -> Result<Vec<crate::sharing::OrgSyncTarget>, crate::error::FoldDbError> {
        crate::sharing::list_org_sync_targets_in_ops(self.db_ops()).await
    }

    /// After a catalog share, route the source instance to org heads that
    /// already track the target locator prefix.
    pub async fn attach_shared_instance_to_org_targets(
        &self,
        target_db_locator: &str,
        schema_name: &str,
        instance_id: Option<&str>,
    ) -> Result<(), crate::error::FoldDbError> {
        let instance_id = instance_id.unwrap_or(crate::db_operations::UNPREFIXED_INSTANCE_ID);
        let parsed = crate::access::parse_db_locator(target_db_locator)
            .map_err(crate::error::FoldDbError::Config)?;
        let Some(target_prefix) = crate::access::storage_prefix_for(&parsed) else {
            return Ok(());
        };
        let targets = crate::sharing::list_org_sync_targets_in_ops(self.db_ops()).await?;
        for target in targets.into_iter().filter(|t| t.active) {
            if !target.storage_prefixes.contains(&target_prefix)
                && !target.storage_prefixes.iter().any(|p| p == instance_id)
            {
                continue;
            }
            self.register_org_sync_target_for_storage_prefix_and_schema(
                &target.org_hash,
                instance_id,
                schema_name,
                &target.e2e_key_b64,
                &target.slug,
            )
            .await?;
        }
        Ok(())
    }

    /// Publish the live rows of a legacy personal schema as self-contained
    /// mutation intents after a personal-to-org share.
    ///
    /// The local molecule and atom planes stay untouched. The org target gets
    /// current row bodies, so a friend can restore the schema without a later
    /// owner write. Exact key reads keep this path within the LastDB access
    /// model; the membership walk only returns keys.
    #[cfg(feature = "cloud-sync")]
    pub(crate) async fn republish_personal_schema_to_org_targets(
        &self,
        schema_name: &str,
    ) -> Result<usize, crate::error::FoldDbError> {
        let Some(engine) = self.sync_engine() else {
            return Ok(0);
        };
        let schema = self
            .schema_manager()
            .get_schema_metadata(schema_name)?
            .ok_or_else(|| crate::schema::SchemaError::NotFound(schema_name.to_string()))?;
        let fields: Vec<String> = schema.runtime_fields.keys().cloned().collect();
        let Some(key) = schema.key.as_ref() else {
            return Ok(0);
        };
        let has_hash = key
            .hash_field
            .as_ref()
            .is_some_and(|field| !field.is_empty());
        let has_range = key
            .range_field
            .as_ref()
            .is_some_and(|field| !field.is_empty());
        if !has_hash && !has_range {
            return Ok(0);
        }

        const KEY_PAGE_SIZE: usize = 128;
        const MUTATION_BATCH_SIZE: usize = 32;
        let mut cursor = None;
        let mut republished_rows = 0;
        loop {
            let page = self
                .list_schema_record_keys(
                    schema_name,
                    schema_name,
                    KEY_PAGE_SIZE,
                    cursor.as_deref(),
                    None,
                )
                .await?;
            let mut batch = Vec::with_capacity(MUTATION_BATCH_SIZE);
            for record_key in page.keys {
                let filter = match (has_hash, has_range) {
                    (true, true) => HashRangeFilter::HashRangeKey {
                        hash: record_key.hash.clone(),
                        range: record_key.range.clone(),
                    },
                    (true, false) => HashRangeFilter::HashKey(record_key.hash.clone()),
                    (false, true) => HashRangeFilter::RangeKey(record_key.range.clone()),
                    (false, false) => continue,
                };
                let rows = self
                    .query_executor()
                    .query(Query::new_with_filter(
                        schema_name.to_string(),
                        fields.clone(),
                        Some(filter),
                    ))
                    .await?;
                let mut by_key: HashMap<crate::schema::types::KeyValue, HashMap<String, _>> =
                    HashMap::new();
                for (field, values) in rows {
                    for (key_value, field_value) in values {
                        by_key
                            .entry(key_value)
                            .or_default()
                            .insert(field.clone(), field_value);
                    }
                }
                for (key_value, values) in by_key {
                    let fields_and_values = values
                        .iter()
                        .map(|(field, value)| (field.clone(), value.value.clone()))
                        .collect();
                    let pub_key = values
                        .values()
                        .find_map(|value| value.writer_pubkey.clone())
                        .unwrap_or_default();
                    let imported_written_at =
                        values.values().filter_map(|value| value.written_at).max();
                    let source_file_name = values
                        .values()
                        .find_map(|value| value.source_file_name.clone());
                    let metadata = values.values().find_map(|value| value.metadata.clone());
                    let mut mutation = Mutation::new(
                        schema_name.to_string(),
                        fields_and_values,
                        key_value,
                        pub_key,
                        MutationType::Update,
                    );
                    mutation.imported_written_at = imported_written_at;
                    mutation.source_file_name = source_file_name;
                    mutation.metadata = metadata;
                    batch.push(mutation);
                    if batch.len() == MUTATION_BATCH_SIZE {
                        let envelopes =
                            crate::sync::mutation_intent::encode_mutations(&batch, None);
                        engine
                            .record_op(crate::sync::mutation_intent::mutation_intent_op(envelopes))
                            .await
                            .map_err(crate::error::FoldDbError::Database)?;
                        republished_rows += batch.len();
                        batch.clear();
                    }
                }
            }
            if !batch.is_empty() {
                let count = batch.len();
                let envelopes = crate::sync::mutation_intent::encode_mutations(&batch, None);
                engine
                    .record_op(crate::sync::mutation_intent::mutation_intent_op(envelopes))
                    .await
                    .map_err(crate::error::FoldDbError::Database)?;
                republished_rows += count;
            }
            if !page.has_more {
                break;
            }
            cursor = page.next_cursor;
        }
        Ok(republished_rows)
    }

    /// Rebuild SyncEngine extra targets from the org sync registry (+ active share rules).
    ///
    /// No-op when sync is not configured. Safe to call after every register.
    pub async fn apply_org_sync_targets_from_store(&self) -> Result<(), crate::error::FoldDbError> {
        let Some(engine) = self.sync_engine() else {
            return Ok(());
        };
        let org_targets =
            crate::sharing::list_active_org_sync_targets_in_ops(self.db_ops()).await?;
        let share_rules = crate::sharing::store::list_share_rules_in_ops(self.db_ops())
            .await
            .unwrap_or_default();
        let partitioner = crate::sync::SyncPartitioner::new_with_orgs(&share_rules, &org_targets);

        let mut extra = Vec::new();
        let mut restore_scopes = std::collections::HashMap::new();
        for org in &org_targets {
            let key = crate::sharing::e2e_key_bytes(org)?;
            restore_scopes.insert(
                org.org_hash.clone(),
                if org.storage_prefixes.is_empty() {
                    vec![org.org_hash.clone()]
                } else {
                    org.storage_prefixes.clone()
                },
            );
            extra.push(crate::sync::SyncTarget {
                label: if org.slug.is_empty() {
                    format!("org:{}", &org.org_hash[..8.min(org.org_hash.len())])
                } else {
                    format!("org:{}", org.slug)
                },
                prefix: org.org_hash.clone(),
                crypto: Arc::new(crate::crypto::LocalCryptoProvider::from_key(key)),
            });
        }
        // Outbound share rules as upload targets (existing cross-user shares).
        for rule in share_rules.iter().filter(|r| r.active) {
            if rule.share_e2e_secret.len() != 32 {
                continue;
            }
            let mut key = [0u8; 32];
            key.copy_from_slice(&rule.share_e2e_secret);
            restore_scopes.insert(rule.share_prefix.clone(), vec![rule.share_prefix.clone()]);
            extra.push(crate::sync::SyncTarget {
                label: format!("share:{}", rule.rule_id),
                prefix: rule.share_prefix.clone(),
                crypto: Arc::new(crate::crypto::LocalCryptoProvider::from_key(key)),
            });
        }

        engine
            .configure_targets_with_restore_scopes(partitioner, extra, restore_scopes)
            .await;
        Ok(())
    }

    /// Start the sync engine on an existing FoldDB instance at runtime.
    /// Called when cloud credentials are written and sync needs to activate
    /// without a full process restart.
    pub async fn start_sync_engine_runtime(
        &self,
        api_url: &str,
        api_key: &str,
        data_dir: &str,
        e2e_keys: &crate::crypto::E2eKeys,
        auth_refresh: Option<crate::sync::AuthRefreshCallback>,
    ) -> crate::error::FoldDbResult<()> {
        if self.sync_coordinator.is_enabled() {
            return Ok(()); // already running
        }
        if self.mutation_log_capture.is_none() {
            return Err(crate::error::FoldDbError::Config(
                "runtime cloud sync requires a serving capture router".to_string(),
            ));
        }

        let mut setup = crate::sync::SyncSetup::from_exemem(api_url, api_key, data_dir);
        setup.auth_refresh = auth_refresh.clone();

        let sync_config = setup.config.unwrap_or_default();
        let interval_ms = sync_config.sync_interval_ms;

        let sync_crypto: Arc<dyn crate::crypto::CryptoProvider> = Arc::new(
            crate::crypto::LocalCryptoProvider::from_key(e2e_keys.encryption_key()),
        );

        // Reuse the already-open namespaced store from DbOperations (Last Store).
        let base_store: Arc<dyn crate::storage::traits::NamespacedStore> =
            self.db_ops.namespaced_store();

        // trace-egress: propagate (shared with skip-s3 — see docs/observability/egress-classification-notes.md)
        // Connect-timeout the shared client so a black-holed endpoint can't
        // wedge the TCP handshake; per-request bounds live in S3Client/AuthClient.
        let http = Arc::new(crate::sync::build_shared_http_client());
        let s3 = crate::sync::s3::S3Client::new(http.clone());
        let auth_client = crate::sync::auth::AuthClient::new(http, setup.auth_url, setup.auth);

        let (backup_publish_target, cloud_sync_disabled_at) = self.sync_coordinator.packing_slots();
        let mut engine = crate::sync::SyncEngine::new_with_shared_packing(
            setup.device_id,
            sync_crypto,
            s3,
            auth_client,
            base_store,
            sync_config,
            Arc::clone(&self.signer),
            None,
            Some(backup_publish_target),
            Some(cloud_sync_disabled_at),
        );
        if let Some(cb) = auth_refresh {
            engine.set_auth_refresh(cb);
        }
        let engine = Arc::new(engine);

        self.set_sync_engine(engine).await;
        self.start_sync(interval_ms);

        Ok(())
    }
}
