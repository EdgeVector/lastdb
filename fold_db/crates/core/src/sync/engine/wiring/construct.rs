//! SyncEngine constructors.

use super::*;

impl SyncEngine {
    pub fn new(
        device_id: String,
        crypto: Arc<dyn CryptoProvider>,
        s3: S3Client,
        auth: AuthClient,
        store: Arc<dyn NamespacedStore>,
        config: SyncConfig,
        node_signer: Arc<Ed25519KeyPair>,
    ) -> Self {
        Self::new_with_laststore_backup_source(
            device_id,
            crypto,
            s3,
            auth,
            store,
            config,
            node_signer,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_laststore_backup_source(
        device_id: String,
        crypto: Arc<dyn CryptoProvider>,
        s3: S3Client,
        auth: AuthClient,
        store: Arc<dyn NamespacedStore>,
        config: SyncConfig,
        node_signer: Arc<Ed25519KeyPair>,
        laststore_backup_source: Option<Arc<crate::storage::LastStoreNamespacedStore>>,
    ) -> Self {
        Self::new_with_shared_packing(
            device_id,
            crypto,
            s3,
            auth,
            store,
            config,
            node_signer,
            laststore_backup_source,
            None,
            None,
        )
    }

    /// Same as [`Self::new_with_laststore_backup_source`], but the packing lock
    /// and cloud-pause slot come from [`crate::fold_db_core::sync_coordinator::SyncCoordinator`].
    ///
    /// Local plane compaction starts before an engine exists. If `cloud on`
    /// later installs an engine with a *new* mutex, an in-flight local rewrite
    /// and a live cut do not serialize. Pass the coordinator's slots here so
    /// there is one packing lock for the process.
    #[allow(clippy::too_many_arguments)]
    // lint:fn-size-ok verbatim move from wiring.rs; splitting this function is separate work
    pub(crate) fn new_with_shared_packing(
        device_id: String,
        crypto: Arc<dyn CryptoProvider>,
        s3: S3Client,
        auth: AuthClient,
        store: Arc<dyn NamespacedStore>,
        config: SyncConfig,
        node_signer: Arc<Ed25519KeyPair>,
        laststore_backup_source: Option<Arc<crate::storage::LastStoreNamespacedStore>>,
        backup_publish_target: Option<
            crate::sync::capture::plane_compactor::BackupPublishTargetSlot,
        >,
        cloud_sync_disabled_at: Option<Arc<Mutex<Option<u64>>>>,
    ) -> Self {
        let initial_caps = super::super::upload_policy::UploadPolicySnapshot {
            mode: super::super::upload_policy::UploadPolicyMode::Fixed,
            max_pending: config.max_pending,
            max_upload_entries: config.max_upload_entries_per_cycle,
            max_upload_bytes: config.max_upload_bytes_per_cycle,
            concurrency: config.sync_concurrency,
            budget_bytes: config.max_upload_bytes_per_cycle,
            headroom_rss_bytes: None,
            rss_bytes: None,
            rss_limit_bytes: super::super::upload_policy::UploadPolicySnapshot::default()
                .rss_limit_bytes,
            ewma_upload_bps: 0.0,
            cpu_percent: None,
            foreground_pressure: None,
            throttle_reason: None,
            throttle_source: super::super::upload_policy::UploadThrottleSource::None,
        };
        let backup_progress_enabled = laststore_backup_source.is_some();
        // The progress tracker is process-local, but a committed LastStore
        // manifest is not. Mutation-log mode deliberately stops cutting new
        // full snapshots after that S0, so failing to restore this marker fact
        // leaves every restarted daemon saying "no cycle has completed" even
        // while the real snapshot base and log publisher are healthy.
        let durable_backup = laststore_backup_source
            .as_ref()
            .and_then(|store| store.backup_durability().ok().flatten())
            .filter(|durability| durability.backup_manifest_counter > 0);
        // Hoisted so `PinLog` can share the same handles the engine keeps
        // rather than owning a divergent copy. `SyncConfig` is `Clone` and is
        // never mutated after construction.
        let targets = Arc::new(Mutex::new(vec![SyncTarget {
            label: "personal".to_string(),
            prefix: String::new(),
            crypto: Arc::clone(&crypto),
        }]));
        let backup_publish_target =
            backup_publish_target.unwrap_or_else(|| Arc::new(Mutex::new(None)));
        let pin_log = super::super::pin_log::PinLog::new_with_packing_lock(
            Arc::clone(&store),
            config.clone(),
            Arc::clone(&targets),
            Arc::clone(&backup_publish_target),
        );
        // Hoisted so the plane compactor shares the engine's packing lock and
        // cloud-pause slot rather than owning divergent copies. A rewrite that
        // took a *different* mutex from the one a backup cut takes would not
        // serialize against that cut at all. When the coordinator already
        // started a local cadence, those slots are the same Arcs.
        let cloud_sync_disabled_at =
            cloud_sync_disabled_at.unwrap_or_else(|| Arc::new(Mutex::new(None)));
        let compaction = Arc::new(crate::sync::capture::PlaneCompactor::new(
            Arc::clone(&store),
            Arc::clone(&backup_publish_target),
            Arc::clone(&cloud_sync_disabled_at),
        ));
        let mutation_log_plane = super::super::pin_log::MutationLogLocalCloud::new();
        let mutation_log_frontier = mutation_log_plane.frontier_snapshot();
        let backup_gc_jobs = super::super::gc_jobs::GcJobManager::new(
            laststore_backup_source
                .as_ref()
                .and_then(|source| source.durable_sidecar_dir()),
        );
        let backup_publish_turn = backup_gc_jobs.publication_lock();
        let backup_orphan_gc_mutex = backup_gc_jobs.executor_lock();
        let engine = Self {
            state: Arc::new(Mutex::new(SyncState::Idle)),
            pending: Arc::new(Mutex::new(Vec::new())),
            seq: Arc::new(Mutex::new(0)),
            outbox_meta: Arc::new(Mutex::new(OutboxMeta::default())),
            device_id,
            crypto,
            s3,
            auth,
            cursor_store: Arc::clone(&store),
            store,
            config,
            last_sync_at: Arc::new(Mutex::new(None)),
            last_error: Arc::new(Mutex::new(None)),
            last_error_at: Arc::new(Mutex::new(None)),
            consecutive_sync_failures: AtomicU64::new(0),
            file_blob_absent_with_memo: AtomicU64::new(0),
            file_blob_absent_without_memo: AtomicU64::new(0),
            file_blob_absent_identities: Arc::new(Mutex::new(HashSet::new())),
            file_blob_absent_identities_capped: AtomicBool::new(false),
            failing_since: Arc::new(Mutex::new(None)),
            replay_blocker: Arc::new(Mutex::new(None)),
            backup_blocker: Arc::new(Mutex::new(None)),
            last_snapshot_completion: Arc::new(Mutex::new(SnapshotCompletionStatus::default())),
            backlog_alerts: Arc::new(Mutex::new(CloudSyncBacklogAlertState::default())),
            partitioner: Arc::new(Mutex::new(None)),
            targets,
            target_restore_scopes: Arc::new(Mutex::new(HashMap::new())),
            target_config_lock: Arc::new(Mutex::new(())),
            target_config_generation: AtomicU64::new(1),
            scoped_upload_turn: AtomicU64::new(0),
            download_cursors: Arc::new(Mutex::new(std::collections::HashMap::new())),
            personal_index_reads_since_reconcile: AtomicU64::new(0),
            unseal_failure_log_cache: Arc::new(Mutex::new(HashSet::new())),
            org_scoped_replay_skips: AtomicU64::new(0),
            schema_reloader: Arc::new(Mutex::new(None)),
            mutation_intent_applier: Arc::new(Mutex::new(None)),
            mutation_intent_materializer: Arc::new(Mutex::new(None)),
            automatic_gc_atom_store: Arc::new(Mutex::new(None)),
            automatic_gc_pin_log_barrier: Arc::new(Mutex::new(())),
            automatic_gc_pin_log_activation_pending: AtomicBool::new(false),
            photograph_cut_barrier: Arc::new(Mutex::new(None)),
            photograph_restore_barrier: Arc::new(Mutex::new(None)),
            embedding_reloader: Arc::new(Mutex::new(None)),
            auth_refresh: None,
            wake: Arc::new(tokio::sync::Notify::new()),
            node_signer,
            enc_key: None,
            bytes_since_snapshot: Arc::new(Mutex::new(0)),
            entries_since_snapshot: Arc::new(Mutex::new(0)),
            last_snapshot_bytes: Arc::new(Mutex::new(0)),
            last_snapshot_at: Arc::new(Mutex::new(None)),
            personal_compaction_backoff: Arc::new(Mutex::new(CompactionFailureBackoff::default())),
            last_download_stats: Arc::new(Mutex::new(None)),
            last_upload_stats: Arc::new(Mutex::new(None)),
            upload_policy: Arc::new(super::super::upload_policy::UploadPolicyRuntime::default()),
            foreground_pressure: Arc::new(std::sync::Mutex::new(None)),
            backup_manifest_cache_path: Arc::new(std::sync::Mutex::new(None)),
            cycle_upload_caps: Arc::new(Mutex::new(initial_caps)),
            backup_upload_concurrency_override: Arc::new(Mutex::new(None)),
            backup_gc_jobs,
            laststore_backup_source,
            laststore_backup_uploader_started: AtomicBool::new(false),
            laststore_backup_uploader_stop: AtomicBool::new(false),
            backup_known_present: Arc::new(Mutex::new(std::collections::HashSet::new())),
            backup_known_present_loaded: AtomicBool::new(false),
            backup_presence_listed_at: Arc::new(Mutex::new(None)),
            backup_unresolvable: Arc::new(Mutex::new(
                backup_uploader::BackupUnresolvableChunks::default(),
            )),
            backup_publish_target,
            backup_publish_turn,
            backup_published_tip_identity: Arc::new(Mutex::new(None)),
            backup_gc_identity_revoked: AtomicBool::new(false),
            backup_consecutive_reseal_kills: AtomicU64::new(0),
            backup_unbackable_manifest_chunks: AtomicU64::new(0),
            backup_progress: Arc::new(std::sync::Mutex::new({
                let mut t = crate::backup_progress::BackupProgressTracker::default();
                t.set_enabled(backup_progress_enabled);
                if let Some(durability) = durable_backup.as_ref() {
                    t.restore_published_manifest(durability.last_backup_commit_unix_secs);
                    // The commit timestamp is positive evidence only. Restore
                    // the abandon beside it, or a home whose sealed base was
                    // given up as unpublishable comes back reporting
                    // `complete: true` off the older commit.
                    if durability.sealed_base_abandoned_outstanding() {
                        t.restore_sealed_base_abandoned(
                            crate::sync::engine::backup_uploader::SEALED_BASE_ABANDONED_STATUS_ERROR
                                .to_string(),
                        );
                    }
                }
                t
            })),
            post_cas_backup_gc_tip: Arc::new(Mutex::new(None)),
            post_cas_backup_gc_generation: AtomicU64::new(0),
            backup_orphan_gc_mutex,
            backup_storage_footprint: Arc::new(std::sync::Mutex::new(None)),
            capture_pending: Arc::new(Mutex::new(std::collections::HashMap::new())),
            capture_reexport_pending_count: AtomicU64::new(0),
            capture_reexport_presence: AtomicU64::new(0),
            capture_reexport_drain_lock: Mutex::new(()),
            capture_reexport_scan_cursor: Mutex::new(None),
            capture_reexport_scan_lap_version: Mutex::new(0),
            capture_reexport_scan_lap_saw_rows: Mutex::new(false),
            capture_reexport_failure_count: AtomicU64::new(0),
            capture_reexport_poison_dropped_count: AtomicU64::new(0),
            capture_physical_fallback_records: AtomicU64::new(0),
            capture_physical_catalog_records: AtomicU64::new(0),
            capture_queue_jobs: AtomicU64::new(0),
            capture_queue_rejections: AtomicU64::new(0),
            capture_queue_admission_us: AtomicU64::new(0),
            capture_queue_delay_us: AtomicU64::new(0),
            capture_queue_worker_panics: AtomicU64::new(0),
            capture_stage_us: AtomicU64::new(0),
            capture_record_us: AtomicU64::new(0),
            capture_cleanup_us: AtomicU64::new(0),
            mutation_log_peer_segments_applied: AtomicU64::new(0),
            mutation_log_peer_records_applied: AtomicU64::new(0),
            mutation_log_last_peer_apply_ms: AtomicU64::new(0),
            mutation_log_last_append_ms: AtomicU64::new(0),
            mutation_log_hold_since_ms: AtomicU64::new(0),
            mutation_log_coalesce_retry_ms: AtomicU64::new(0),
            compaction: Arc::clone(&compaction),
            status_disk_usage_cache: Arc::new(StatusDiskUsageCache::new()),
            last_capture_reexport_error: Arc::new(Mutex::new(None)),
            cloud_sync_disabled_at,
            backup_only_mode: std::sync::atomic::AtomicBool::new(false),
            primary_resume_frontier: Mutex::new(None),
            pin_log,
            mutation_log_plane: Arc::new(Mutex::new(mutation_log_plane)),
            mutation_log_frontier,
            outbox_overflow_last_attempt_at: Arc::new(Mutex::new(None)),
            last_outbox_overflow_reason: Arc::new(Mutex::new(None)),
            oversize_outbox_drop_count: AtomicU64::new(0),
            adaptive_deferred_head_seq: AtomicU64::new(0),
            last_oversize_outbox_drop: Arc::new(Mutex::new(None)),
        };
        // The packing-lock cut path no longer clones sealed files into
        // `backup-cut-freeze/`. Pre-packing-lock binaries did, and a daemon
        // that exited mid-cut under that design left a full extra copy of the
        // sealed set with no owner. At construction no target is held yet, so
        // every freeze dir on disk is stale. Sweep unconditionally.
        engine.sweep_backup_freeze_dirs(None);
        engine
    }
}
