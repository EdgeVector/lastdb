//! Phases of `lastdb restore`: preflight, source scope, identity, destination
//! preparation, engine wiring, the S0 install plus mutation-log tail, and the
//! completion markers. `restore_cmds::restore_command_inner_with_cache` runs
//! them in order; each phase owns one failure stage.

use super::*;

use fold_db::sync::engine::{BackupRestoreMode, LastStoreCloudRestoreReport, RestoreProgress};
use RestoreFailureCode as Code;
use RestoreFailureStage as Stage;

type Phase<T> = Result<T, RestoreFailure>;

fn preflight_failure(detail: impl Into<String>) -> RestoreFailure {
    RestoreFailure::new(Stage::Preflight, Code::OperationFailed, detail)
}

fn io_failure(stage: Stage, detail: impl Into<String>) -> RestoreFailure {
    RestoreFailure::new(stage, Code::IoError, detail)
}

fn runtime_failure(detail: impl Into<String>) -> RestoreFailure {
    RestoreFailure::new(Stage::RuntimeInit, Code::OperationFailed, detail)
}

fn scope_failure(detail: impl Into<String>) -> RestoreFailure {
    RestoreFailure::new(Stage::SourceScope, Code::InvalidSourceScope, detail)
}

fn tail_failure(detail: impl Into<String>) -> RestoreFailure {
    RestoreFailure::new(Stage::RestoreTail, Code::OperationFailed, detail)
}

/// Which restore entry point is running and what it selected.
pub(super) struct Flavor<'a> {
    cache_home: Option<&'a Path>,
    pub(super) remote_s0_only: bool,
    pub(super) remote_latest: bool,
    selection: RemoteRecoverySelector<'a>,
}

impl<'a> Flavor<'a> {
    pub(super) fn from_mode(mode: RestoreSourceMode<'a>) -> Self {
        let (cache_home, remote_s0_only, remote_latest, selection) = match mode {
            RestoreSourceMode::Normal { cache_home } => {
                (cache_home, false, false, RemoteRecoverySelector::default())
            }
            RestoreSourceMode::RemoteS0(selection) => (None, true, false, selection),
            RestoreSourceMode::RemoteLatest(selection) => (None, false, true, selection),
        };
        Self {
            cache_home,
            remote_s0_only,
            remote_latest,
            selection,
        }
    }

    pub(super) fn remote_only(&self) -> bool {
        self.remote_s0_only || self.remote_latest
    }
}

pub(super) struct Homes {
    pub(super) source: PathBuf,
    pub(super) target: PathBuf,
    target_active_cloud: PathBuf,
    target_paused_cloud: PathBuf,
    resume_mutation_log: bool,
    cache: Option<fold_db::sync::engine::RestoreChunkCache>,
}

/// Resolve both homes and refuse a destination that is not safe to write.
pub(super) fn resolve_homes(
    data_dir: Option<PathBuf>,
    into: &Path,
    flavor: &Flavor<'_>,
) -> Phase<Homes> {
    let remote_only = flavor.remote_only();
    let source = lastdb_node::host::resolve_home(data_dir).map_err(preflight_failure)?;
    let target = expand_home_path(into).map_err(preflight_failure)?;
    refuse_same_home(&source, &target).map_err(preflight_failure)?;
    if remote_only {
        refuse_overlapping_restore_cache(&source, &target).map_err(preflight_failure)?;
        refuse_recovery_home_with_store_data(&source).map_err(preflight_failure)?;
    }
    let resume_mutation_log = dest_has_committed_s0(&target);
    if flavor.cache_home.is_some() || remote_only {
        refuse_non_fresh_migration_home(&target).map_err(preflight_failure)?;
    }
    if !resume_mutation_log || flavor.cache_home.is_some() || remote_only {
        refuse_non_fresh_restore_home(&target).map_err(preflight_failure)?;
    }
    let (target_active_cloud, target_paused_cloud) = lastdb_node::cloud::cloud_sync_paths(&target);
    if target_active_cloud.exists()
        || lastdb_node::cloud::cloud_resume_requested_path(&target).exists()
        || lastdb_node::cloud::cloud_resume_ready_path(&target).exists()
    {
        return Err(preflight_failure(
            "restore destination has active or stale cloud state; use a fresh destination",
        ));
    }

    let cache = flavor
        .cache_home
        .map(|home| {
            let home = expand_home_path(home)?;
            refuse_overlapping_restore_cache(&home, &target)?;
            fold_db::sync::engine::RestoreChunkCache::new(&home)
        })
        .transpose()
        .map_err(preflight_failure)?;

    Ok(Homes {
        source,
        target,
        target_active_cloud,
        target_paused_cloud,
        resume_mutation_log,
        cache,
    })
}

pub(super) struct Source {
    cloud_path: PathBuf,
    paused_source: bool,
    paused_receipt: Option<lastdb_node::cloud::PausedHomeBackupReceipt>,
    pub(super) resume_report: Option<LastStoreCloudRestoreReport>,
    local_db_hash: Option<String>,
}

/// Pin the cloud namespace and the paused or resumable state of the source.
pub(super) fn resolve_source(homes: &Homes, flavor: &Flavor<'_>) -> Phase<Source> {
    let remote_only = flavor.remote_only();
    // The source store identity selects the only cloud namespace this command
    // may read. Missing or malformed local proof must fail before auth or S3.
    let local_db_hash = if remote_only {
        None
    } else {
        Some(
            fold_db::storage::laststore::read_cloud_db_hash(&homes.source.join("data"))
                .ok_or_else(|| {
                    RestoreFailure::new(
                        Stage::SourceScope,
                        Code::InvalidSourceScope,
                        "source LastStore cloud identity is missing or invalid; refusing an unscoped restore",
                    )
                })?,
        )
    };

    let (active_cloud, paused_cloud) = lastdb_node::cloud::cloud_sync_paths(&homes.source);
    if active_cloud.exists() && paused_cloud.exists() {
        return Err(preflight_failure(
            "source has both active and paused cloud configuration",
        ));
    }
    let paused_source = remote_only || (!active_cloud.exists() && paused_cloud.exists());
    let cloud_path = if paused_source {
        if remote_only && active_cloud.exists() {
            active_cloud
        } else {
            paused_cloud
        }
    } else {
        active_cloud
    };
    let paused_receipt = if paused_source && !remote_only {
        if !lastdb_node::cloud::cloud_resume_required_path(&homes.source).is_file()
            || lastdb_node::cloud::cloud_resume_requested_path(&homes.source).exists()
        {
            return Err(preflight_failure(
                "paused source needs a completed backup and a durable resume barrier",
            ));
        }
        Some(
            lastdb_node::cloud::read_paused_home_backup_receipt(&homes.source)
                .map_err(preflight_failure)?,
        )
    } else {
        None
    };
    let resume_report = if homes.resume_mutation_log && !remote_only {
        Some(
            restore_checkpoint::load(
                &homes.target,
                local_db_hash.as_deref().expect("local source hash"),
                &homes.source,
            )
            .map_err(preflight_failure)?,
        )
    } else {
        None
    };
    Ok(Source {
        cloud_path,
        paused_source,
        paused_receipt,
        resume_report,
        local_db_hash,
    })
}

/// The cloud namespace to restore from: the descriptor's when the source
/// home is lost, else the local store identity checked in `resolve_source`.
pub(super) fn source_db_hash(source: &Source, remote: Option<&RemoteRecovery>) -> String {
    remote
        .map(|recovery| recovery.descriptor.db_hash.clone())
        .or_else(|| source.local_db_hash.clone())
        .expect("source scope was checked")
}

pub(super) struct Identity {
    url: String,
    api_key: String,
    seed: [u8; 32],
    e2e: fold_db::crypto::E2eKeys,
}

/// Load cloud credentials and derive the account keys from the identity seed.
pub(super) fn load_identity(
    homes: &Homes,
    source: &Source,
    api_url: Option<String>,
    env: Option<&str>,
) -> Phase<Identity> {
    let (url, api_key) =
        load_cloud_creds_from_path(&source.cloud_path, api_url, env).map_err(|detail| {
            RestoreFailure::new(Stage::CloudCredentials, Code::OperationFailed, detail)
        })?;
    let seed_path = homes.source.join(lastdb_node::host::IDENTITY_KEY_FILE);
    let seed_bytes = std::fs::read(&seed_path).map_err(|e| {
        io_failure(
            Stage::IdentityKey,
            format!("read {}: {e}", seed_path.display()),
        )
    })?;
    if seed_bytes.len() != 32 {
        return Err(RestoreFailure::new(
            Stage::IdentityKey,
            Code::InvalidIdentityKey,
            format!(
                "{} must be 32 bytes, got {}",
                seed_path.display(),
                seed_bytes.len()
            ),
        ));
    }
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&seed_bytes);
    let e2e = fold_db::crypto::E2eKeys::from_ed25519_seed(&seed).map_err(|e| {
        RestoreFailure::new(
            Stage::IdentityKey,
            Code::CryptoError,
            format!("E2E derive: {e}"),
        )
    })?;
    Ok(Identity {
        url,
        api_key,
        seed,
        e2e,
    })
}

fn build_runtime() -> Phase<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| runtime_failure(format!("tokio runtime construction failed: {e}")))
}

/// A lost source home has no local store UUID, layout, or receipt. The
/// encrypted account descriptor supplies them only after cloud latest and
/// its exact manifest identity agree. Do this before writing the target.
pub(super) fn discover_remote_descriptor(
    flavor: &Flavor<'_>,
    identity: &Identity,
) -> Phase<Option<RemoteRecovery>> {
    if !flavor.remote_only() {
        return Ok(None);
    }
    let runtime = build_runtime()?;
    let http = std::sync::Arc::new(fold_db::sync::build_shared_http_client());
    let s3 = fold_db::sync::s3::S3Client::new(std::sync::Arc::clone(&http));
    let unscoped = fold_db::sync::auth::AuthClient::new(
        http,
        identity.url.clone(),
        fold_db::sync::auth::SyncAuth::ApiKey(identity.api_key.clone()),
    )
    .without_db_auto_claim();
    let key = identity.e2e.encryption_key();
    let discovered = if flavor.remote_s0_only {
        runtime.block_on(discover_remote_recovery_descriptor(
            &unscoped,
            &s3,
            &key,
            flavor.selection,
        ))
    } else {
        runtime.block_on(discover_remote_latest_descriptor(
            &unscoped,
            &s3,
            &key,
            flavor.selection,
        ))
    };
    discovered.map(Some).map_err(scope_failure)
}

/// Create the destination home, copy the identity seed and the paused cloud
/// configuration, and return the destination `data` directory.
pub(super) fn prepare_destination(
    homes: &Homes,
    source: &Source,
    identity: &Identity,
) -> Phase<PathBuf> {
    let target = &homes.target;
    std::fs::create_dir_all(target).map_err(|e| {
        io_failure(
            Stage::PrepareDestination,
            format!("create {}: {e}", target.display()),
        )
    })?;
    write_owner_only_local(
        &target.join(lastdb_node::host::IDENTITY_KEY_FILE),
        &identity.seed,
    )
    .map_err(|detail| io_failure(Stage::PrepareDestination, detail))?;
    let cloud_bytes = std::fs::read(&source.cloud_path).map_err(|e| {
        io_failure(
            Stage::PrepareDestination,
            format!("read {}: {e}", source.cloud_path.display()),
        )
    })?;
    // Every incomplete restore stays Off, including a failed normal tail replay.
    write_owner_only_local(&homes.target_paused_cloud, &cloud_bytes)
        .map_err(|detail| io_failure(Stage::PrepareDestination, detail))?;
    lastdb_node::cloud::mark_cloud_resume_required(target)
        .map_err(|detail| io_failure(Stage::PrepareDestination, detail))?;

    let data_path = target.join("data");
    std::fs::create_dir_all(&data_path).map_err(|e| {
        io_failure(
            Stage::PrepareDestination,
            format!("create {}: {e}", data_path.display()),
        )
    })?;
    Ok(data_path)
}

/// Open the destination LastStore.
///
/// Backup chunks keep the source shard/group addresses. The destination
/// descriptor must therefore match the source descriptor before any chunk
/// is installed. Reading the descriptor does not open or lock the live
/// source store. Existing destination descriptors still win through
/// open_existing_or_with, which keeps mutation-log resume stable.
pub(super) fn open_destination_store(
    homes: &Homes,
    identity: &Identity,
    remote_descriptor: Option<&RemoteRecovery>,
    data_path: &Path,
) -> Phase<std::sync::Arc<fold_db::storage::LastStoreNamespacedStore>> {
    let frame_aead = env_flag::var_truthy("LASTDB_RESTORE_FRAME_AEAD");
    let opts = if let Some(recovery) = remote_descriptor {
        recovery
            .descriptor
            .to_options(&identity.e2e.encryption_key(), frame_aead)
            .map_err(|detail| {
                let code = if detail == "frame AEAD recovery layout requires opt-in" {
                    Code::FrameAeadOptInRequired
                } else {
                    Code::LayoutMismatch
                };
                RestoreFailure::new(Stage::SourceLayout, code, detail)
            })?
    } else {
        restore_options_from_source_layout(
            &homes.source.join("data"),
            identity.e2e.encryption_key(),
            frame_aead,
        )?
    };
    let high_water = homes.target.join("laststore_high_water.json");
    let (opened, label) = if opts.packaging == laststore::PackagingMode::FrameAead {
        (
            fold_db::storage::LastStoreNamespacedStore::open_with_options_and_high_water_data_key(
                data_path, opts, high_water,
            ),
            "open target LastStore (frame_aead)",
        )
    } else {
        (
            fold_db::storage::LastStoreNamespacedStore::open_with_options_and_high_water(
                data_path, opts, high_water,
            ),
            "open target LastStore",
        )
    };
    let store = opened.map_err(|e| {
        RestoreFailure::new(
            Stage::OpenDestination,
            Code::OperationFailed,
            format!("{label}: {e}"),
        )
    })?;
    Ok(std::sync::Arc::new(store))
}

pub(super) struct CloudClients {
    runtime: tokio::runtime::Runtime,
    s3: fold_db::sync::s3::S3Client,
    auth: fold_db::sync::auth::AuthClient,
}

pub(super) fn build_cloud_clients(
    identity: &Identity,
    source_db_hash: &str,
) -> Phase<CloudClients> {
    let runtime = build_runtime()?;
    let http = std::sync::Arc::new(fold_db::sync::build_shared_http_client());
    let s3 = fold_db::sync::s3::S3Client::new(std::sync::Arc::clone(&http));
    // Publish writes `{cloud_db_hash(source store_uuid)}/backup/latest`. A
    // restore AuthClient with no db_hash looks under the authenticated
    // user_hash instead and reports "backup latest pointer missing".
    let auth = fold_db::sync::auth::AuthClient::new(
        http,
        identity.url.clone(),
        fold_db::sync::auth::SyncAuth::ApiKey(identity.api_key.clone()),
    )
    .with_db_hash(Some(source_db_hash.to_string()))
    .without_db_auto_claim();
    Ok(CloudClients { runtime, s3, auth })
}

pub(super) struct Engine {
    engine: std::sync::Arc<fold_db::sync::SyncEngine>,
    signer: std::sync::Arc<fold_db::security::Ed25519KeyPair>,
}

pub(super) fn build_engine(
    identity: &Identity,
    clients: &CloudClients,
    store: &std::sync::Arc<fold_db::storage::LastStoreNamespacedStore>,
) -> Phase<Engine> {
    let e2e = &identity.e2e;
    // Cloud outer seal uses the account E2E content key (same key that sealed
    // continuous mutation-log segments). After S0 chunks land, apply the
    // continuous plane so post-S0 history is not write-only.
    let sync_crypto: std::sync::Arc<dyn fold_db::crypto::CryptoProvider> = std::sync::Arc::new(
        fold_db::crypto::LocalCryptoProvider::from_key(e2e.encryption_key()),
    );
    // Mutation-log replay must write through the local at-rest seam, exactly as
    // the daemon wires it (`fold_db_core::factory::local::store_stack`: the
    // engine receives `enc_store`, the cursor store stays raw). Handing the
    // engine the bare LastStore made `stored_replay_value` fall through to its
    // no-key branch and write logical bytes verbatim, so every replayed
    // `main`/`metadata` row landed unsealed on disk while S0 chunks stayed
    // sealed. Catalog namespaces stay plaintext by policy through the same
    // allowlist the factory uses.
    let at_rest_crypto: std::sync::Arc<dyn fold_db::crypto::CryptoProvider> = std::sync::Arc::new(
        fold_db::crypto::LocalCryptoProvider::from_key(e2e.encryption_key()),
    );
    let namespaced: std::sync::Arc<dyn fold_db::storage::NamespacedStore> = std::sync::Arc::new(
        fold_db::storage::EncryptingNamespacedStore::with_plaintext_namespaces(
            std::sync::Arc::clone(store) as _,
            at_rest_crypto,
            fold_db::storage::LASTSTORE_PLAINTEXT_NAMESPACES
                .iter()
                .copied()
                .map(str::to_string)
                .collect(),
        ),
    );
    let signer = std::sync::Arc::new(
        fold_db::security::Ed25519KeyPair::from_secret_key(&identity.seed)
            .map_err(|e| runtime_failure(format!("restore signer from identity seed: {e}")))?,
    );
    let mut engine = fold_db::sync::SyncEngine::new_with_laststore_backup_source(
        "lastdb-restore".to_string(),
        sync_crypto,
        clients.s3.clone(),
        clients.auth.clone(),
        namespaced,
        fold_db::sync::SyncConfig {
            capture_mode: fold_db::sync::engine::CaptureMode::MutationLog,
            legacy_personal_cloud_sync: false,
            ..fold_db::sync::SyncConfig::default()
        },
        std::sync::Arc::clone(&signer),
        Some(std::sync::Arc::clone(store)),
    );
    // The photograph and download-cursor planes carry on-disk `ENC:` envelopes
    // already, so they stay on the raw store and are not sealed twice. Without
    // this the engine store above would become the cursor store by default.
    engine.set_cursor_store(std::sync::Arc::clone(store) as _);
    // Cursor bookkeeping is sealed under the portable content key before it
    // reaches the local seam, and legacy `ENC:` replay values are unwrapped
    // with it. Same key the daemon passes.
    engine.set_at_rest_key(e2e.encryption_key());
    Ok(Engine {
        engine: std::sync::Arc::new(engine),
        signer,
    })
}

/// Everything the async install and replay steps read.
pub(super) struct Apply<'a> {
    pub(super) flavor: &'a Flavor<'a>,
    pub(super) homes: &'a Homes,
    pub(super) source: &'a Source,
    pub(super) remote_descriptor: Option<&'a RemoteRecovery>,
    pub(super) source_db_hash: &'a str,
    pub(super) identity: &'a Identity,
    pub(super) clients: &'a CloudClients,
    pub(super) store: &'a std::sync::Arc<fold_db::storage::LastStoreNamespacedStore>,
    pub(super) engine: &'a Engine,
    pub(super) data_path: &'a Path,
    pub(super) progress: Option<&'a RestoreProgress>,
}

pub(super) struct Applied {
    report: LastStoreCloudRestoreReport,
    mode: BackupRestoreMode,
}

impl Apply<'_> {
    /// Install S0, then replay the mutation-log tail when the marker asks for it.
    pub(super) fn run(self, resume_report: Option<LastStoreCloudRestoreReport>) -> Phase<Applied> {
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

/// Mark the destination complete and, for a flush-and-resume restore, re-arm
/// cloud sync. Returns the remote-ready record for a remote-only restore.
pub(super) fn finalize_destination(
    homes: &Homes,
    flavor: &Flavor<'_>,
    applied: &Applied,
    remote_descriptor: Option<&RemoteRecovery>,
    source_db_hash: &str,
) -> Phase<Option<serde_json::Value>> {
    let target = &homes.target;
    let bootstrap_done = target.join(lastdb_node::cloud::BOOTSTRAP_DONE_FILE);
    write_owner_only_local(&bootstrap_done, b"ok\n").map_err(|detail| {
        io_failure(
            Stage::CompletionMarker,
            format!(
                "write {} after LastStore restore: {detail}",
                bootstrap_done.display()
            ),
        )
    })?;
    if applied.mode == BackupRestoreMode::ReplayTail && !flavor.remote_latest {
        lastdb_node::cloud::clear_cloud_resume_required(target)
            .map_err(|detail| io_failure(Stage::CompletionMarker, detail))?;
        lastdb_node::cloud::resume_cloud_sync_file(target)
            .map_err(|detail| io_failure(Stage::CompletionMarker, detail))?;
    }
    if !flavor.remote_only() {
        return Ok(None);
    }
    let report = &applied.report;
    let expected_mode = if flavor.remote_s0_only {
        BackupRestoreMode::S0Only
    } else {
        BackupRestoreMode::ReplayTail
    };
    if applied.mode != expected_mode
        || !report.remote_read_only
        || !report.source_scope_verified
        || !homes.target_paused_cloud.is_file()
        || !lastdb_node::cloud::cloud_resume_required_path(target).is_file()
        || homes.target_active_cloud.exists()
        || !bootstrap_done.is_file()
    {
        return Err(RestoreFailure::new(
            Stage::CompletionMarker,
            Code::OperationFailed,
            "remote restore did not leave a complete Cloud Off target",
        ));
    }
    let recovery = remote_descriptor.expect("remote recovery was checked");
    let (ready_file, mode_name) = if flavor.remote_s0_only {
        (RESCUE_S0_RESTORE_READY_FILE, "s0_only")
    } else {
        (NORMAL_LATEST_RESTORE_READY_FILE, "replay_tail")
    };
    let ready = serde_json::json!({
        "version": 1,
        "ok": true,
        "db_hash": source_db_hash,
        "store_uuid": recovery.descriptor.store_uuid,
        "manifest_sha256": report.manifest_sha256,
        "counter": report.counter,
        "restore_mode": mode_name,
        "cloud_sync_off": true,
    });
    let bytes = serde_json::to_vec_pretty(&ready).map_err(|error| {
        RestoreFailure::new(
            Stage::CompletionMarker,
            Code::SerializationError,
            format!("encode remote restore record: {error}"),
        )
    })?;
    write_owner_only_local(&target.join(ready_file), &bytes)
        .map_err(|detail| io_failure(Stage::CompletionMarker, detail))?;
    std::fs::File::open(target)
        .and_then(|dir| dir.sync_all())
        .map_err(|error| {
            io_failure(
                Stage::CompletionMarker,
                format!("sync remote restore home: {error}"),
            )
        })?;
    Ok(Some(ready))
}

fn render_failure(error: impl std::fmt::Display) -> RestoreFailure {
    RestoreFailure::new(
        Stage::RenderReport,
        Code::SerializationError,
        format!("encode report: {error}"),
    )
}

/// Print the restore report as JSON, or as the human-readable summary.
pub(super) fn render_report(
    homes: &Homes,
    flavor: &Flavor<'_>,
    applied: &Applied,
    remote_ready: Option<serde_json::Value>,
    json_only: bool,
) -> Phase<()> {
    let report = &applied.report;
    if json_only {
        let mut report_json = serde_json::to_value(report).map_err(render_failure)?;
        if let Some(serde_json::Value::Object(ready)) = remote_ready {
            report_json
                .as_object_mut()
                .expect("restore report is an object")
                .extend(ready);
        }
        println!(
            "{}",
            serde_json::to_string_pretty(&report_json).map_err(render_failure)?
        );
        return Ok(());
    }
    println!("Restored LastStore backup into {}", homes.target.display());
    println!("  manifest: {}", report.manifest_sha256);
    println!("  counter:  {}", report.counter);
    println!("  cut_csn:  {}", report.cut_csn);
    println!("  chunks:   {}", report.chunks_installed);
    println!("  bytes:    {}", format_bytes(report.bytes_installed));
    println!("  epoch:    {}", report.restored_epoch);
    println!("  source scope verified: {}", report.source_scope_verified);
    println!("  remote read-only:      {}", report.remote_read_only);
    if applied.mode == BackupRestoreMode::S0Only {
        println!("  cloud mutation tail:  skipped");
        println!("  Cloud Sync:           Off");
    } else if flavor.remote_latest {
        println!("  Cloud Sync:           Off");
    }
    if let Some(ml) = &report.mutation_log_replay {
        println!(
            "  mutation-log: considered={} applied={} records={}",
            ml.segments_considered, ml.segments_applied, ml.records_applied
        );
    }
    Ok(())
}
