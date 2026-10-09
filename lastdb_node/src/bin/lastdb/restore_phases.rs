//! Phases of `lastdb restore`: preflight, source scope, identity, destination
//! preparation, engine wiring, the S0 install plus mutation-log tail, and the
//! completion markers. `restore_cmds::restore_command_inner_with_cache` runs
//! them in order; each phase owns one failure stage.

use super::*;

use fold_db::sync::engine::LastStoreCloudRestoreReport;
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
pub(crate) struct Flavor<'a> {
    pub(crate) cache_home: Option<&'a Path>,
    pub(crate) remote_s0_only: bool,
    pub(crate) remote_latest: bool,
    pub(crate) selection: RemoteRecoverySelector<'a>,
}

impl<'a> Flavor<'a> {
    pub(crate) fn from_mode(mode: RestoreSourceMode<'a>) -> Self {
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

    pub(crate) fn remote_only(&self) -> bool {
        self.remote_s0_only || self.remote_latest
    }
}

pub(crate) struct Homes {
    pub(crate) source: PathBuf,
    pub(crate) target: PathBuf,
    pub(crate) target_active_cloud: PathBuf,
    pub(crate) target_paused_cloud: PathBuf,
    pub(crate) resume_mutation_log: bool,
    pub(crate) cache: Option<fold_db::sync::engine::RestoreChunkCache>,
}

/// Resolve both homes and refuse a destination that is not safe to write.
pub(crate) fn resolve_homes(
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

pub(crate) struct Source {
    pub(crate) cloud_path: PathBuf,
    pub(crate) paused_source: bool,
    pub(crate) paused_receipt: Option<lastdb_node::cloud::PausedHomeBackupReceipt>,
    pub(crate) resume_report: Option<LastStoreCloudRestoreReport>,
    pub(crate) local_db_hash: Option<String>,
}

/// Pin the cloud namespace and the paused or resumable state of the source.
pub(crate) fn resolve_source(homes: &Homes, flavor: &Flavor<'_>) -> Phase<Source> {
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
pub(crate) fn source_db_hash(source: &Source, remote: Option<&RemoteRecovery>) -> String {
    remote
        .map(|recovery| recovery.descriptor.db_hash.clone())
        .or_else(|| source.local_db_hash.clone())
        .expect("source scope was checked")
}

pub(crate) struct Identity {
    pub(crate) url: String,
    pub(crate) api_key: String,
    pub(crate) seed: [u8; 32],
    e2e: fold_db::crypto::E2eKeys,
}

/// Load cloud credentials and derive the account keys from the identity seed.
pub(crate) fn load_identity(
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
pub(crate) fn discover_remote_descriptor(
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

#[path = "restore_phases/destination.rs"]
mod destination;
pub(crate) use destination::*;
#[path = "restore_phases/apply.rs"]
mod apply;
pub(crate) use apply::*;
#[path = "restore_phases/finish.rs"]
mod finish;
pub(crate) use finish::*;
