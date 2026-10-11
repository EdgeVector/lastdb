//! Node-owned durable selected-device operations. The runtime serializes calls per home.

use crate::host::{Host, CLOUD_SYNC_CONFIG_FILE, IDENTITY_KEY_FILE};
use fold_db::storage::config::CloudSyncConfig;
use fold_db::sync::cloud_purge::protocol::{CloudAccount, CloudAccountKind};
use fold_db::sync::cloud_purge::{CloudPurgeDriver, CloudPurgeJob};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(crate) fn directory(home: &Path, operation_id: &str) -> Result<PathBuf, String> {
    uuid::Uuid::parse_str(operation_id).map_err(|_| "invalid cloud purge operation ID")?;
    Ok(home
        .join("cloud-purge-resync")
        .join("jobs")
        .join(operation_id))
}

pub(crate) async fn prepare_held(
    host: &Host,
    account: CloudAccount,
    operation_id: &str,
    fence: &fold_db::sync::engine::CloudPurgeWriteFence,
) -> Result<CloudPurgeJob, String> {
    let directory = directory(&host.home, operation_id)?;
    if directory.join("job.json").exists() {
        return CloudPurgeJob::reopen(&directory).map_err(|error| error.to_string());
    }
    if account.kind == CloudAccountKind::Personal && account.id != host.user_hash {
        return Err("the personal purge account differs from this node identity".into());
    }
    let engine = source_engine(host).await?;
    let preparation = engine
        .cloud_purge_auth()
        .cloud_purge_prepare(&account)
        .await
        .map_err(|error| error.to_string())?;
    if preparation.account != account {
        return Err("the cloud resolved a different purge account".into());
    }
    preparation
        .validate_inventory_capabilities()
        .map_err(|error| error.to_string())?;
    let source_dir = directory.join("source");
    let source = if source_dir.join("source.json").exists() {
        fold_db::sync::cloud_purge::RetainedSource::reopen(&source_dir)
            .map_err(|error| error.to_string())?
    } else {
        engine
            .complete_cloud_purge_file_closure(host.db.db_ops(), fence)
            .await
            .map_err(|error| error.to_string())?;
        if account.kind == CloudAccountKind::Org {
            engine
                .prepare_cloud_purge_org_source_held(
                    &source_dir,
                    account.clone(),
                    fence,
                    host.db.db_ops(),
                )
                .await
        } else {
            engine
                .prepare_cloud_purge_source_held(
                    &source_dir,
                    account.clone(),
                    fence,
                    host.db.db_ops(),
                )
                .await
        }
        .map_err(|error| error.to_string())?
    };
    CloudPurgeJob::create(&directory, operation_id, &source, preparation.scope_version)
        .map_err(|error| error.to_string())
}

pub(crate) async fn run_step(host: &Host, operation_id: &str) -> Result<bool, String> {
    let mut job = CloudPurgeJob::reopen(&directory(&host.home, operation_id)?)
        .map_err(|error| error.to_string())?;
    let source = job.source().map_err(|error| error.to_string())?;
    let result = driver(host).await?.step(&mut job, &source).await;
    if result.is_err() {
        job.last_error =
            Some("the operation stopped; its exact source and cloud hold remain available".into());
        job.save().map_err(|error| error.to_string())?;
    }
    result.map_err(|error| error.to_string())
}

pub(crate) async fn status(
    host: &Host,
    operation_id: &str,
    proof_receipt: Option<&Path>,
) -> Result<serde_json::Value, String> {
    let mut job = CloudPurgeJob::reopen(&directory(&host.home, operation_id)?)
        .map_err(|error| error.to_string())?;
    driver(host)
        .await?
        .status_readonly(&mut job)
        .await
        .map_err(|error| error.to_string())?;
    if let Some(path) = proof_receipt {
        job.write_proof_receipt(path)
            .map_err(|error| error.to_string())?;
    }
    serde_json::to_value(job).map_err(|error| error.to_string())
}

pub(crate) async fn cancel(host: &Host, operation_id: &str) -> Result<serde_json::Value, String> {
    let mut job = CloudPurgeJob::reopen(&directory(&host.home, operation_id)?)
        .map_err(|error| error.to_string())?;
    driver(host)
        .await?
        .cancel(&mut job)
        .await
        .map_err(|error| error.to_string())?;
    serde_json::to_value(job).map_err(|error| error.to_string())
}

async fn driver(host: &Host) -> Result<CloudPurgeDriver, String> {
    let auth = source_engine(host).await?.cloud_purge_auth();
    Ok(CloudPurgeDriver::new(
        auth,
        Arc::new(crate::cloud_purge_secrets::LastSecretsCapabilities),
    ))
}

pub(crate) async fn source_engine(host: &Host) -> Result<Arc<fold_db::sync::SyncEngine>, String> {
    static CONSTRUCT: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    if let Some(engine) = crate::cloud_purge_runtime::cached_engine(host) {
        return Ok(engine);
    }
    if let Some(engine) = host.db.sync_engine() {
        return Ok(engine);
    }
    let _construct = CONSTRUCT.lock().await;
    if let Some(engine) = crate::cloud_purge_runtime::cached_engine(host) {
        return Ok(engine);
    }
    let config = read_cloud_config(&host.home)?;
    let raw = std::fs::read(host.home.join(IDENTITY_KEY_FILE))
        .map_err(|_| "node identity is unavailable")?;
    let seed: [u8; 32] = raw
        .as_slice()
        .try_into()
        .map_err(|_| "node identity has invalid length")?;
    let keys = fold_db::crypto::E2eKeys::from_ed25519_seed(&seed)
        .map_err(|_| "node encryption keys are unavailable")?;
    let engine = host
        .db
        .cloud_purge_source_engine(
            &config.api_url,
            &config.api_key,
            &host.data_dir.to_string_lossy(),
            &keys,
        )
        .await
        .map_err(|error| error.to_string())?;
    crate::cloud_purge_runtime::cache_engine(host, Arc::clone(&engine))?;
    Ok(engine)
}

pub(crate) fn read_cloud_config(home: &Path) -> Result<CloudSyncConfig, String> {
    let (active, paused) = (
        home.join(CLOUD_SYNC_CONFIG_FILE),
        home.join(format!("{CLOUD_SYNC_CONFIG_FILE}.paused")),
    );
    if active.exists() && paused.exists() {
        return Err("the home has two cloud configurations".into());
    }
    let path = if active.exists() { active } else { paused };
    let metadata = std::fs::symlink_metadata(&path)
        .map_err(|_| "the home has no cloud account configuration")?;
    if !metadata.file_type().is_file() || metadata.len() > 65_536 {
        return Err("invalid cloud account configuration file".into());
    }
    let config: CloudSyncConfig = serde_json::from_slice(
        &std::fs::read(path).map_err(|_| "cloud account configuration is unreadable")?,
    )
    .map_err(|_| "invalid cloud account configuration")?;
    if config.api_key.is_empty() {
        return Err("cloud account credentials are absent".into());
    }
    Ok(config)
}
