//! Account clients do not open a local store or create repair records.

use crate::host::{Host, CLOUD_SYNC_CONFIG_FILE};
use fold_db::storage::config::CloudSyncConfig;
use fold_db::sync::auth::{AuthClient, SyncAuth};
use std::path::Path;
use std::sync::Arc;

pub(crate) fn auth(host: &Host) -> Result<AuthClient, String> {
    if let Some(engine) = host.db.sync_engine() {
        return Ok(engine.cloud_auth());
    }
    let config = read_cloud_config(&host.home)?;
    let http = fold_db::sync::build_shared_http_client().map_err(|error| error.to_string())?;
    let refresh = crate::cloud::auth_refresh_callback(
        host.home.clone(),
        config.api_url.clone(),
        Arc::clone(&host.keypair),
    );
    Ok(AuthClient::new(
        Arc::new(http),
        config.api_url,
        SyncAuth::ApiKey(config.api_key),
    )
    .with_auth_refresh_callback(refresh))
}

pub(crate) fn read_cloud_config(home: &Path) -> Result<CloudSyncConfig, String> {
    let active = home.join(CLOUD_SYNC_CONFIG_FILE);
    let paused = home.join(format!("{CLOUD_SYNC_CONFIG_FILE}.paused"));
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
