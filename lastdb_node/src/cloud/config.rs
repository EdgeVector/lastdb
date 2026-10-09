//! Cloud sync config and environment pinning.

use super::*;

/// The Exemem environment this home is connected to, from the `api_url`
/// that `lastdb connect --env <e>` persisted in `cloud_sync.json` (or the
/// paused copy). `None` when the home has no cloud connection or points at
/// a custom URL.
pub fn home_cloud_environment(home: &Path) -> Option<folddb_profile::endpoints::Environment> {
    let (active, paused) = cloud_sync_paths(home);
    let raw = std::fs::read(&active)
        .or_else(|_| std::fs::read(&paused))
        .ok()?;
    let value: serde_json::Value = serde_json::from_slice(&raw).ok()?;
    let api_url = value.get("api_url")?.as_str()?;
    folddb_profile::endpoints::environment_for_exemem_api_url(api_url)
}

/// Daemon boot: pin the schema-service environment to the home's cloud
/// environment, so a home connected with `--env dev` does not send schema
/// resolve/register to the PROD schema service when `EXEMEM_ENV` is unset.
/// Env vars still win; a mismatch between them and the home is logged as a
/// loud WARN (not a refusal, so a misconfigured node still boots).
pub fn pin_schema_service_environment_from_home(home: &Path) {
    use folddb_profile::endpoints::{self, Environment};
    let Some(cloud_env) = home_cloud_environment(home) else {
        return;
    };
    endpoints::set_home_environment(cloud_env);
    let effective = Environment::from_env();
    let schema_url = endpoints::schema_service_url();
    let expected_url = endpoints::schema_service_url_for(cloud_env);
    if effective != cloud_env || schema_url != expected_url {
        tracing::warn!(
            target: "lastdb_node::cloud",
            cloud_env = %cloud_env,
            schema_service_env = %effective,
            schema_service_url = %schema_url,
            expected_schema_service_url = %expected_url,
            "ENVIRONMENT MISMATCH: this home syncs to the {cloud_env} Exemem cloud \
             (cloud_sync.json), but schema resolve/register go to {schema_url}. \
             An env var (ENVIRONMENT / EXEMEM_ENV / FOLD_SCHEMA_SERVICE_URL) overrides \
             the home. Unset it, or reconnect the home to the matching environment."
        );
    } else {
        tracing::info!(
            target: "lastdb_node::cloud",
            cloud_env = %cloud_env,
            schema_service_url = %schema_url,
            "schema service environment follows the home cloud environment"
        );
    }
}

/// Persist the L2 cloud-sync intent: `<home>/cloud_sync.json`, owner-only.
///
/// Written with explicit `json!` (not `CloudSyncConfig` serialization —
/// its `api_key` field is `skip_serializing`, by design for the full node's
/// node_config.json; here the file IS the per-device credential store, the
/// minimal daemon's analogue of `credentials.json`).
pub fn write_cloud_sync_config(home: &Path, api_url: &str, api_key: &str) -> Result<(), String> {
    write_cloud_sync_config_at(home, api_url, api_key, CLOUD_SYNC_CONFIG_FILE)
}

pub(super) fn write_cloud_sync_config_at(
    home: &Path,
    api_url: &str,
    api_key: &str,
    file_name: &str,
) -> Result<(), String> {
    let path = home.join(file_name);
    let body = serde_json::json!({ "api_url": api_url, "api_key": api_key });
    let bytes =
        serde_json::to_vec_pretty(&body).map_err(|e| format!("serialize cloud_sync: {e}"))?;
    // Replace atomically-ish: write a temp then rename, keeping 0600 perms.
    let tmp = home.join(".cloud_sync.json.tmp");
    let _ = std::fs::remove_file(&tmp);
    host::write_owner_only(&tmp, &bytes).map_err(|e| format!("write {}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, &path).map_err(|e| format!("rename to {}: {e}", path.display()))?;
    sync_cloud_home_dir(home)?;
    Ok(())
}
