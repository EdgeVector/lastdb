//! Cloud sync config and environment pinning.

use super::*;

/// The Exemem environment this home is connected to, from the `api_url`
/// that explicit backup enrollment saved in the credential file (or the
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
