//! Themed module split from the parent: the signed release writes
//! (`release-publish`, `release-channel`, `release-revoke`).

// The handlers take the parsed CLI fields by value: the dispatcher destructures
// an owned `AppCommand`, so borrowing would only add clones at the call sites.
#![allow(clippy::needless_pass_by_value)]

use super::*;

#[allow(clippy::too_many_arguments)]
pub(crate) fn app_release_publish(
    home: &Path,
    manifest_path: &Path,
    app_uuid: &str,
    source_commit: &str,
    artifact: &Path,
    artifact_url: &str,
    env: Option<String>,
    schema_url: Option<String>,
    api_url: Option<String>,
    key_file: Option<PathBuf>,
    api_key: Option<String>,
    json: bool,
) -> Result<(), String> {
    use lastdb_node::app_publish;
    let app_manifest = app_publish::load_manifest(manifest_path)?;
    let artifact_bytes = std::fs::read(artifact)
        .map_err(|e| format!("failed to read {}: {e}", artifact.display()))?;
    let dev_key = app_publish::load_dev_key(&resolve_key_file(home, key_file))?;
    let release = app_publish::build_release_manifest(
        &app_manifest,
        manifest_path,
        app_uuid,
        source_commit,
        &artifact_bytes,
        artifact_url,
        &dev_key,
    )?;
    let schema_service = resolve_schema_service_url(schema_url, env.as_deref())?;
    let exemem = resolve_exemem_url(api_url, env.as_deref())?;
    let key = require_dev_api_key(api_key)?;
    let crypto_env = crypto_env_for(env.as_deref())?;
    let response = block_on_app(app_publish::publish_release(
        &schema_service,
        &exemem,
        &key,
        &dev_key,
        crypto_env,
        &release,
    ))?;
    if json {
        print_pretty_json(&response);
    } else {
        println!(
            "release published: {}",
            response
                .get("release_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("<none>")
        );
        println!("  artifact_digest: {}", release.artifact_digest);
        println!("  source_commit: {}", release.source_commit);
        println!("  locked schemas: {}", release.schemas.len());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn app_release_channel(
    home: &Path,
    app_id: &str,
    channel: &str,
    release_id: &str,
    generation: Option<u64>,
    env: Option<String>,
    schema_url: Option<String>,
    api_url: Option<String>,
    key_file: Option<PathBuf>,
    api_key: Option<String>,
    json: bool,
) -> Result<(), String> {
    use lastdb_node::app_publish;
    let target =
        DevPublishTarget::resolve(home, env.as_deref(), schema_url, api_url, key_file, api_key)?;
    // No `--generation`: read the channel and use what it reports. A
    // channel that does not exist yet is generation 0.
    let generation = if let Some(generation) = generation {
        generation
    } else {
        let registry =
            lastdb_node::app_release_host::ReleaseRegistryClient::new(&target.schema_service);
        block_on_app(async { Ok(registry.get_channel(app_id, channel).await) })?
            .map_or(0, |read| read.generation)
    };
    let response = block_on_app(app_publish::set_release_channel(
        &target.schema_service,
        &target.exemem,
        &target.api_key,
        &target.dev_key,
        target.crypto_env,
        app_id,
        channel,
        release_id,
        generation,
    ))?;
    if json {
        print_pretty_json(&response);
    } else {
        println!("channel {app_id}/{channel} -> {release_id}");
        println!(
            "  generation: {}",
            response
                .get("generation")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default()
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn app_release_revoke(
    home: &Path,
    app_id: &str,
    release_id: &str,
    reason: Option<&str>,
    env: Option<String>,
    schema_url: Option<String>,
    api_url: Option<String>,
    key_file: Option<PathBuf>,
    api_key: Option<String>,
    json: bool,
) -> Result<(), String> {
    let target =
        DevPublishTarget::resolve(home, env.as_deref(), schema_url, api_url, key_file, api_key)?;
    let response = block_on_app(lastdb_node::app_publish::revoke_release(
        &target.schema_service,
        &target.exemem,
        &target.api_key,
        &target.dev_key,
        target.crypto_env,
        app_id,
        release_id,
        reason,
    ))?;
    if json {
        print_pretty_json(&response);
    } else {
        println!("revoked: {release_id}");
    }
    Ok(())
}
