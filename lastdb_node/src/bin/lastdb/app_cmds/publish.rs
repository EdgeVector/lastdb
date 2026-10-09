//! Themed module split from the parent: the developer key and the publish /
//! promote steps that talk to the registry service and Exemem.

// The handlers take the parsed CLI fields by value: the dispatcher destructures
// an owned `AppCommand`, so borrowing would only add clones at the call sites.
#![allow(clippy::needless_pass_by_value)]

use super::*;

use lastdb_node::app_publish;

/// The default developer key path unless `--key-file` names one.
pub(crate) fn resolve_key_file(home: &Path, key_file: Option<PathBuf>) -> PathBuf {
    key_file.unwrap_or_else(|| app_publish::default_dev_key_path(home))
}

/// Everything a signed registry write needs: both service URLs, the developer
/// API key, the signing key and the crypto environment.
pub(crate) struct DevPublishTarget {
    pub(crate) schema_service: String,
    pub(crate) exemem: String,
    pub(crate) api_key: String,
    pub(crate) dev_key: app_identity_crypto::SigningKey,
    pub(crate) crypto_env: app_identity_crypto::Env,
}

impl DevPublishTarget {
    /// Resolve in the order the commands always did: schema service, Exemem,
    /// API key, signing key, crypto environment. The first failure wins.
    pub(crate) fn resolve(
        home: &Path,
        env: Option<&str>,
        schema_url: Option<String>,
        api_url: Option<String>,
        key_file: Option<PathBuf>,
        api_key: Option<String>,
    ) -> Result<Self, String> {
        let schema_service = resolve_schema_service_url(schema_url, env)?;
        Self::resolve_with_schema_service(schema_service, home, env, api_url, key_file, api_key)
    }

    /// As [`Self::resolve`] when the caller already resolved the schema
    /// service URL (promote checks the catalog between the two steps).
    pub(crate) fn resolve_with_schema_service(
        schema_service: String,
        home: &Path,
        env: Option<&str>,
        api_url: Option<String>,
        key_file: Option<PathBuf>,
        api_key: Option<String>,
    ) -> Result<Self, String> {
        let exemem = resolve_exemem_url(api_url, env)?;
        let api_key = require_dev_api_key(api_key)?;
        let dev_key = app_publish::load_dev_key(&resolve_key_file(home, key_file))?;
        let crypto_env = crypto_env_for(env)?;
        Ok(Self {
            schema_service,
            exemem,
            api_key,
            dev_key,
            crypto_env,
        })
    }
}

pub(crate) fn app_dev_init(home: &Path, key_file: Option<PathBuf>) -> Result<(), String> {
    let path = resolve_key_file(home, key_file);
    let pubkey = app_publish::generate_dev_key(&path)?;
    println!("dev signing key written to {}", path.display());
    println!("dev_pubkey: {pubkey}");
    Ok(())
}

pub(crate) fn app_publish_command(
    home: &Path,
    manifest: &Path,
    env: Option<String>,
    schema_url: Option<String>,
    api_url: Option<String>,
    key_file: Option<PathBuf>,
    api_key: Option<String>,
) -> Result<(), String> {
    let manifest = app_publish::load_manifest(manifest)?;
    let target =
        DevPublishTarget::resolve(home, env.as_deref(), schema_url, api_url, key_file, api_key)?;
    let outcome = block_on_app(app_publish::publish_app(
        &target.schema_service,
        &target.exemem,
        &target.api_key,
        &target.dev_key,
        target.crypto_env,
        &manifest,
    ))?;
    println!(
        "published ({}): {}",
        outcome.status,
        serde_json::to_string_pretty(&outcome.response).unwrap_or_default()
    );
    Ok(())
}

pub(crate) fn app_promote(
    home: &Path,
    manifest_path: &Path,
    env: Option<String>,
    schema_url: Option<String>,
    api_url: Option<String>,
    key_file: Option<PathBuf>,
    api_key: Option<String>,
) -> Result<(), String> {
    let manifest = app_publish::load_manifest(manifest_path)?;
    let has_source = manifest
        .source
        .as_ref()
        .is_some_and(|s| !s.trim().is_empty());
    let has_artifact = manifest.artifact.is_some();
    if !has_source && !has_artifact {
        return Err(
            "promote rejected: manifest has neither `source` nor `artifact` — \
             live apps must be installable (set a git URL in source, or an artifact \
             pointer, re-publish/PUT, then promote). The service also enforces this \
             on the registry row."
                .into(),
        );
    }
    let locked = app_publish::load_lockfile(manifest_path);
    let schema_service = resolve_schema_service_url(schema_url, env.as_deref())?;
    ensure_app_schemas_catalog_ready(home, &schema_service, &manifest, &locked)?;
    let target = DevPublishTarget::resolve_with_schema_service(
        schema_service,
        home,
        env.as_deref(),
        api_url,
        key_file,
        api_key,
    )?;
    let outcome = block_on_app(app_publish::promote_app(
        &target.schema_service,
        &target.exemem,
        &target.api_key,
        &target.dev_key,
        target.crypto_env,
        &manifest,
    ))?;
    println!(
        "promoted: {}",
        serde_json::to_string_pretty(&outcome.response).unwrap_or_default()
    );
    Ok(())
}
