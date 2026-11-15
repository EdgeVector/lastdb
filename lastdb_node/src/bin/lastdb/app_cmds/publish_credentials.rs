//! Developer signing-key guard, publish API-key resolution, and registry HTTP transport.

use super::PublishArgs;
use app_identity_crypto::SigningKey;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use lastdb_node::cloud_backup_client::build_shared_http_client;
use lastdb_node::profile::endpoints::{
    environment_for_exemem_api_url, environment_for_registry_api_url, registry_api_url_for,
    Environment,
};
use serde_json::Value;
use std::fs::OpenOptions;
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::time::Duration;
use zeroize::Zeroizing;

const MAX_DEV_KEY_BYTES: u64 = 256;

pub(super) fn load_dev_key(path: &Path) -> Result<SigningKey, String> {
    if path
        .to_str()
        .is_some_and(|v| v.starts_with("lastsecrets://"))
    {
        return Err("dev key path does not accept a lastsecrets locator".into());
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|e| format!("failed to open dev key {}: {e}", path.display()))?;
    let meta = file
        .metadata()
        .map_err(|e| format!("failed to inspect dev key {}: {e}", path.display()))?;
    if !meta.is_file()
        || meta.uid() != unsafe { libc::geteuid() }
        || meta.permissions().mode() & 0o077 != 0
    {
        return Err(format!(
            "dev key {} must be a private regular owner file (chmod 600)",
            path.display()
        ));
    }
    if meta.len() > MAX_DEV_KEY_BYTES {
        return Err(format!("dev key {} exceeds size limit", path.display()));
    }
    let mut raw = Zeroizing::new(Vec::new());
    file.take(MAX_DEV_KEY_BYTES + 1)
        .read_to_end(&mut raw)
        .map_err(|e| format!("failed to read dev key {}: {e}", path.display()))?;
    if raw.len() as u64 > MAX_DEV_KEY_BYTES {
        return Err(format!("dev key {} exceeds size limit", path.display()));
    }
    let text = Zeroizing::new(
        String::from_utf8(raw.to_vec())
            .map_err(|_| format!("dev key {} is not UTF-8", path.display()))?,
    );
    let decoded = Zeroizing::new(
        BASE64
            .decode(text.trim())
            .map_err(|e| format!("dev key {} is not valid base64: {e}", path.display()))?,
    );
    let secret: Zeroizing<[u8; 32]> = decoded
        .as_slice()
        .try_into()
        .map(Zeroizing::new)
        .map_err(|_| format!("dev key {} is not a 32-byte Ed25519 secret", path.display()))?;
    Ok(SigningKey::from_bytes(&secret))
}

pub(super) fn home_cloud_environment(home: &Path) -> Result<Option<Environment>, String> {
    let Some(creds) = lastdb_node::cloud::load_saved_credentials(home)? else {
        return Ok(None);
    };
    Ok(environment_for_exemem_api_url(&creds.api_url).or_else(|| {
        creds
            .connection
            .as_ref()
            .and_then(|c| match c.environment.as_str() {
                "dev" => Some(Environment::Dev),
                "prod" => Some(Environment::Prod),
                _ => None,
            })
    }))
}

fn has_explicit_api_key(input: &PublishArgs) -> bool {
    input
        .api_key
        .as_deref()
        .is_some_and(|k| !k.trim().is_empty())
        || std::env::var("EXEMEM_DEV_API_KEY").is_ok_and(|k| !k.trim().is_empty())
}

pub(super) fn resolve_target_environment(
    home: &Path,
    input: &PublishArgs,
    flag_env: Option<Environment>,
    home_env: Option<Environment>,
) -> Result<Environment, String> {
    let explicit_env = Environment::explicit_from_env();
    if flag_env.is_none() && !has_explicit_api_key(input) {
        if let (Some(explicit), Some(saved)) = (explicit_env, home_env) {
            if explicit != saved {
                return Err(format!(
                    "LASTDB_ENV={explicit} conflicts with saved {saved} cloud credentials in {} — \
                     pass --env {explicit} with --api-key, or unset LASTDB_ENV",
                    home.display()
                ));
            }
        }
    }
    Ok(flag_env
        .or(explicit_env)
        .or(home_env)
        .unwrap_or(Environment::Prod))
}

pub(super) fn resolve_registry_url(
    input: &PublishArgs,
    target_env: Environment,
) -> Result<String, String> {
    let non_empty = |s: &str| {
        let trimmed = s.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    };
    let explicit = input
        .registry_url
        .as_deref()
        .and_then(non_empty)
        .or_else(|| {
            std::env::var("LASTDB_APP_REGISTRY_URL")
                .ok()
                .and_then(|u| non_empty(&u))
        });
    let url = explicit.unwrap_or_else(|| registry_api_url_for(target_env).to_owned());
    if !url.starts_with("https://") {
        return Err("registry URL must use HTTPS".into());
    }
    Ok(url)
}

pub(super) fn resolve_publish_api_key(
    home: &Path,
    input: &PublishArgs,
    home_env: Option<Environment>,
    target_env: Environment,
    registry_url: &str,
) -> Result<(Zeroizing<String>, String), String> {
    if let Some(explicit) = input
        .api_key
        .as_deref()
        .map(str::trim)
        .filter(|k| !k.is_empty())
    {
        return Ok((Zeroizing::new(explicit.to_owned()), "--api-key".to_owned()));
    }
    if let Ok(env_key) = std::env::var("EXEMEM_DEV_API_KEY") {
        let trimmed = env_key.trim();
        if !trimmed.is_empty() {
            return Ok((
                Zeroizing::new(trimmed.to_owned()),
                "EXEMEM_DEV_API_KEY".to_owned(),
            ));
        }
    }
    if let Some(saved_env) = home_env {
        if saved_env != target_env {
            return Err(format!(
                "saved home credentials belong to {saved_env}, \
                 but publish targets {target_env} — pass --api-key or set EXEMEM_DEV_API_KEY for {target_env}"
            ));
        }
        if let Some(url_env) =
            environment_for_registry_api_url(registry_url).filter(|&e| e != saved_env)
        {
            return Err(format!(
                "saved home credentials belong to {saved_env}, \
                 but registry URL belongs to {url_env} — pass --api-key or set EXEMEM_DEV_API_KEY for {url_env}"
            ));
        }
    }
    if let Some(dev_key) = lastdb_node::cloud::load_saved_developer_key(home)? {
        let path = home.join(lastdb_node::cloud::DEVELOPER_API_KEY_FILE);
        return Ok((dev_key, path.display().to_string()));
    }
    if target_env == Environment::Dev
        && home_env == Some(Environment::Dev)
        && environment_for_registry_api_url(registry_url).is_none_or(|e| e == Environment::Dev)
    {
        if let Some(creds) = lastdb_node::cloud::load_saved_credentials(home)? {
            if !creds.api_key.trim().is_empty() {
                let path = home.join(lastdb_node::cloud::CLOUD_CREDENTIALS_FILE);
                return Ok((creds.api_key, path.display().to_string()));
            }
        }
    }
    Err(
        "no developer API key — pass --api-key, set EXEMEM_DEV_API_KEY, or run 'lastdb cloud developer-key create --save'"
            .into(),
    )
}

pub(super) async fn post_publish(
    url: &str,
    api_key: &str,
    key_source: &str,
    payload: &Value,
) -> Result<Value, String> {
    let endpoint = reqwest::Url::parse(&format!("{}/v1/apps", url.trim_end_matches('/')))
        .map_err(|_| "invalid registry URL".to_owned())?;
    if endpoint.scheme() != "https" || endpoint.host_str().is_none() {
        return Err("registry URL must use HTTPS".into());
    }
    let req = build_shared_http_client()
        .map_err(|e| format!("cannot construct registry HTTP client: {e}"))?
        .post(endpoint.clone())
        .timeout(Duration::from_secs(120))
        .bearer_auth(api_key)
        .json(payload);
    let resp = observability::propagation::inject_w3c(req)
        .send()
        .await
        .map_err(|e| format!("registry publish request failed: {e}"))?;
    if resp.status().is_redirection() || resp.url() != &endpoint {
        return Err("registry publish request redirected unexpectedly".into());
    }
    let status = resp.status();
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| format!("failed to read registry response: {e}"))?;
    if bytes.len() > 262_144 {
        return Err("registry response exceeds byte limit".into());
    }
    if !status.is_success() {
        return Err(format_publish_error(status.as_u16(), &bytes, key_source));
    }
    serde_json::from_slice(&bytes).map_err(|e| format!("registry response was not valid JSON: {e}"))
}

fn format_publish_error(code: u16, bytes: &[u8], key_source: &str) -> String {
    let parsed = serde_json::from_slice::<Value>(bytes).ok();
    let is_payment = lastdb_node::cloud::is_payment_subscription_error(code, bytes)
        || (code == 403
            && parsed.as_ref().is_some_and(|v| {
                v.get("code").and_then(Value::as_str) == Some("PAID_ACCOUNT_REQUIRED")
                    || v.get("field").and_then(Value::as_str) == Some("payment.subscription")
            }));
    if is_payment {
        return format!(
            "registry publish failed (HTTP 403, key from {key_source}): {}",
            lastdb_node::cloud::PAYMENT_REQUIRED_MESSAGE
        );
    }
    let detail = parsed.and_then(|v| {
        v.get("error")
            .or_else(|| v.get("message"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty() && s.len() <= 256 && !s.chars().any(char::is_control))
            .map(str::to_owned)
    });
    let auth_ctx = matches!(code, 401 | 403)
        .then(|| format!(", key from {key_source}"))
        .unwrap_or_default();
    detail.map_or_else(
        || format!("registry publish failed with HTTP status {code}{auth_ctx}"),
        |msg| format!("registry publish failed (HTTP {code}{auth_ctx}): {msg}"),
    )
}
