//! Signed registry interface lists, rollback-safe caching, and node contract probing.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

use app_identity_crypto::{key_id, verifying_key_from_base64, VerifyingKey};

mod command_names;
mod fetch_cache;
mod install_poll;
pub mod launch;
pub mod managed;
mod managed_manifest;
mod managed_steps;
mod place_schemas;
pub mod private_lists;
mod register_grant;
pub mod remove;
mod transition;

pub(crate) use crate::uds_client::running_node;
pub use app_identity_crypto::{
    valid_app_id, valid_source, validate_channel, InterfaceList, ListEntry,
};
pub use fetch_cache::fetch_supported;
pub(crate) use transition::authorized_transition;

pub const RECEIPT_FILE: &str = "lastdb-app-install.json";
pub const DEFAULT_CHANNEL: &str = "stable";
pub const TRUST_KEY_ENV: &str = "LASTDB_REGISTRY_TRUST_KEY";
pub const NO_LIST_PREFIX: &str = "the registry has no ";
pub(super) const PROD_INDEX_BASE_URL: &str =
    lastdb_node::profile::endpoints::registry_index_url_for(
        lastdb_node::profile::endpoints::Environment::Prod,
    );

/// Prefix of the error for a list the registry does not have. The list bucket
/// answers 403 for a missing object because anonymous readers cannot list it.
pub(super) const ABSENT: &str = "registry list is absent";

pub(crate) fn str_err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexLocation {
    Url {
        base: String,
        locator: Option<String>,
    },
    Dir(PathBuf),
}

impl IndexLocation {
    #[must_use]
    pub fn has_explicit_source(flag: Option<&str>) -> bool {
        flag.is_some_and(|s| !s.trim().is_empty())
            || std::env::var("LASTDB_REGISTRY_INDEX").is_ok_and(|s| !s.trim().is_empty())
    }

    #[must_use]
    pub fn raw_url_base(&self) -> Option<&str> {
        match self {
            Self::Url { base, .. } => Some(base.as_str()),
            Self::Dir(_) => None,
        }
    }

    pub(super) fn fetch_address(&self, channel: &str, api: u32) -> String {
        let path = app_identity_crypto::interface_list_path(channel, api);
        match self {
            Self::Url { base, .. } => format!("{base}/{path}"),
            Self::Dir(dir) => dir.join(path).display().to_string(),
        }
    }

    #[must_use]
    pub fn describe_interface(&self, channel: &str, api: u32) -> String {
        let path = app_identity_crypto::interface_list_path(channel, api);
        match self {
            Self::Url {
                locator: Some(loc), ..
            } => format!("{loc}/{path}"),
            Self::Url { base, .. } => {
                if let Some((env, _)) =
                    lastdb_node::profile::endpoints::private_list_prefix_for(base)
                {
                    format!("private:{env}/{path}")
                } else {
                    format!("{base}/{path}")
                }
            }
            Self::Dir(dir) => dir.join(path).display().to_string(),
        }
    }

    pub(super) async fn fetch_interface(
        &self,
        channel: &str,
        api: u32,
    ) -> Result<(Vec<u8>, Vec<u8>), String> {
        validate_channel(channel)?;
        let addr = self.fetch_address(channel, api);
        let shown = self.describe_interface(channel, api);
        let sig = format!("{addr}.sig");
        let shown_sig = format!("{shown}.sig");
        if matches!(self, Self::Dir(_)) {
            if !Path::new(&addr).exists() {
                return Err(format!("{ABSENT}: {shown}"));
            }
            let b = std::fs::read(&addr).map_err(|e| format!("{shown}: {e}"))?;
            let s = std::fs::read(&sig).map_err(|e| format!("{shown_sig}: {e}"))?;
            return Ok((b, s));
        }
        let Self::Url { base, .. } = self else {
            unreachable!()
        };
        if !base.starts_with("https://") {
            return Err("registry list URL must use HTTPS".into());
        }
        let client = registry_http_client()?;
        let bytes = fetch_url(client, &addr, &shown).await?;
        // A list without its signature is a failed read, not an absent list.
        let signature = fetch_url(client, &sig, &shown_sig)
            .await
            .map_err(|e| e.replacen(ABSENT, "registry list signature is missing", 1))?;
        Ok((bytes, signature))
    }

    pub fn resolve(
        flag: Option<&str>,
        env_override: Option<lastdb_node::profile::endpoints::Environment>,
    ) -> Result<Self, String> {
        let env = std::env::var("LASTDB_REGISTRY_INDEX").ok();
        let raw = flag
            .or(env.as_deref())
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let target_env = env_override
            .unwrap_or_else(lastdb_node::profile::endpoints::Environment::from_env_or_prod);
        let Some(raw) = raw else {
            let base = lastdb_node::profile::endpoints::registry_index_url_for(target_env)
                .trim_end_matches('/')
                .to_owned();
            return Ok(Self::Url {
                base,
                locator: None,
            });
        };
        let raw = raw.trim_end_matches('/');
        if raw == "private" || raw.starts_with("private:") {
            let (_resolved_env, locator, base) = private_lists::resolve_locator(raw, target_env)?;
            return Ok(Self::Url {
                base,
                locator: Some(locator),
            });
        }
        if raw.starts_with("lastsecrets://") {
            return Err(
                "registry index location does not accept a lastsecrets locator; run 'lastdb app show <app> --save' to save a private list locator"
                    .into(),
            );
        }
        if raw.starts_with("http://") || raw.starts_with("https://") {
            return Ok(Self::Url {
                base: raw.to_string(),
                locator: None,
            });
        }
        Ok(Self::Dir(PathBuf::from(
            raw.strip_prefix("file://").unwrap_or(raw),
        )))
    }
}

fn registry_http_client() -> Result<&'static reqwest::Client, String> {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    if let Some(client) = CLIENT.get() {
        return Ok(client);
    }
    // trace-egress: propagate (anonymous static registry list endpoint)
    let built = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(0)
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(str_err)?;
    let _ = CLIENT.set(built);
    CLIENT.get().ok_or_else(|| "HTTP client unavailable".into())
}

async fn fetch_url(client: &reqwest::Client, url: &str, shown: &str) -> Result<Vec<u8>, String> {
    let resp = client
        .get(url)
        .send()
        .await
        .map_err(|e| format!("GET {shown} failed: {}", e.without_url()))?;
    let code = resp.status().as_u16();
    if code == 403 || code == 404 {
        return Err(format!("{ABSENT}: GET {shown} returned {code}"));
    }
    if !resp.status().is_success() {
        return Err(format!("GET {shown} returned {code}"));
    }
    lastdb_node::cloud_backup_client::read_bounded_bytes(resp, 2_097_152)
        .await
        .map_err(|e| format!("GET {shown}: {e}"))
}

pub fn resolve_list_trust_key(
    flag: Option<&str>,
    location: &IndexLocation,
    env_override: Option<lastdb_node::profile::endpoints::Environment>,
) -> Result<(VerifyingKey, String), String> {
    use lastdb_node::profile::endpoints::{
        environment_for_registry_index_url, registry_list_key_for, Environment,
    };
    let env = std::env::var(TRUST_KEY_ENV)
        .ok()
        .filter(|s| !s.trim().is_empty());
    let (raw, what, note, tag) = if let Some(r) = flag {
        (r, "--trust-key", "--trust-key", "flag")
    } else if let Some(r) = env.as_deref() {
        (
            r,
            TRUST_KEY_ENV,
            "$LASTDB_REGISTRY_TRUST_KEY",
            "env:LASTDB_REGISTRY_TRUST_KEY",
        )
    } else {
        let target = match location {
            IndexLocation::Url { base, .. } => environment_for_registry_index_url(base)
                .or(env_override)
                .unwrap_or_else(Environment::from_env_or_prod),
            IndexLocation::Dir(_) => env_override.unwrap_or_else(Environment::from_env_or_prod),
        };
        let k = verifying_key_from_base64(registry_list_key_for(target)).map_err(str_err)?;
        return Ok((k, format!("pinned:{}", key_id(&k))));
    };
    let raw = raw.trim();
    let text = if Path::new(raw).is_file() {
        std::fs::read_to_string(raw).map_err(|e| format!("{what}: failed to read {raw}: {e}"))?
    } else {
        raw.to_string()
    };
    let k = verifying_key_from_base64(text.trim())
        .map_err(|e| format!("{what} is not a valid base64 Ed25519 public key: {e:?}"))?;
    eprintln!("note: index trust key overridden by {note}");
    Ok((k, format!("{tag}:{}", key_id(&k))))
}

#[derive(Debug, Clone)]
pub struct Resolution {
    pub entry: ListEntry,
    pub index: String,
    pub raw_base: Option<String>,
    pub trust: String,
}
