//! Anonymous installation from a KMS-signed S3 list for one exact API version.
//! Every entry also carries a publisher Ed25519 signature over RFC 8785 fields.
//! Verified cached lists survive failed downloads and reject sequence rollback.

use std::path::{Path, PathBuf};
use std::time::Duration;

use app_identity_crypto::{verifying_key_from_base64, VerifyingKey};
use serde::Serialize;

mod install;
mod list_cache;
pub mod managed;
mod managed_manifest;
pub mod managed_poll;

mod managed_links {
    pub(crate) use crate::app_registry_index::managed_manifest::{
        flip, install, restore, snapshot, validate_sources, verify,
    };
}

mod managed_process {
    pub(crate) use crate::app_registry_index::managed_poll::steps;
}

mod managed_publisher {
    pub(crate) use crate::app_registry_index::managed_manifest::authorized_transition;
}

pub use app_identity_crypto::{
    valid_app_id, valid_source, validate_channel, InterfaceList, ListEntry,
    REGISTRY_LIST_PUBKEY_B64,
};
pub use install::{install_pinned, running_node, RECEIPT_FILE};
pub use list_cache::fetch_list;

/// The channel a public install reads when none is named.
pub const DEFAULT_CHANNEL: &str = "stable";

/// Public list base. Fetch `<base>/<channel>/api/<api_version>.json` and `.sig`.
pub const DEFAULT_INDEX_BASE_URL: &str =
    crate::profile::endpoints::registry_index_url_for(crate::profile::endpoints::Environment::Prod);

/// Environment override for the index location (a URL base or a directory).
pub const INDEX_LOCATION_ENV: &str = "LASTDB_REGISTRY_INDEX";
/// Environment override for the trusted verifying key.
pub const TRUST_KEY_ENV: &str = "LASTDB_REGISTRY_TRUST_KEY";

const HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// Where a channel's index files live.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexLocation {
    /// `<base>/<channel>.json` over HTTP(S).
    Url(String),
    /// `<dir>/<channel>.json` on the local filesystem.
    Dir(PathBuf),
}

impl IndexLocation {
    /// Address of the list for one exact interface version.
    #[must_use]
    pub fn describe_interface(&self, channel: &str, api: u32) -> String {
        let path = app_identity_crypto::interface_list_path(channel, api);
        match self {
            Self::Url(base) => format!("{base}/{path}"),
            Self::Dir(dir) => dir.join(path).display().to_string(),
        }
    }

    /// Download one list and its detached signature. No credentials are sent.
    ///
    /// # Errors
    /// An invalid channel, insecure URL, or a failed transport or filesystem read.
    pub async fn fetch_interface(
        &self,
        channel: &str,
        api: u32,
    ) -> Result<(Vec<u8>, Vec<u8>), String> {
        validate_channel(channel)?;
        let address = self.describe_interface(channel, api);
        match self {
            Self::Dir(_) => {
                let bytes = std::fs::read(&address).map_err(|e| format!("{address}: {e}"))?;
                let sig = std::fs::read(format!("{address}.sig"))
                    .map_err(|e| format!("{address}.sig: {e}"))?;
                Ok((bytes, sig))
            }
            Self::Url(base) => {
                if !base.starts_with("https://") {
                    return Err("registry list URL must use HTTPS".into());
                }
                // trace-egress: propagate (anonymous static registry list endpoint)
                let client = reqwest::Client::builder()
                    .timeout(HTTP_TIMEOUT)
                    .no_proxy()
                    .redirect(reqwest::redirect::Policy::none())
                    .build()
                    .map_err(|e| e.to_string())?;
                let bytes = fetch_url(&client, &address).await?;
                let sig = fetch_url(&client, &format!("{address}.sig")).await?;
                Ok((bytes, sig))
            }
        }
    }

    /// `--index` flag, then `$LASTDB_REGISTRY_INDEX`, then the public tap.
    pub fn resolve(flag: Option<&str>) -> Result<Self, String> {
        let raw = flag
            .map(str::to_string)
            .or_else(|| std::env::var(INDEX_LOCATION_ENV).ok())
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_INDEX_BASE_URL.to_string());
        Self::from_raw(&raw)
    }

    /// # Errors
    /// A `lastsecrets://` locator is not an index location.
    pub fn from_raw(raw: &str) -> Result<Self, String> {
        let raw = raw.trim().trim_end_matches('/');
        if raw.starts_with("lastsecrets://") {
            return Err("registry index location does not accept a lastsecrets locator".into());
        }
        if let Some(path) = raw.strip_prefix("file://") {
            return Ok(Self::Dir(PathBuf::from(path)));
        }
        if raw.starts_with("http://") || raw.starts_with("https://") {
            return Ok(Self::Url(raw.to_string()));
        }
        Ok(Self::Dir(PathBuf::from(raw)))
    }
}

async fn fetch_url(client: &reqwest::Client, url: &str) -> Result<Vec<u8>, String> {
    // trace-egress: propagate (anonymous static index read; no credential)
    let mut response = client
        .get(url)
        .send()
        .await
        .map_err(|e| format!("GET {url} failed: {e}"))?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("GET {url} returned {}", status.as_u16()));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| format!("GET {url}: {e}"))?
    {
        if bytes.len().saturating_add(chunk.len()) > 2_097_152 {
            return Err(format!("GET {url}: registry object exceeds 2 MiB"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

/// Where the trusted verifying key came from, so the CLI can say so.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrustSource {
    Pinned,
    Flag,
    Env,
}

/// Resolve the KMS list verifying key (`--trust-key`, `$LASTDB_REGISTRY_TRUST_KEY`, or pinned).
pub fn resolve_list_trust_key(flag: Option<&str>) -> Result<(VerifyingKey, TrustSource), String> {
    if let Some(raw) = flag {
        return Ok((parse_trust_key(raw, "--trust-key")?, TrustSource::Flag));
    }
    if let Ok(raw) = std::env::var(TRUST_KEY_ENV) {
        if !raw.trim().is_empty() {
            return Ok((parse_trust_key(&raw, TRUST_KEY_ENV)?, TrustSource::Env));
        }
    }
    verifying_key_from_base64(REGISTRY_LIST_PUBKEY_B64)
        .map(|key| (key, TrustSource::Pinned))
        .map_err(|e| e.to_string())
}

fn parse_trust_key(raw: &str, what: &str) -> Result<VerifyingKey, String> {
    let raw = raw.trim();
    let text = if Path::new(raw).is_file() {
        std::fs::read_to_string(raw).map_err(|e| format!("{what}: failed to read {raw}: {e}"))?
    } else {
        raw.to_string()
    };
    verifying_key_from_base64(text.trim())
        .map_err(|e| format!("{what} is not a valid base64 Ed25519 public key: {e:?}"))
}

/// Contract version ranges this node supports.
#[derive(Debug, Clone, Copy)]
pub struct ContractVersions {
    pub api_version: u32,
    pub min_supported_api_version: u32,
    pub outbox_format_version: u32,
    pub min_supported_outbox_format_version: u32,
}

/// The running LastDB version and how it was determined.
#[derive(Debug, Clone)]
pub struct NodeVersion<'a> {
    pub version: &'a str,
    pub source: &'a str,
}

/// What `lastdb app list APP` answers: the one proved row for this node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Resolution {
    #[serde(skip)]
    pub list_entry: Option<ListEntry>,
    pub app_id: String,
    pub channel: String,
    pub source: String,
    pub lastdb_version: String,
    pub lastdb_version_source: String,
    pub app_version: String,
    pub sha: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact: Option<String>,
    pub proved_at: String,
    pub proof_run: String,
    pub index: String,
    pub trust: String,
}

/// Fetch the signed interface list and select one app.
pub async fn resolve(
    location: &IndexLocation,
    channel: &str,
    trust: &VerifyingKey,
    trust_source: &TrustSource,
    app_id: &str,
    node_version: &NodeVersion<'_>,
    versions: &ContractVersions,
) -> Result<Resolution, String> {
    let index = fetch_list(location, channel, versions.api_version, trust).await?;
    let entry = index
        .apps
        .into_iter()
        .find(|entry| {
            entry.app_id == app_id
                && entry
                    .outbox_format_version
                    .is_none_or(|v| v == versions.outbox_format_version)
        })
        .ok_or_else(|| {
            format!(
                "no {channel} app '{app_id}' was proved with lastdb API {} (build {})",
                versions.api_version, node_version.version
            )
        })?;
    Ok(Resolution {
        list_entry: Some(entry.clone()),
        app_id: entry.app_id.clone(),
        channel: channel.to_string(),
        source: entry.source.clone(),
        lastdb_version: node_version.version.to_string(),
        lastdb_version_source: node_version.source.to_string(),
        app_version: entry.app_version,
        sha: entry.sha,
        artifact: None,
        proved_at: entry.published_at,
        proof_run: String::new(),
        index: location.describe_interface(channel, versions.api_version),
        trust: format!(
            "{}:{}",
            match trust_source {
                TrustSource::Pinned => "pinned".to_string(),
                TrustSource::Flag => "flag".to_string(),
                TrustSource::Env => format!("env:{TRUST_KEY_ENV}"),
            },
            app_identity_crypto::key_id(trust)
        ),
    })
}
