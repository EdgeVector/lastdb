//! Signed registry interface lists, rollback-safe caching, and app resolution.

use std::path::{Path, PathBuf};
use std::time::Duration;

use app_identity_crypto::{verifying_key_from_base64, VerifyingKey};
use serde::{Deserialize, Serialize};

mod install;
pub mod managed;
mod managed_manifest;
pub mod managed_poll;

pub use app_identity_crypto::{
    valid_app_id, valid_source, validate_channel, InterfaceList, ListEntry,
    REGISTRY_LIST_PUBKEY_B64,
};
pub use install::{install_pinned, running_node, RECEIPT_FILE};

pub const DEFAULT_CHANNEL: &str = "stable";
pub const DEFAULT_INDEX_BASE_URL: &str =
    crate::profile::endpoints::registry_index_url_for(crate::profile::endpoints::Environment::Prod);
pub const INDEX_LOCATION_ENV: &str = "LASTDB_REGISTRY_INDEX";
pub const TRUST_KEY_ENV: &str = "LASTDB_REGISTRY_TRUST_KEY";

const HTTP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexLocation {
    Url(String),
    Dir(PathBuf),
}

impl IndexLocation {
    #[must_use]
    pub fn describe_interface(&self, channel: &str, api: u32) -> String {
        let path = app_identity_crypto::interface_list_path(channel, api);
        match self {
            Self::Url(base) => format!("{base}/{path}"),
            Self::Dir(dir) => dir.join(path).display().to_string(),
        }
    }

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

    pub fn resolve(flag: Option<&str>) -> Result<Self, String> {
        let raw = flag
            .map(str::to_string)
            .or_else(|| std::env::var(INDEX_LOCATION_ENV).ok())
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_INDEX_BASE_URL.to_string());
        Self::from_raw(&raw)
    }

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
    if !response.status().is_success() {
        return Err(format!("GET {url} returned {}", response.status().as_u16()));
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrustSource {
    Pinned,
    Flag,
    Env,
}

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

#[derive(Debug, Clone, Copy)]
pub struct ContractVersions {
    pub api_version: u32,
    pub min_supported_api_version: u32,
    pub outbox_format_version: u32,
    pub min_supported_outbox_format_version: u32,
}

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

#[derive(Serialize, Deserialize)]
struct StoredList {
    payload_sha256: String,
    bytes: Vec<u8>,
    signature: Vec<u8>,
}

pub async fn fetch_list(
    location: &IndexLocation,
    channel: &str,
    api: u32,
    trust: &VerifyingKey,
) -> Result<InterfaceList, String> {
    validate_channel(channel)?;
    let download = location
        .fetch_interface(channel, api)
        .await
        .and_then(|(bytes, signature)| {
            let list = InterfaceList::load(&bytes, &signature, trust, channel, api)?;
            Ok((
                StoredList {
                    payload_sha256: fold_db::hex::sha256_hex(&bytes),
                    bytes,
                    signature,
                },
                list,
            ))
        });

    let state = std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/state")))
        .ok_or("HOME or XDG_STATE_HOME is required for the registry cache")?;
    let base_root = state.join("lastdb-app-registry");
    let root = if matches!(location, IndexLocation::Url(base) if base == DEFAULT_INDEX_BASE_URL) {
        base_root
    } else {
        base_root.join("sources").join(fold_db::hex::sha256_hex(
            location.describe_interface(channel, api).as_bytes(),
        ))
    };
    let path = root.join(channel).join("api").join(format!("{api}.json"));
    let parent = path.parent().ok_or("registry cache parent missing")?;
    std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path.with_extension("lock"))
        .map_err(|e| e.to_string())?;
    lock.lock().map_err(|e| e.to_string())?;

    let cached = if path.exists() {
        let bytes = std::fs::read(&path).map_err(|e| e.to_string())?;
        let value: StoredList =
            serde_json::from_slice(&bytes).map_err(|e| format!("invalid registry cache: {e}"))?;
        if value.payload_sha256 != fold_db::hex::sha256_hex(&value.bytes) {
            return Err("registry cache digest mismatch".into());
        }
        let list = InterfaceList::load(&value.bytes, &value.signature, trust, channel, api)?;
        Some((value, list))
    } else {
        None
    };

    let accepted = accept_list(download, cached, &path, parent)?;
    if let Ok(generated) = chrono::DateTime::parse_from_rfc3339(&accepted.generated_at) {
        if chrono::Utc::now()
            .signed_duration_since(generated)
            .num_days()
            >= 7
        {
            eprintln!(
                "note: registry list was generated at {} (at least 7 days ago)",
                accepted.generated_at
            );
        }
    }
    Ok(accepted)
}

fn accept_list(
    download: Result<(StoredList, InterfaceList), String>,
    cached: Option<(StoredList, InterfaceList)>,
    path: &Path,
    parent: &Path,
) -> Result<InterfaceList, String> {
    let (value, list) = match download {
        Ok(value) => value,
        Err(error) => {
            return cached.map_or_else(
                || Err(error.clone()),
                |(_, prior)| {
                    eprintln!(
                        "note: registry download rejected; using stored seq {}: {error}",
                        prior.seq
                    );
                    Ok(prior)
                },
            );
        }
    };
    if let Some((prior, old)) = cached {
        if list.seq < old.seq
            || (list.seq == old.seq && value.payload_sha256 != prior.payload_sha256)
        {
            eprintln!(
                "note: registry sequence rejected; retaining stored seq {}",
                old.seq
            );
            return Ok(old);
        }
        if list.seq == old.seq {
            return Ok(old);
        }
    }
    let temp = path.with_extension(format!("{}.tmp", std::process::id()));
    std::fs::write(
        &temp,
        serde_json::to_vec(&value).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    std::fs::File::open(&temp)
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())?;
    std::fs::rename(&temp, path).map_err(|e| e.to_string())?;
    std::fs::File::open(parent)
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())?;
    Ok(list)
}
