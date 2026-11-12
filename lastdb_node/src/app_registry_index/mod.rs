//! Signed registry interface lists, rollback-safe caching, and node contract probing.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use std::time::Duration;

use app_identity_crypto::{verifying_key_from_base64, VerifyingKey};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub mod managed;
mod managed_manifest;

pub use app_identity_crypto::{
    valid_app_id, valid_source, validate_channel, InterfaceList, ListEntry,
    REGISTRY_LIST_PUBKEY_B64,
};

pub const RECEIPT_FILE: &str = "lastdb-app-install.json";
pub const DEFAULT_CHANNEL: &str = "stable";
pub const DEFAULT_INDEX_BASE_URL: &str =
    crate::profile::endpoints::registry_index_url_for(crate::profile::endpoints::Environment::Prod);
pub const INDEX_LOCATION_ENV: &str = "LASTDB_REGISTRY_INDEX";
pub const TRUST_KEY_ENV: &str = "LASTDB_REGISTRY_TRUST_KEY";

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
        let addr = self.describe_interface(channel, api);
        match self {
            Self::Dir(_) => Ok((
                std::fs::read(&addr).map_err(|e| format!("{addr}: {e}"))?,
                std::fs::read(format!("{addr}.sig")).map_err(|e| format!("{addr}.sig: {e}"))?,
            )),
            Self::Url(base) => {
                if !base.starts_with("https://") {
                    return Err("registry list URL must use HTTPS".into());
                }
                // trace-egress: propagate (anonymous static registry list endpoint)
                let client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(30))
                    .no_proxy()
                    .redirect(reqwest::redirect::Policy::none())
                    .build()
                    .map_err(|e| e.to_string())?;
                Ok((
                    fetch_url(&client, &addr).await?,
                    fetch_url(&client, &format!("{addr}.sig")).await?,
                ))
            }
        }
    }

    pub fn resolve(flag: Option<&str>) -> Result<Self, String> {
        let raw = flag
            .map(str::to_string)
            .or_else(|| std::env::var(INDEX_LOCATION_ENV).ok())
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_INDEX_BASE_URL.to_string());
        let raw = raw.trim().trim_end_matches('/');
        if raw.starts_with("lastsecrets://") {
            return Err("registry index location does not accept a lastsecrets locator".into());
        }
        if let Some(p) = raw.strip_prefix("file://") {
            return Ok(Self::Dir(PathBuf::from(p)));
        }
        if raw.starts_with("http://") || raw.starts_with("https://") {
            return Ok(Self::Url(raw.to_string()));
        }
        Ok(Self::Dir(PathBuf::from(raw)))
    }
}

async fn fetch_url(client: &reqwest::Client, url: &str) -> Result<Vec<u8>, String> {
    // trace-egress: propagate (anonymous static index read; no credential)
    let mut resp = client
        .get(url)
        .send()
        .await
        .map_err(|e| format!("GET {url} failed: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("GET {url} returned {}", resp.status().as_u16()));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = resp.chunk().await.map_err(|e| format!("GET {url}: {e}"))? {
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
    let parse = |raw: &str, what: &str| -> Result<VerifyingKey, String> {
        let raw = raw.trim();
        let text = if Path::new(raw).is_file() {
            std::fs::read_to_string(raw)
                .map_err(|e| format!("{what}: failed to read {raw}: {e}"))?
        } else {
            raw.to_string()
        };
        verifying_key_from_base64(text.trim())
            .map_err(|e| format!("{what} is not a valid base64 Ed25519 public key: {e:?}"))
    };
    if let Some(raw) = flag {
        return Ok((parse(raw, "--trust-key")?, TrustSource::Flag));
    }
    if let Ok(raw) = std::env::var(TRUST_KEY_ENV) {
        if !raw.trim().is_empty() {
            return Ok((parse(&raw, TRUST_KEY_ENV)?, TrustSource::Env));
        }
    }
    verifying_key_from_base64(REGISTRY_LIST_PUBKEY_B64)
        .map(|k| (k, TrustSource::Pinned))
        .map_err(|e| e.to_string())
}

#[derive(Debug, Clone, Copy)]
pub struct ContractVersions {
    pub api_version: u32,
    pub outbox_format_version: u32,
}

#[derive(Debug, Clone)]
pub struct Resolution {
    pub entry: ListEntry,
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
        let v: StoredList =
            serde_json::from_slice(&std::fs::read(&path).map_err(|e| e.to_string())?)
                .map_err(|e| format!("invalid registry cache: {e}"))?;
        if v.payload_sha256 != fold_db::hex::sha256_hex(&v.bytes) {
            return Err("registry cache digest mismatch".into());
        }
        let list = InterfaceList::load(&v.bytes, &v.signature, trust, channel, api)?;
        Some((v, list))
    } else {
        None
    };
    let accepted = accept_list(download, cached, &path, parent)?;
    if chrono::DateTime::parse_from_rfc3339(&accepted.generated_at)
        .is_ok_and(|g| chrono::Utc::now().signed_duration_since(g).num_days() >= 7)
    {
        eprintln!(
            "note: registry list was generated at {} (at least 7 days ago)",
            accepted.generated_at
        );
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
        Ok(v) => v,
        Err(error) => {
            let (_, prior) = cached.ok_or_else(|| error.clone())?;
            eprintln!(
                "note: registry download rejected; using stored seq {}: {error}",
                prior.seq
            );
            return Ok(prior);
        }
    };
    if let Some((prior, old)) = cached {
        if list.seq <= old.seq {
            if list.seq < old.seq || value.payload_sha256 != prior.payload_sha256 {
                eprintln!(
                    "note: registry sequence rejected; retaining stored seq {}",
                    old.seq
                );
            }
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

#[must_use]
pub fn running_node(socket: &Path) -> (String, &'static str, ContractVersions) {
    let mut versions = ContractVersions {
        api_version: crate::version::CURRENT_API_VERSION,
        outbox_format_version: crate::version::CURRENT_OUTBOX_FORMAT_VERSION,
    };
    if let Some(v) = socket_version(socket) {
        let u32_f = |k| v.get(k)?.as_u64().and_then(|n| u32::try_from(n).ok());
        if let (Some(build), Some(api), Some(outbox)) = (
            v.get("build").and_then(Value::as_str),
            u32_f("api_version"),
            u32_f("current_outbox_format_version"),
        ) {
            versions.api_version = api;
            versions.outbox_format_version = outbox;
            return (build.to_string(), "socket", versions);
        }
    }
    let sibling = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|d| d.join("lastdbd")))
        .into_iter()
        .chain([PathBuf::from("lastdbd")])
        .find_map(|c| {
            let out = ProcessCommand::new(c)
                .arg("--version")
                .env("RUST_LOG", "off")
                .output()
                .ok()?;
            out.status.success().then_some(())?;
            String::from_utf8_lossy(&out.stdout)
                .lines()
                .find_map(|l| l.trim().strip_prefix("lastdbd ").map(str::trim))
                .filter(|b| !b.is_empty())
                .map(str::to_string)
        });
    match sibling {
        Some(build) => (build, "lastdbd", versions),
        None => (crate::version::build_version().to_string(), "cli", versions),
    }
}

fn socket_version(socket: &Path) -> Option<Value> {
    let mut stream = UnixStream::connect(socket).ok()?;
    let timeout = Some(Duration::from_secs(5));
    stream.set_read_timeout(timeout).ok()?;
    stream.set_write_timeout(timeout).ok()?;
    stream
        .write_all(b"GET /api/version HTTP/1.1\r\nHost: localhost\r\nX-LastDB-Client: lastdb\r\nConnection: close\r\n\r\n")
        .ok()?;
    let mut raw = String::new();
    stream.read_to_string(&mut raw).ok()?;
    (raw.split_whitespace().nth(1)?.parse::<u16>().ok()? == 200).then_some(())?;
    let body = raw.split_once("\r\n\r\n")?.1.trim();
    serde_json::from_str(&body[body.find('{')?..=body.rfind('}')?]).ok()
}

pub(crate) fn authorized_transition(
    prior: &managed::ManagedReceipt,
    resolution: &Resolution,
) -> bool {
    use crate::profile::endpoints::{
        registry_ci_publisher_keys_for, registry_index_url_for, Environment,
    };
    let e = &resolution.entry;
    if !prior.trust.starts_with("pinned:")
        || !resolution.trust.starts_with("pinned:")
        || prior.app_id != e.app_id
        || prior.publisher != prior.listing.publisher_key
        || prior.listing.source != e.source
    {
        return false;
    }
    [Environment::Dev, Environment::Prod]
        .into_iter()
        .any(|env| {
            let source = match (env, e.app_id.as_str()) {
                (Environment::Prod, "brain" | "lastsecrets" | "search") => {
                    format!("https://github.com/EdgeVector/{}.git", e.app_id)
                }
                (Environment::Dev, "registry-daily-dev") => {
                    "https://github.com/EdgeVector/lastdb-app-registry.git".to_owned()
                }
                _ => return false,
            };
            let base = registry_index_url_for(env);
            let (owner, delegate) = registry_ci_publisher_keys_for(env);
            let ok_keys = (prior.publisher == owner && e.publisher_key == delegate)
                || (prior.publisher == delegate && e.publisher_key == owner);
            !delegate.is_empty()
                && !owner.is_empty()
                && ok_keys
                && e.source == source
                && prior.index == base
                && resolution.index == format!("{base}/{}/api/{}.json", e.channel, e.api_version)
        })
}
