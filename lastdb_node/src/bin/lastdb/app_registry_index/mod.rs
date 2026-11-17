//! Signed registry interface lists, rollback-safe caching, and node contract probing.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

use app_identity_crypto::{key_id, verifying_key_from_base64, VerifyingKey};
use serde::{Deserialize, Serialize};

pub mod managed;
mod managed_manifest;
mod manifest_access;

pub(crate) use manifest_access::ManifestAccess;

pub(crate) use crate::uds_client::running_node;
pub use app_identity_crypto::{
    valid_app_id, valid_source, validate_channel, InterfaceList, ListEntry,
};

pub const RECEIPT_FILE: &str = "lastdb-app-install.json";
pub const DEFAULT_CHANNEL: &str = "stable";
pub const TRUST_KEY_ENV: &str = "LASTDB_REGISTRY_TRUST_KEY";
const PROD_INDEX_BASE_URL: &str = lastdb_node::profile::endpoints::registry_index_url_for(
    lastdb_node::profile::endpoints::Environment::Prod,
);

/// Prefix of the error for a list the registry does not have. The list bucket
/// answers 403 for a missing object because anonymous readers cannot list it.
const ABSENT: &str = "registry list is absent";

pub(crate) fn str_err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

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

    async fn fetch_interface(&self, channel: &str, api: u32) -> Result<(Vec<u8>, Vec<u8>), String> {
        validate_channel(channel)?;
        let addr = self.describe_interface(channel, api);
        let sig = format!("{addr}.sig");
        if matches!(self, Self::Dir(_)) {
            if !Path::new(&addr).exists() {
                return Err(format!("{ABSENT}: {addr}"));
            }
            let b = std::fs::read(&addr).map_err(|e| format!("{addr}: {e}"))?;
            let s = std::fs::read(&sig).map_err(|e| format!("{sig}: {e}"))?;
            return Ok((b, s));
        }
        let Self::Url(base) = self else {
            unreachable!()
        };
        if !base.starts_with("https://") {
            return Err("registry list URL must use HTTPS".into());
        }
        let client = registry_http_client()?;
        let bytes = fetch_url(client, &addr).await?;
        // A list without its signature is a failed read, not an absent list.
        let signature = fetch_url(client, &sig)
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
        let default_url = lastdb_node::profile::endpoints::registry_index_url_for(target_env);
        let raw = raw.unwrap_or(default_url).trim_end_matches('/');
        if raw.starts_with("lastsecrets://") {
            return Err("registry index location does not accept a lastsecrets locator".into());
        }
        if raw.starts_with("http://") || raw.starts_with("https://") {
            return Ok(Self::Url(raw.to_string()));
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

async fn fetch_url(client: &reqwest::Client, url: &str) -> Result<Vec<u8>, String> {
    let resp = client
        .get(url)
        .send()
        .await
        .map_err(|e| format!("GET {url} failed: {e}"))?;
    let code = resp.status().as_u16();
    if code == 403 || code == 404 {
        return Err(format!("{ABSENT}: GET {url} returned {code}"));
    }
    if !resp.status().is_success() {
        return Err(format!("GET {url} returned {code}"));
    }
    lastdb_node::cloud_backup_client::read_bounded_bytes(resp, 2_097_152)
        .await
        .map_err(|e| format!("GET {url}: {e}"))
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
            IndexLocation::Url(base) => environment_for_registry_index_url(base)
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
    pub trust: String,
}

#[derive(Serialize, Deserialize)]
struct StoredList {
    payload_sha256: String,
    bytes: Vec<u8>,
    signature: Vec<u8>,
}

fn cache_file(location: &IndexLocation, channel: &str, api: u32) -> Result<PathBuf, String> {
    let state = std::env::var_os("XDG_STATE_HOME").map(PathBuf::from);
    let state =
        state.or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/state")));
    let mut root = state
        .ok_or("HOME or XDG_STATE_HOME is required for the registry cache")?
        .join("lastdb-app-registry");
    if !matches!(location, IndexLocation::Url(base) if base == PROD_INDEX_BASE_URL) {
        let key = fold_db::hex::sha256_hex(location.describe_interface(channel, api).as_bytes());
        root = root.join("sources").join(key);
    }
    Ok(root.join(channel).join("api").join(format!("{api}.json")))
}

async fn fetch_list(
    location: &IndexLocation,
    channel: &str,
    api: u32,
    trust: &VerifyingKey,
) -> Result<InterfaceList, String> {
    validate_channel(channel)?;
    let download = match location.fetch_interface(channel, api).await {
        Ok((bytes, signature)) => {
            let list = InterfaceList::load(&bytes, &signature, trust, channel, api)?;
            let payload_sha256 = fold_db::hex::sha256_hex(&bytes);
            Ok((
                StoredList {
                    payload_sha256,
                    bytes,
                    signature,
                },
                list,
            ))
        }
        Err(e) => Err(e),
    };
    let path = cache_file(location, channel, api)?;
    let parent = path.parent().ok_or("registry cache parent missing")?;
    std::fs::create_dir_all(parent).map_err(str_err)?;
    let mut opts = std::fs::OpenOptions::new();
    opts.create(true).truncate(false).read(true).write(true);
    let lock = opts.open(path.with_extension("lock")).map_err(str_err)?;
    lock.lock().map_err(str_err)?;
    let cached = if path.exists() {
        let raw = std::fs::read(&path).map_err(str_err)?;
        let v: StoredList =
            serde_json::from_slice(&raw).map_err(|e| format!("invalid registry cache: {e}"))?;
        if v.payload_sha256 != fold_db::hex::sha256_hex(&v.bytes) {
            return Err("registry cache digest mismatch".into());
        }
        Some((
            v.payload_sha256,
            InterfaceList::load(&v.bytes, &v.signature, trust, channel, api)?,
        ))
    } else {
        None
    };
    let accepted = match download {
        Ok((val, list)) => {
            if let Some((sha, old)) = cached.filter(|(_, old)| list.seq <= old.seq) {
                if list.seq < old.seq || val.payload_sha256 != sha {
                    eprintln!(
                        "note: registry sequence rejected; retaining stored seq {}",
                        old.seq
                    );
                }
                old
            } else {
                let tmp = path.with_extension(format!("{}.tmp", std::process::id()));
                std::fs::write(&tmp, serde_json::to_vec(&val).map_err(str_err)?)
                    .map_err(str_err)?;
                std::fs::File::open(&tmp)
                    .and_then(|f| f.sync_all())
                    .map_err(str_err)?;
                std::fs::rename(&tmp, &path).map_err(str_err)?;
                std::fs::File::open(parent)
                    .and_then(|f| f.sync_all())
                    .map_err(str_err)?;
                list
            }
        }
        Err(err) => {
            let Some((_, old)) = cached else {
                return Err(err);
            };
            eprintln!(
                "note: registry download rejected; using stored seq {}: {err}",
                old.seq
            );
            old
        }
    };
    let stale = chrono::DateTime::parse_from_rfc3339(&accepted.generated_at)
        .is_ok_and(|g| chrono::Utc::now().signed_duration_since(g).num_days() >= 7);
    if stale {
        eprintln!(
            "note: registry list was generated at {} (at least 7 days ago)",
            accepted.generated_at
        );
    }
    Ok(accepted)
}

/// The apps a node can install on one channel: for each app, its entry from the
/// highest list between the node's minimum supported API version and its own.
///
/// LastDB raises `API_VERSION` on every surface change and proves in CI that the
/// apps listed down to `MIN_SUPPORTED_API_VERSION` still run, so an app listed at
/// an older supported version needs no new listing. A level with no list is
/// skipped. Any other failure stops the lookup: skipping it could silently offer
/// an older release of an app whose newer list could not be read.
///
/// The returned list carries the header of the highest level found.
pub async fn fetch_supported(
    location: &IndexLocation,
    channel: &str,
    (min_api, api): (u32, u32),
    trust: &VerifyingKey,
) -> Result<InterfaceList, String> {
    let mut merged: Option<InterfaceList> = None;
    for level in (min_api.min(api)..=api).rev() {
        let list = match fetch_list(location, channel, level, trust).await {
            Ok(list) => list,
            Err(e) if e.starts_with(ABSENT) => continue,
            Err(e) => return Err(e),
        };
        if let Some(top) = merged.as_mut() {
            for entry in list.apps {
                if !top.apps.iter().any(|e| e.app_id == entry.app_id) {
                    top.apps.push(entry);
                }
            }
        } else {
            merged = Some(list);
        }
    }
    merged.ok_or_else(|| {
        format!("the registry has no {channel} list for lastdb API {min_api} to {api}")
    })
}

pub(crate) fn authorized_transition(
    prior: &managed::ManagedReceipt,
    resolution: &Resolution,
) -> bool {
    use lastdb_node::profile::endpoints::{
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
    for env in [Environment::Dev, Environment::Prod] {
        let src = match (env, e.app_id.as_str()) {
            (Environment::Prod, "brain" | "lastsecrets" | "search") => {
                format!("https://github.com/EdgeVector/{}.git", e.app_id)
            }
            (Environment::Dev, "registry-daily-dev") => {
                "https://github.com/EdgeVector/lastdb-app-registry.git".into()
            }
            _ => continue,
        };
        let base = registry_index_url_for(env);
        let (owner, del) = registry_ci_publisher_keys_for(env);
        let rotated = (prior.publisher == owner && e.publisher_key == del)
            || (prior.publisher == del && e.publisher_key == owner);
        if !del.is_empty()
            && !owner.is_empty()
            && rotated
            && e.source == src
            && prior.index == base
            && resolution.index == format!("{base}/{}/api/{}.json", e.channel, e.api_version)
        {
            return true;
        }
    }
    false
}
