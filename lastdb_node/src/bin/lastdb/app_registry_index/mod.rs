//! Signed registry interface lists, rollback-safe caching, and node contract probing.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use std::time::Duration;

use app_identity_crypto::{
    key_id, verifying_key_from_base64, VerifyingKey, REGISTRY_LIST_PUBKEY_B64,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub mod managed;
mod managed_manifest;

pub use app_identity_crypto::{
    valid_app_id, valid_source, validate_channel, InterfaceList, ListEntry,
};

pub const RECEIPT_FILE: &str = "lastdb-app-install.json";
pub const DEFAULT_CHANNEL: &str = "stable";
pub const TRUST_KEY_ENV: &str = "LASTDB_REGISTRY_TRUST_KEY";
const DEFAULT_INDEX_BASE_URL: &str = lastdb_node::profile::endpoints::registry_index_url_for(
    lastdb_node::profile::endpoints::Environment::Prod,
);

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
        // trace-egress: propagate (anonymous static registry list endpoint)
        let b = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .no_proxy();
        let client = b
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(str_err)?;
        Ok((
            fetch_url(&client, &addr).await?,
            fetch_url(&client, &sig).await?,
        ))
    }

    pub fn resolve(flag: Option<&str>) -> Result<Self, String> {
        let env = std::env::var("LASTDB_REGISTRY_INDEX").ok();
        let raw = flag
            .or(env.as_deref())
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let raw = raw.unwrap_or(DEFAULT_INDEX_BASE_URL).trim_end_matches('/');
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

async fn fetch_url(client: &reqwest::Client, url: &str) -> Result<Vec<u8>, String> {
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

pub fn resolve_list_trust_key(flag: Option<&str>) -> Result<(VerifyingKey, String), String> {
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
        let k = verifying_key_from_base64(REGISTRY_LIST_PUBKEY_B64).map_err(str_err)?;
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
    if !matches!(location, IndexLocation::Url(base) if base == DEFAULT_INDEX_BASE_URL) {
        let key = fold_db::hex::sha256_hex(location.describe_interface(channel, api).as_bytes());
        root = root.join("sources").join(key);
    }
    Ok(root.join(channel).join("api").join(format!("{api}.json")))
}

pub async fn fetch_list(
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

#[must_use]
pub fn running_node(home: &Path) -> (String, &'static str, u32, u32) {
    if let Some(v) = socket_version(&lastdb_node::lastdb_uds::socket_path(&home.join("data"))) {
        let u32_f = |k| v.get(k)?.as_u64().and_then(|n| u32::try_from(n).ok());
        if let (Some(b), Some(a), Some(o)) = (
            v["build"].as_str(),
            u32_f("api_version"),
            u32_f("current_outbox_format_version"),
        ) {
            return (b.to_string(), "socket", a, o);
        }
    }
    let (api, outbox) = (
        lastdb_node::version::CURRENT_API_VERSION,
        lastdb_node::version::CURRENT_OUTBOX_FORMAT_VERSION,
    );
    let sibling = std::env::current_exe()
        .ok()
        .and_then(|e| e.parent().map(|p| p.join("lastdbd")));
    for c in sibling.into_iter().chain([PathBuf::from("lastdbd")]) {
        let Ok(out) = ProcessCommand::new(c)
            .arg("--version")
            .env("RUST_LOG", "off")
            .output()
        else {
            continue;
        };
        if !out.status.success() {
            continue;
        }
        for l in String::from_utf8_lossy(&out.stdout).lines() {
            if let Some(b) = l
                .trim()
                .strip_prefix("lastdbd ")
                .map(str::trim)
                .filter(|b| !b.is_empty())
            {
                return (b.to_string(), "lastdbd", api, outbox);
            }
        }
    }
    (
        lastdb_node::version::build_version().to_string(),
        "cli",
        api,
        outbox,
    )
}

fn socket_version(socket: &Path) -> Option<Value> {
    let mut s = UnixStream::connect(socket).ok()?;
    let t = Some(Duration::from_secs(5));
    s.set_read_timeout(t).ok()?;
    s.set_write_timeout(t).ok()?;
    s.write_all(b"GET /api/version HTTP/1.1\r\nHost: localhost\r\nX-LastDB-Client: lastdb\r\nConnection: close\r\n\r\n").ok()?;
    let mut raw = String::new();
    s.read_to_string(&mut raw).ok()?;
    (raw.split_whitespace().nth(1) == Some("200")).then_some(())?;
    let body = raw.split_once("\r\n\r\n")?.1.trim();
    serde_json::from_str(&body[body.find('{')?..=body.rfind('}')?]).ok()
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
