//! The static signed app index — the anonymous read surface of the LastDB app
//! registry (brain `decision-2026-09-19-app-registry-is-the-release-unit`).
//!
//! One JSON file per channel (`stable`, `next`) plus a detached Ed25519
//! signature. Each app row carries **compat rows**: `<app_version> @ <sha>`
//! proved with `lastdb <version>` by one named proof run. Install reads the
//! running node's build string and picks the newest row that names it. There
//! is no minimum-version rule: a minimum alone fails when the node removes
//! something, so only an exact proved pair counts.
//!
//! The index is served as a plain file (the Homebrew tap repo, or any static
//! host). A fresh install needs no account and no live service. Publish stays
//! DevCert-gated elsewhere; this module only signs, verifies, resolves, and
//! installs.
//!
//! Trust: the verifying key is pinned in this binary
//! ([`RELEASE_INDEX_PUBKEY_B64`]). An operator can override it for a test
//! index with `--trust-key` or `LASTDB_REGISTRY_TRUST_KEY` (a base64 key or a
//! path to a file holding one); the override is printed, never silent.

use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use std::time::Duration;

use app_identity_crypto::{
    key_id, sign as ed25519_sign, verify as ed25519_verify, verifying_key_from_base64, SigningKey,
    VerifyingKey, SIGNATURE_LEN,
};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use fold_db::hex::sha256_hex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// The index grammar this binary reads and writes.
pub const INDEX_VERSION: u32 = 1;

/// The channel a public install reads when none is named.
pub const DEFAULT_CHANNEL: &str = "stable";

/// Where the public index lives: the Homebrew tap repo, served raw by GitHub.
/// `<base>/<channel>.json` and `<base>/<channel>.json.sig`.
pub const DEFAULT_INDEX_BASE_URL: &str =
    "https://raw.githubusercontent.com/EdgeVector/homebrew-lastdb/main/registry";

/// The Ed25519 verifying key that signs the public index. Generated
/// 2026-09-20 (`lastdb app dev-init --key-file ~/.lastdb/registry-index-signing.key`
/// on the release host); the same key is published as
/// `registry/index-signing.pub` in the tap repo so a reader can cross-check.
pub const RELEASE_INDEX_PUBKEY_B64: &str = "Aw807ErGvi7+cJ0RhSvRbAt9UWwKOwrfAPvAYF4fNdQ=";

/// Environment override for the index location (a URL base or a directory).
pub const INDEX_LOCATION_ENV: &str = "LASTDB_REGISTRY_INDEX";
/// Environment override for the trusted verifying key.
pub const TRUST_KEY_ENV: &str = "LASTDB_REGISTRY_TRUST_KEY";

const HTTP_TIMEOUT: Duration = Duration::from_secs(30);

// ─── Index types ──────────────────────────────────────────────────────────

/// One proved pair: this app version at this commit, proved with this node
/// build by this proof run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompatRow {
    pub app_version: String,
    /// Full git commit of the app source that was proved.
    pub sha: String,
    /// Optional signed artifact pointer; `None` means install from source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact: Option<String>,
    /// The node build string (`lastdbd --version`, `/api/version` `build`).
    pub lastdb_version: String,
    /// The node's request grammar at proof time, when the proof recorded it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lastdb_api_version: Option<u32>,
    /// RFC 3339 UTC timestamp of the proof.
    pub proved_at: String,
    /// The proof run id (routine + run id, or a path the operator can open).
    pub proof_run: String,
}

/// One app on the shelf, with every proved pair the channel carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexApp {
    pub app_id: String,
    /// Public git checkout URL. Install clones this and checks out `sha`.
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub compat: Vec<CompatRow>,
}

/// One channel's index file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistryIndex {
    pub index_version: u32,
    pub channel: String,
    pub generated_at: String,
    #[serde(default)]
    pub apps: Vec<IndexApp>,
}

/// The detached signature file next to an index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexSignature {
    pub alg: String,
    pub key_id: String,
    /// Lowercase hex SHA-256 of the exact index bytes.
    pub payload_sha256: String,
    /// Base64 Ed25519 signature over the ASCII bytes of `payload_sha256`.
    pub sig: String,
}

// ─── Sign / verify ────────────────────────────────────────────────────────

/// Sign the exact bytes of an index file.
///
/// The digest covers the bytes; the signature covers the digest. A reader
/// hashes what it downloaded and refuses on either mismatch, so a signature
/// cannot be moved onto a different file.
#[must_use]
pub fn sign_index_bytes(key: &SigningKey, index_bytes: &[u8]) -> IndexSignature {
    let payload_sha256 = sha256_hex(index_bytes);
    let sig = ed25519_sign(key, payload_sha256.as_bytes());
    IndexSignature {
        alg: "ed25519".to_string(),
        key_id: key_id(&key.verifying_key()),
        payload_sha256,
        sig: BASE64.encode(sig),
    }
}

/// Verify a detached signature against the exact index bytes under `trust`.
///
/// # Errors
/// Names the first check that fails: algorithm, key id, digest, or signature.
pub fn verify_index_bytes(
    trust: &VerifyingKey,
    index_bytes: &[u8],
    signature: &IndexSignature,
) -> Result<(), String> {
    if signature.alg != "ed25519" {
        return Err(format!(
            "index signature alg is '{}', expected ed25519",
            signature.alg
        ));
    }
    let expected_key_id = key_id(trust);
    if signature.key_id != expected_key_id {
        return Err(format!(
            "index is signed by key {} but this binary trusts {}",
            signature.key_id, expected_key_id
        ));
    }
    let digest = sha256_hex(index_bytes);
    if signature.payload_sha256 != digest {
        return Err(format!(
            "index bytes hash to {digest} but the signature names {}",
            signature.payload_sha256
        ));
    }
    let raw = BASE64
        .decode(signature.sig.trim())
        .map_err(|e| format!("index signature is not valid base64: {e}"))?;
    let sig: [u8; SIGNATURE_LEN] = raw
        .try_into()
        .map_err(|_| "index signature is not 64 bytes".to_string())?;
    ed25519_verify(trust, &sig, digest.as_bytes())
        .map_err(|_| "index signature does not verify".to_string())
}

/// Parse and verify an index from its bytes and its detached signature.
///
/// # Errors
/// A bad signature is reported before a parse error: an unsigned file is
/// never read for content.
pub fn load_verified_index(
    trust: &VerifyingKey,
    index_bytes: &[u8],
    signature_bytes: &[u8],
) -> Result<RegistryIndex, String> {
    let signature: IndexSignature = serde_json::from_slice(signature_bytes)
        .map_err(|e| format!("index signature file does not parse: {e}"))?;
    verify_index_bytes(trust, index_bytes, &signature)?;
    parse_index(index_bytes)
}

/// Parse an index without a signature check. Only for `index show` and the
/// signer itself; every install path goes through [`load_verified_index`].
///
/// # Errors
/// Returns the parse error or an unsupported `index_version`.
pub fn parse_index(index_bytes: &[u8]) -> Result<RegistryIndex, String> {
    let index: RegistryIndex =
        serde_json::from_slice(index_bytes).map_err(|e| format!("index does not parse: {e}"))?;
    if index.index_version != INDEX_VERSION {
        return Err(format!(
            "index_version {} is not supported by this lastdb (reads {INDEX_VERSION}); run `brew upgrade lastdb`",
            index.index_version
        ));
    }
    Ok(index)
}

// ─── Trust ────────────────────────────────────────────────────────────────

/// Where the trusted verifying key came from, so the CLI can say so.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrustSource {
    Pinned,
    Flag,
    Env,
}

/// Resolve the verifying key: `--trust-key`, then `$LASTDB_REGISTRY_TRUST_KEY`,
/// then the pinned release key. Each override may be a base64 key or a path
/// to a file that holds one.
///
/// # Errors
/// Returns a parse error for a malformed override or a corrupt pinned key.
pub fn resolve_trust_key(flag: Option<&str>) -> Result<(VerifyingKey, TrustSource), String> {
    if let Some(raw) = flag {
        return Ok((parse_trust_key(raw, "--trust-key")?, TrustSource::Flag));
    }
    if let Ok(raw) = std::env::var(TRUST_KEY_ENV) {
        if !raw.trim().is_empty() {
            return Ok((parse_trust_key(&raw, TRUST_KEY_ENV)?, TrustSource::Env));
        }
    }
    let key = verifying_key_from_base64(RELEASE_INDEX_PUBKEY_B64)
        .map_err(|e| format!("pinned release index key is corrupt: {e:?}"))?;
    Ok((key, TrustSource::Pinned))
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

// ─── Location / fetch ─────────────────────────────────────────────────────

/// Where a channel's index files live.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexLocation {
    /// `<base>/<channel>.json` over HTTP(S).
    Url(String),
    /// `<dir>/<channel>.json` on the local filesystem.
    Dir(PathBuf),
}

impl IndexLocation {
    /// `--index` flag, then `$LASTDB_REGISTRY_INDEX`, then the public tap.
    /// A value that names an existing directory or starts with `file://` is a
    /// directory; anything else is a URL base.
    #[must_use]
    pub fn resolve(flag: Option<&str>) -> Self {
        let raw = flag
            .map(str::to_string)
            .or_else(|| std::env::var(INDEX_LOCATION_ENV).ok())
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_INDEX_BASE_URL.to_string());
        Self::from_raw(&raw)
    }

    #[must_use]
    pub fn from_raw(raw: &str) -> Self {
        let raw = raw.trim().trim_end_matches('/');
        if let Some(path) = raw.strip_prefix("file://") {
            return Self::Dir(PathBuf::from(path));
        }
        if raw.starts_with("http://") || raw.starts_with("https://") {
            return Self::Url(raw.to_string());
        }
        Self::Dir(PathBuf::from(raw))
    }

    /// The index file's address for `channel`, for receipts and messages.
    #[must_use]
    pub fn describe(&self, channel: &str) -> String {
        match self {
            Self::Url(base) => format!("{base}/{channel}.json"),
            Self::Dir(dir) => dir.join(format!("{channel}.json")).display().to_string(),
        }
    }

    /// Fetch `<channel>.json` and `<channel>.json.sig`.
    ///
    /// # Errors
    /// Returns the transport or filesystem error, naming the address.
    pub async fn fetch(&self, channel: &str) -> Result<(Vec<u8>, Vec<u8>), String> {
        validate_channel(channel)?;
        match self {
            Self::Dir(dir) => {
                let index_path = dir.join(format!("{channel}.json"));
                let sig_path = dir.join(format!("{channel}.json.sig"));
                let index = std::fs::read(&index_path)
                    .map_err(|e| format!("failed to read {}: {e}", index_path.display()))?;
                let sig = std::fs::read(&sig_path)
                    .map_err(|e| format!("failed to read {}: {e}", sig_path.display()))?;
                Ok((index, sig))
            }
            Self::Url(base) => {
                let client = reqwest::Client::builder()
                    .timeout(HTTP_TIMEOUT)
                    .no_proxy()
                    .build()
                    .map_err(|e| format!("failed to build HTTP client: {e}"))?;
                let index = fetch_url(&client, &format!("{base}/{channel}.json")).await?;
                let sig = fetch_url(&client, &format!("{base}/{channel}.json.sig")).await?;
                Ok((index, sig))
            }
        }
    }
}

async fn fetch_url(client: &reqwest::Client, url: &str) -> Result<Vec<u8>, String> {
    // trace-egress: propagate (anonymous static index read; no credential)
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|e| format!("GET {url} failed: {e}"))?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("GET {url} returned {}", status.as_u16()));
    }
    response
        .bytes()
        .await
        .map(|b| b.to_vec())
        .map_err(|e| format!("GET {url}: failed to read body: {e}"))
}

/// A channel name is one path segment: lowercase letters, digits, `-`.
///
/// # Errors
/// Returns a message naming the offending channel.
pub fn validate_channel(channel: &str) -> Result<(), String> {
    let ok = !channel.is_empty()
        && channel.len() <= 32
        && channel
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if ok {
        Ok(())
    } else {
        Err(format!(
            "invalid channel '{channel}' (lowercase letters, digits, '-'; at most 32)"
        ))
    }
}

// ─── Node version ─────────────────────────────────────────────────────────

/// The build string of the node this machine runs.
///
/// Order: `--lastdb-version` override, then `GET /api/version` on the owner
/// socket (the running daemon is the truth), then `lastdbd --version` from
/// the daemon next to this CLI (a node older than the handshake route), then
/// this binary's own build string (the CLI and the daemon ship in one bottle).
#[must_use]
pub fn running_lastdb_version(
    override_version: Option<&str>,
    socket: &Path,
) -> (String, &'static str) {
    if let Some(v) = override_version.map(str::trim).filter(|v| !v.is_empty()) {
        return (v.to_string(), "flag");
    }
    if let Some(build) = socket_api_version_build(socket) {
        return (build, "socket");
    }
    if let Some(build) = sibling_lastdbd_version() {
        return (build, "lastdbd");
    }
    (
        crate::ops::crash_attribution::build_version().to_string(),
        "cli",
    )
}

/// `lastdbd --version` from the daemon that ships next to this CLI (same
/// bottle), then from `PATH`. A node that predates `/api/version` still has
/// a build string; this is how a pre-handshake node is named.
fn sibling_lastdbd_version() -> Option<String> {
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            candidates.push(dir.join("lastdbd"));
        }
    }
    candidates.push(PathBuf::from("lastdbd"));
    for candidate in candidates {
        let output = ProcessCommand::new(&candidate)
            .arg("--version")
            .env("RUST_LOG", "off")
            .output();
        let Ok(output) = output else { continue };
        if !output.status.success() {
            continue;
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        for line in stdout.lines() {
            if let Some(rest) = line.trim().strip_prefix("lastdbd ") {
                let build = rest.trim();
                if !build.is_empty() {
                    return Some(build.to_string());
                }
            }
        }
    }
    None
}

fn socket_api_version_build(socket: &Path) -> Option<String> {
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;
    let mut stream = UnixStream::connect(socket).ok()?;
    let timeout = Some(Duration::from_secs(5));
    stream.set_read_timeout(timeout).ok()?;
    stream.set_write_timeout(timeout).ok()?;
    stream
        .write_all(
            b"GET /api/version HTTP/1.1\r\nHost: localhost\r\nX-LastDB-Client: lastdb\r\nConnection: close\r\n\r\n",
        )
        .ok()?;
    let mut raw = String::new();
    stream.read_to_string(&mut raw).ok()?;
    let status: u16 = raw.split_whitespace().nth(1)?.parse().ok()?;
    if status != 200 {
        return None;
    }
    let body = raw.split_once("\r\n\r\n")?.1.trim();
    let start = body.find('{')?;
    let end = body.rfind('}')?;
    let payload: Value = serde_json::from_str(&body[start..=end]).ok()?;
    payload
        .get("build")
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|b| !b.is_empty())
}

// ─── Resolve ──────────────────────────────────────────────────────────────

/// What `lastdb app resolve` answers: the one proved row for this node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Resolution {
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

/// Pick the newest compat row of `app_id` that names `lastdb_version`
/// exactly. Newest is by `proved_at`, then `app_version` as a tie-break.
///
/// # Errors
/// Names the app when it is absent, and lists the node builds the app was
/// proved with when none matches, so the operator sees the remedy.
pub fn pick_row<'a>(
    index: &'a RegistryIndex,
    app_id: &str,
    lastdb_version: &str,
) -> Result<(&'a IndexApp, &'a CompatRow), String> {
    let app = index
        .apps
        .iter()
        .find(|a| a.app_id == app_id)
        .ok_or_else(|| format!("app '{app_id}' is not in the {} index", index.channel))?;
    let mut matches: Vec<&CompatRow> = app
        .compat
        .iter()
        .filter(|row| row.lastdb_version == lastdb_version)
        .collect();
    if matches.is_empty() {
        let mut proved: Vec<&str> = app
            .compat
            .iter()
            .map(|r| r.lastdb_version.as_str())
            .collect();
        proved.sort_unstable();
        proved.dedup();
        return Err(format!(
            "no {} row for app '{app_id}' was proved with lastdb {lastdb_version} (proved with: {}); run `brew upgrade lastdb` or wait for the next proved set",
            index.channel,
            if proved.is_empty() {
                "none".to_string()
            } else {
                proved.join(", ")
            }
        ));
    }
    matches.sort_by(|a, b| {
        a.proved_at
            .cmp(&b.proved_at)
            .then_with(|| a.app_version.cmp(&b.app_version))
    });
    let row = matches
        .pop()
        .ok_or_else(|| "compat row selection produced no row".to_string())?;
    Ok((app, row))
}

/// Fetch, verify, and pick: the whole anonymous resolve in one call.
///
/// # Errors
/// Any fetch, signature, parse, or selection failure, with its cause.
pub async fn resolve(
    location: &IndexLocation,
    channel: &str,
    trust: &VerifyingKey,
    trust_source: &TrustSource,
    app_id: &str,
    lastdb_version: &str,
    lastdb_version_source: &str,
) -> Result<Resolution, String> {
    let (index_bytes, sig_bytes) = location.fetch(channel).await?;
    let index = load_verified_index(trust, &index_bytes, &sig_bytes)?;
    if index.channel != channel {
        return Err(format!(
            "index at {} says channel '{}' but '{channel}' was requested",
            location.describe(channel),
            index.channel
        ));
    }
    let (app, row) = pick_row(&index, app_id, lastdb_version)?;
    Ok(Resolution {
        app_id: app.app_id.clone(),
        channel: channel.to_string(),
        source: app.source.clone(),
        lastdb_version: lastdb_version.to_string(),
        lastdb_version_source: lastdb_version_source.to_string(),
        app_version: row.app_version.clone(),
        sha: row.sha.clone(),
        artifact: row.artifact.clone(),
        proved_at: row.proved_at.clone(),
        proof_run: row.proof_run.clone(),
        index: location.describe(channel),
        trust: match trust_source {
            TrustSource::Pinned => "pinned".to_string(),
            TrustSource::Flag => "flag".to_string(),
            TrustSource::Env => format!("env:{TRUST_KEY_ENV}"),
        },
    })
}

/// Fetch and verify a whole channel for `list` / `info`.
///
/// # Errors
/// Any fetch, signature, or parse failure.
pub async fn fetch_verified(
    location: &IndexLocation,
    channel: &str,
    trust: &VerifyingKey,
) -> Result<RegistryIndex, String> {
    let (index_bytes, sig_bytes) = location.fetch(channel).await?;
    load_verified_index(trust, &index_bytes, &sig_bytes)
}

// ─── Install by proof ─────────────────────────────────────────────────────

/// The receipt `lastdb app install` leaves in the install directory.
pub const RECEIPT_FILE: &str = "lastdb-app-install.json";

/// What a pinned install produced.
#[derive(Debug, Clone, Serialize)]
pub struct PinnedInstallOutcome {
    pub app_id: String,
    pub app_version: String,
    pub sha: String,
    pub lastdb_version: String,
    pub channel: String,
    pub source: String,
    pub install_dir: String,
    pub checkout_path: String,
    pub proof_run: String,
}

/// Outcome of `lastdb app upgrade` on the proved path.
#[derive(Debug, Clone, Serialize)]
pub struct PinnedUpgradeOutcome {
    pub app_id: String,
    pub installed_sha: String,
    pub resolved_sha: String,
    pub installed_version: String,
    pub resolved_version: String,
    pub upgraded: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub install: Option<PinnedInstallOutcome>,
}

#[derive(Debug, Deserialize)]
struct PinnedReceipt {
    #[serde(default)]
    sha: String,
    #[serde(default)]
    version: String,
}

/// Clone `resolution.source` and check out `resolution.sha` under
/// `install_dir/source`, then write the receipt.
///
/// The checkout is verified by `git rev-parse HEAD`; a clone that lands on a
/// different commit is removed, not installed.
///
/// # Errors
/// Filesystem and git failures, and a HEAD that is not the proved commit.
pub fn install_pinned(
    resolution: &Resolution,
    install_dir: &Path,
    force: bool,
) -> Result<PinnedInstallOutcome, String> {
    if !resolution.sha.chars().all(|c| c.is_ascii_hexdigit()) || resolution.sha.len() < 7 {
        return Err(format!(
            "compat row sha '{}' is not a git commit",
            resolution.sha
        ));
    }
    let checkout_path = install_dir.join("source");
    if install_dir.exists() {
        if !force {
            return Err(format!(
                "install directory already exists: {} (pass --force to replace it)",
                install_dir.display()
            ));
        }
        std::fs::remove_dir_all(install_dir).map_err(|e| {
            format!(
                "failed to remove existing install directory {}: {e}",
                install_dir.display()
            )
        })?;
    }
    std::fs::create_dir_all(install_dir).map_err(|e| {
        format!(
            "failed to create install directory {}: {e}",
            install_dir.display()
        )
    })?;

    let cleanup = |why: String| -> String {
        let _ = std::fs::remove_dir_all(install_dir);
        why
    };

    let clone = ProcessCommand::new("git")
        .arg("clone")
        .arg("--quiet")
        .arg("--no-checkout")
        .arg(&resolution.source)
        .arg(&checkout_path)
        .status()
        .map_err(|e| cleanup(format!("failed to run git clone: {e}")))?;
    if !clone.success() {
        return Err(cleanup(format!(
            "git clone failed for app '{}' from {}",
            resolution.app_id, resolution.source
        )));
    }
    let checkout = ProcessCommand::new("git")
        .arg("-C")
        .arg(&checkout_path)
        .arg("checkout")
        .arg("--quiet")
        .arg("--detach")
        .arg(&resolution.sha)
        .status()
        .map_err(|e| cleanup(format!("failed to run git checkout: {e}")))?;
    if !checkout.success() {
        return Err(cleanup(format!(
            "git checkout {} failed for app '{}': the proved commit is not in {}",
            resolution.sha, resolution.app_id, resolution.source
        )));
    }
    let head = ProcessCommand::new("git")
        .arg("-C")
        .arg(&checkout_path)
        .arg("rev-parse")
        .arg("HEAD")
        .output()
        .map_err(|e| cleanup(format!("failed to run git rev-parse: {e}")))?;
    let head = String::from_utf8_lossy(&head.stdout).trim().to_string();
    if !head.starts_with(&resolution.sha) && !resolution.sha.starts_with(&head) {
        return Err(cleanup(format!(
            "checkout HEAD is {head} but the proved commit is {}",
            resolution.sha
        )));
    }

    let receipt = json!({
        "app_id": resolution.app_id,
        "version": resolution.app_version,
        "sha": head,
        "lastdb_version": resolution.lastdb_version,
        "channel": resolution.channel,
        "source": resolution.source,
        "proved_at": resolution.proved_at,
        "proof_run": resolution.proof_run,
        "index": resolution.index,
        "trust": resolution.trust,
        "installed_at": chrono::Utc::now().to_rfc3339(),
    });
    let receipt_path = install_dir.join(RECEIPT_FILE);
    let bytes = serde_json::to_vec_pretty(&receipt)
        .map_err(|e| cleanup(format!("failed to encode install receipt: {e}")))?;
    std::fs::write(&receipt_path, bytes)
        .map_err(|e| cleanup(format!("failed to write {}: {e}", receipt_path.display())))?;

    Ok(PinnedInstallOutcome {
        app_id: resolution.app_id.clone(),
        app_version: resolution.app_version.clone(),
        sha: head,
        lastdb_version: resolution.lastdb_version.clone(),
        channel: resolution.channel.clone(),
        source: resolution.source.clone(),
        install_dir: install_dir.display().to_string(),
        checkout_path: checkout_path.display().to_string(),
        proof_run: resolution.proof_run.clone(),
    })
}

/// Reinstall only when the resolved commit differs from the installed one.
///
/// # Errors
/// A corrupt receipt or any [`install_pinned`] failure.
pub fn upgrade_pinned(
    resolution: &Resolution,
    install_dir: &Path,
) -> Result<PinnedUpgradeOutcome, String> {
    let receipt_path = install_dir.join(RECEIPT_FILE);
    if !receipt_path.exists() {
        let install = install_pinned(resolution, install_dir, false)?;
        return Ok(PinnedUpgradeOutcome {
            app_id: resolution.app_id.clone(),
            installed_sha: String::new(),
            resolved_sha: install.sha.clone(),
            installed_version: String::new(),
            resolved_version: resolution.app_version.clone(),
            upgraded: true,
            install: Some(install),
        });
    }
    let raw = std::fs::read_to_string(&receipt_path)
        .map_err(|e| format!("failed to read {}: {e}", receipt_path.display()))?;
    let receipt: PinnedReceipt = serde_json::from_str(&raw)
        .map_err(|e| format!("invalid install receipt {}: {e}", receipt_path.display()))?;
    let same = !receipt.sha.is_empty()
        && (receipt.sha.starts_with(&resolution.sha) || resolution.sha.starts_with(&receipt.sha));
    if same {
        return Ok(PinnedUpgradeOutcome {
            app_id: resolution.app_id.clone(),
            installed_sha: receipt.sha,
            resolved_sha: resolution.sha.clone(),
            installed_version: receipt.version,
            resolved_version: resolution.app_version.clone(),
            upgraded: false,
            install: None,
        });
    }
    let install = install_pinned(resolution, install_dir, true)?;
    Ok(PinnedUpgradeOutcome {
        app_id: resolution.app_id.clone(),
        installed_sha: receipt.sha,
        resolved_sha: install.sha.clone(),
        installed_version: receipt.version,
        resolved_version: resolution.app_version.clone(),
        upgraded: true,
        install: Some(install),
    })
}
