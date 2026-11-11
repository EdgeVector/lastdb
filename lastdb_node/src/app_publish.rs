//! `lastdb app …` — developer-facing app registration + publish flow.
//!
//! Load app manifests, manage developer keys, and check schema coverage on
//! the local Mini daemon. Signed registry publication uses these helpers.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use app_identity_crypto::SigningKey;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use fold_db::schema::ManifestRetention;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

mod schema_check;

pub use schema_check::*;

/// Human-facing metadata carried in `lastdb-app.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppMetadata {
    /// Display name shown in consent prompts and app lists. <= 80 chars.
    pub display_name: String,
    /// One-paragraph description. <= 500 chars.
    pub description: String,
    /// Homepage URL. <= 200 chars.
    pub homepage_url: String,
    /// Optional icon URL. <= 200 chars.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon_url: Option<String>,
}

/// Validate that an app version is a non-empty SemVer string without padding.
pub fn validate_app_version(version: &str) -> Result<(), String> {
    let trimmed = version.trim();
    if trimmed != version || trimmed.is_empty() {
        return Err(
            "version must be a non-empty SemVer string without surrounding whitespace".into(),
        );
    }
    semver::Version::parse(trimmed).map_err(|e| format!("version must be valid SemVer: {e}"))?;
    Ok(())
}

/// Generate a new developer Ed25519 signing key at `path` (0600, base64 of
/// the 32-byte secret). Refuses to overwrite an existing key.
pub fn generate_dev_key(path: &Path) -> Result<String, String> {
    reject_locator(path)?;
    if path.exists() {
        return Err(format!(
            "dev key already exists at {} — delete it first if you really mean to rotate",
            path.display()
        ));
    }
    let mut secret = [0u8; 32];
    let mut urandom = std::fs::File::open("/dev/urandom")
        .map_err(|e| format!("failed to open /dev/urandom: {e}"))?;
    urandom
        .read_exact(&mut secret)
        .map_err(|e| format!("failed to read entropy: {e}"))?;
    let key = SigningKey::from_bytes(&secret);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create {}: {e}", parent.display()))?;
    }
    std::fs::write(path, BASE64.encode(secret))
        .map_err(|e| format!("failed to write {}: {e}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .map_err(|e| format!("failed to chmod {}: {e}", path.display()))?;
    }
    Ok(BASE64.encode(key.verifying_key().to_bytes()))
}

pub fn load_dev_key(path: &Path) -> Result<SigningKey, String> {
    reject_locator(path)?;
    let raw = std::fs::read_to_string(path).map_err(|e| {
        format!(
            "failed to read dev key {} (run `lastdb app dev-init` first): {e}",
            path.display()
        )
    })?;
    let bytes = BASE64
        .decode(raw.trim())
        .map_err(|e| format!("dev key {} is not valid base64: {e}", path.display()))?;
    let secret: [u8; 32] = bytes
        .try_into()
        .map_err(|_| format!("dev key {} is not a 32-byte Ed25519 secret", path.display()))?;
    Ok(SigningKey::from_bytes(&secret))
}

fn reject_locator(path: &Path) -> Result<(), String> {
    if path
        .to_str()
        .is_some_and(|value| value.starts_with("lastsecrets://"))
    {
        return Err("dev key path does not accept a lastsecrets locator".into());
    }
    Ok(())
}

/// Default developer key location under the resolved node home.
pub fn default_dev_key_path(home: &Path) -> PathBuf {
    home.join("dev-signing.key")
}

/// Headers every hand-built publish-path request sends: the client self-ID
/// for Mini request-ops telemetry plus a fresh per-request correlation ID.
pub(crate) fn client_headers() -> String {
    format!(
        "X-LastDB-Client: lastdb\r\nX-LastDB-Request-Id: {}\r\n",
        uuid::Uuid::new_v4()
    )
}

pub(crate) fn socket_auto_identity(socket: &Path) -> Result<String, String> {
    let request = format!(
        "GET /api/system/auto-identity HTTP/1.1\r\nHost: localhost\r\n{}Connection: close\r\n\r\n",
        client_headers()
    );
    let (status, payload) = socket_request(socket, request.as_bytes())?;
    if status != 200 {
        return Err(format!("auto-identity returned {status}"));
    }
    payload
        .get("user_hash")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| "auto-identity response missing user_hash".to_string())
}

pub(crate) fn socket_post_json(
    socket: &Path,
    path: &str,
    user_hash: &str,
    body: &Value,
) -> Result<(u16, Value), String> {
    let body = serde_json::to_vec(body).map_err(|e| format!("failed to encode JSON: {e}"))?;
    let mut request = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\n{}X-User-Hash: {user_hash}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        client_headers(),
        body.len()
    )
    .into_bytes();
    request.extend_from_slice(&body);
    let timeout = if path == "/api/apps/declare-schema" {
        fold_db::env_flag::var_parsed::<u64>("LASTDB_UDS_ADMIN_TIMEOUT_SECS")
            .filter(|value| *value > 0)
            .map_or(Duration::from_secs(600), Duration::from_secs)
    } else {
        Duration::from_secs(crate::exec::DEFAULT_CLI_UDS_TIMEOUT_SECS)
    };
    socket_request_with_timeout(socket, &request, timeout)
}

pub(crate) fn socket_request(socket: &Path, bytes: &[u8]) -> Result<(u16, Value), String> {
    socket_request_with_timeout(
        socket,
        bytes,
        Duration::from_secs(crate::exec::DEFAULT_CLI_UDS_TIMEOUT_SECS),
    )
}

pub(crate) fn socket_request_with_timeout(
    socket: &Path,
    bytes: &[u8],
    timeout: Duration,
) -> Result<(u16, Value), String> {
    let mut stream = UnixStream::connect(socket)
        .map_err(|e| format!("failed to connect to daemon socket: {e}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|e| format!("failed to set socket read timeout: {e}"))?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|e| format!("failed to set socket write timeout: {e}"))?;
    stream
        .write_all(bytes)
        .map_err(|e| format!("failed to write request: {e}"))?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|e| format!("failed to read response: {e}"))?;
    parse_http_response(&response)
}

/// Minimal HTTP/1.1 response parse: status code + JSON body (after the
/// header/body separator, tolerating chunked-free `Connection: close` bodies).
pub(crate) fn parse_http_response(raw: &str) -> Result<(u16, Value), String> {
    let status = raw
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| format!("unparseable HTTP response: {}", head(raw)))?;
    let body = raw.split_once("\r\n\r\n").map_or("", |(_, b)| b.trim());
    if body.is_empty() {
        return Ok((status, Value::Null));
    }
    // Tolerate chunked transfer encoding by extracting the outermost JSON
    // object if a plain parse fails.
    let payload = serde_json::from_str(body).or_else(|_| {
        let start = body.find('{');
        let end = body.rfind('}');
        match (start, end) {
            (Some(s), Some(e)) if e > s => serde_json::from_str(&body[s..=e]),
            _ => serde_json::from_str(body),
        }
    });
    match payload {
        Ok(v) => Ok((status, v)),
        Err(_) if status != 200 => Ok((status, json!({ "error": body }))),
        Err(e) => Err(format!("invalid JSON response body: {e}")),
    }
}

pub(crate) fn head(raw: &str) -> &str {
    observability::truncate::truncate_on_char_boundary(raw, 80)
}

/// App manifest consumed by `lastdb app …` (JSON file, conventionally
/// `lastdb-app.json`). `schemas` entries are declarative schema definitions
/// (`{ "name", "descriptive_name"?, "schema_type", "fields": [...] }`).
#[derive(Debug, Clone, Deserialize)]
pub struct AppManifest {
    pub app_id: String,
    pub metadata: AppMetadata,
    pub version: String,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub schemas: Vec<Value>,
    /// Contract surfaces this app depends on: "http" for HTTP API routes,
    /// "outbox" for IndexChangeBatch format.
    #[serde(default)]
    pub contract_surfaces: Vec<String>,
    /// HTTP API version this app requires.
    #[serde(default)]
    pub api_version: Option<u32>,
    /// Outbox format version this app requires.
    #[serde(default)]
    pub outbox_format_version: Option<u32>,
}

pub fn load_manifest(path: &Path) -> Result<AppManifest, String> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read manifest {}: {e}", path.display()))?;
    let manifest: AppManifest = serde_json::from_str(&raw)
        .map_err(|e| format!("invalid manifest {}: {e}", path.display()))?;
    if manifest.app_id.trim().is_empty() {
        return Err("manifest app_id must be non-empty".into());
    }
    validate_app_version(&manifest.version)?;

    // Validate contract_surfaces: only "http" and "outbox" are allowed
    for surface in &manifest.contract_surfaces {
        if surface != "http" && surface != "outbox" {
            return Err(format!(
                "manifest contract_surfaces contains unknown value '{surface}': only 'http' and 'outbox' are supported"
            ));
        }
    }

    if manifest.schemas.is_empty() {
        return Err(
            "manifest declares no schemas — a LastDB app manifest must list the schemas it uses"
                .into(),
        );
    }
    for schema in &manifest.schemas {
        manifest_schema_retention(schema)?;
    }
    Ok(manifest)
}

/// The validated `retention` of one manifest schema entry, if it has one.
pub fn manifest_schema_retention(schema: &Value) -> Result<Option<ManifestRetention>, String> {
    let name = schema
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("<unnamed>");
    schema
        .get("retention")
        .map(ManifestRetention::from_value)
        .transpose()
        .map_err(|e| format!("manifest schema '{name}': {e}"))
}
