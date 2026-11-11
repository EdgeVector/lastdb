//! Developer-facing app manifest loading, signing keys, and schema coverage checks.

use std::collections::BTreeMap;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppMetadata {
    pub display_name: String,
    pub description: String,
    pub homepage_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon_url: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppManifest {
    pub app_id: String,
    pub metadata: AppMetadata,
    pub version: String,
    #[serde(default)]
    pub schemas: Vec<Value>,
    #[serde(default)]
    pub contract_surfaces: Vec<String>,
}

pub fn default_dev_key_path(home: &Path) -> PathBuf {
    home.join("dev-signing.key")
}

fn reject_locator(path: &Path) -> Result<(), String> {
    if path
        .to_str()
        .is_some_and(|v| v.starts_with("lastsecrets://"))
    {
        return Err("dev key path does not accept a lastsecrets locator".into());
    }
    Ok(())
}

pub fn generate_dev_key(path: &Path) -> Result<String, String> {
    reject_locator(path)?;
    if path.exists() {
        return Err(format!(
            "dev key already exists at {} — delete it first if you really mean to rotate",
            path.display()
        ));
    }
    let mut secret = [0u8; 32];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut secret))
        .map_err(|e| format!("failed to read /dev/urandom: {e}"))?;
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

pub fn load_manifest(path: &Path) -> Result<AppManifest, String> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read manifest {}: {e}", path.display()))?;
    let manifest: AppManifest = serde_json::from_str(&raw)
        .map_err(|e| format!("invalid manifest {}: {e}", path.display()))?;
    if manifest.app_id.trim().is_empty() {
        return Err("manifest app_id must be non-empty".into());
    }
    let trimmed = manifest.version.trim();
    if trimmed != manifest.version || trimmed.is_empty() {
        return Err(
            "version must be a non-empty SemVer string without surrounding whitespace".into(),
        );
    }
    semver::Version::parse(trimmed).map_err(|e| format!("version must be valid SemVer: {e}"))?;
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
        let name = schema
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("<unnamed>");
        if let Some(retention) = schema.get("retention") {
            ManifestRetention::from_value(retention)
                .map_err(|e| format!("manifest schema '{name}': {e}"))?;
        }
    }
    Ok(manifest)
}

#[derive(Debug, Clone)]
pub enum SchemaCoverage {
    Reuse {
        identity_hash: String,
    },
    Register {
        identity_hash: String,
    },
    WouldRegister {
        identity_hash: String,
        reason: Option<String>,
    },
    Novel {
        reason: String,
    },
}

impl SchemaCoverage {
    pub fn is_covered(&self) -> bool {
        !matches!(self, Self::Novel { .. })
    }

    fn hash(&self) -> Option<&str> {
        match self {
            Self::Reuse { identity_hash }
            | Self::Register { identity_hash }
            | Self::WouldRegister { identity_hash, .. } => Some(identity_hash),
            Self::Novel { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckMode {
    ReadOnly,
    Sync,
}

#[derive(Debug, Clone)]
pub struct CheckOutcome {
    pub schema_name: String,
    pub coverage: SchemaCoverage,
    pub retention: Option<Value>,
}

pub fn novel_schema_names(outcomes: &[CheckOutcome]) -> Vec<String> {
    outcomes
        .iter()
        .filter(|o| !o.coverage.is_covered())
        .map(|o| o.schema_name.clone())
        .collect()
}

pub fn missing_field_descriptions(raw: &Value) -> Vec<String> {
    let descs = raw.get("field_descriptions").and_then(Value::as_object);
    raw.get("fields")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter(|f| {
            descs
                .and_then(|d| d.get(*f))
                .and_then(Value::as_str)
                .is_none_or(|s| s.trim().is_empty())
        })
        .map(str::to_string)
        .collect()
}

pub fn catalog_collisions(outcomes: &[CheckOutcome]) -> Vec<(String, Vec<String>)> {
    let mut by_hash: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for o in outcomes {
        if let Some(h) = o.coverage.hash() {
            by_hash
                .entry(h.to_string())
                .or_default()
                .push(o.schema_name.clone());
        }
    }
    by_hash
        .into_iter()
        .filter(|(_, names)| names.len() > 1)
        .collect()
}

pub fn check_schemas_with(
    socket: &Path,
    manifest: &AppManifest,
    mode: CheckMode,
    confirm_expire_existing: bool,
) -> Result<Vec<CheckOutcome>, String> {
    let headers = || {
        format!(
            "X-LastDB-Client: lastdb\r\nX-LastDB-Request-Id: {}\r\n",
            uuid::Uuid::new_v4()
        )
    };
    let get_req = format!(
        "GET /api/system/auto-identity HTTP/1.1\r\nHost: localhost\r\n{}Connection: close\r\n\r\n",
        headers()
    );
    let default_timeout = Duration::from_secs(crate::exec::DEFAULT_CLI_UDS_TIMEOUT_SECS);
    let (status, id_payload) = socket_request(socket, get_req.as_bytes(), default_timeout)?;
    if status != 200 {
        return Err(format!("auto-identity returned {status}"));
    }
    let user_hash = id_payload["user_hash"]
        .as_str()
        .ok_or("auto-identity response missing user_hash")?;
    let admin_timeout = crate::exec::admin_handler_timeout();

    let mut outcomes = Vec::with_capacity(manifest.schemas.len());
    for schema in &manifest.schemas {
        let name = schema
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("<unnamed>")
            .to_string();
        let mut schema_val = schema.clone();
        let mut retention = schema_val
            .as_object_mut()
            .and_then(|e| e.remove("retention"));
        if let Some(entry) = retention.as_mut().filter(|_| confirm_expire_existing) {
            entry["confirm_expire_existing"] = Value::Bool(true);
        }
        let mut body = json!({
            "app_id": manifest.app_id, "schema": schema_val,
            "intent": if mode == CheckMode::ReadOnly { "check" } else { "catalog_sync" },
        });
        if let Some(retention) = retention {
            body["retention"] = retention;
        }
        let encoded =
            serde_json::to_vec(&body).map_err(|e| format!("failed to encode JSON: {e}"))?;
        let mut req = format!(
            "POST /api/apps/declare-schema HTTP/1.1\r\nHost: localhost\r\n{}X-User-Hash: {user_hash}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            headers(),
            encoded.len()
        )
        .into_bytes();
        req.extend_from_slice(&encoded);
        let (status, payload) = socket_request(socket, &req, admin_timeout)?;
        let mut coverage = classify_response(mode, status, &payload);
        if let Some(reason) = payload
            .pointer("/retention/blocked")
            .and_then(Value::as_str)
        {
            coverage = SchemaCoverage::Novel {
                reason: format!("retention refused: {reason}"),
            };
        }
        outcomes.push(CheckOutcome {
            schema_name: name,
            coverage,
            retention: payload.get("retention").cloned(),
        });
    }
    Ok(outcomes)
}

fn socket_request(socket: &Path, bytes: &[u8], timeout: Duration) -> Result<(u16, Value), String> {
    let mut stream = UnixStream::connect(socket)
        .map_err(|e| format!("failed to connect to daemon socket: {e}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|e| format!("socket read timeout: {e}"))?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|e| format!("socket write timeout: {e}"))?;
    stream
        .write_all(bytes)
        .map_err(|e| format!("failed to write request: {e}"))?;
    let mut raw = String::new();
    stream
        .read_to_string(&mut raw)
        .map_err(|e| format!("failed to read response: {e}"))?;
    let status = raw
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| {
            format!(
                "unparseable HTTP response: {}",
                observability::truncate::truncate_on_char_boundary(&raw, 80)
            )
        })?;
    let body = raw.split_once("\r\n\r\n").map_or("", |(_, b)| b.trim());
    if body.is_empty() {
        return Ok((status, Value::Null));
    }
    let parsed = serde_json::from_str(body).or_else(|_| match (body.find('{'), body.rfind('}')) {
        (Some(s), Some(e)) if e > s => serde_json::from_str(&body[s..=e]),
        _ => serde_json::from_str(body),
    });
    match parsed {
        Ok(v) => Ok((status, v)),
        Err(_) if status != 200 => Ok((status, json!({ "error": body }))),
        Err(e) => Err(format!("invalid JSON response body: {e}")),
    }
}

fn classify_response(mode: CheckMode, status: u16, payload: &Value) -> SchemaCoverage {
    if status != 200 {
        let detail = payload
            .get("error")
            .or_else(|| payload.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("cannot resolve against the catalog");
        return SchemaCoverage::Novel {
            reason: format!("declare returned {status}: {detail}"),
        };
    }
    let resolution = payload
        .get("resolution")
        .and_then(Value::as_str)
        .unwrap_or("");
    let identity_hash = payload
        .get("identity_hash")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
        .map(str::to_string);
    if mode == CheckMode::ReadOnly {
        if payload.get("dry_run").and_then(Value::as_bool) != Some(true) {
            return SchemaCoverage::Novel {
                reason: "node did not honour the read-only check intent (no dry_run=true); upgrade lastdbd or pass --sync".into(),
            };
        }
        return match (resolution, identity_hash) {
            ("reuse", Some(identity_hash)) => SchemaCoverage::Reuse { identity_hash },
            ("register", Some(identity_hash)) => SchemaCoverage::WouldRegister {
                identity_hash,
                reason: payload
                    .get("register_reason")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            },
            (other, _) => SchemaCoverage::Novel {
                reason: format!("check returned resolution '{other}' without an identity"),
            },
        };
    }
    if payload.get("bind_eligible").and_then(Value::as_bool) != Some(true) {
        return SchemaCoverage::Novel {
            reason: "declare result lacks bind_eligible=true".into(),
        };
    }
    match (resolution, identity_hash) {
        ("reuse", Some(identity_hash)) => SchemaCoverage::Reuse { identity_hash },
        ("register", Some(identity_hash)) => SchemaCoverage::Register { identity_hash },
        ("reuse" | "register", None) => SchemaCoverage::Novel {
            reason: format!("{resolution} result lacks identity_hash"),
        },
        (other, _) => SchemaCoverage::Novel {
            reason: format!("unrecognized declare resolution '{other}'"),
        },
    }
}
