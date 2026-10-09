use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Local primary storage engine for the FoldDB namespaced data path.
///
/// **Last Store only.** Sled was removed after the Mini cutover; configs or
/// `LASTDB_ENGINE=sled` fail at parse so operators never boot a missing backend.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageEngine {
    /// Last Store multi-collection document engine (sole product engine).
    #[default]
    #[serde(alias = "last_store", alias = "last-store")]
    Laststore,
}

impl StorageEngine {
    /// True when this is the serde default, so `skip_serializing_if` keeps the
    /// field out of written configs and a default engine does not churn them.
    ///
    /// Replaces the sled-era `is_sled`, which returned a constant `false` and so
    /// serialized `engine` unconditionally — the opposite of what the attribute
    /// existed to do. Comparing against `default()` stays correct if a second
    /// engine variant is ever added.
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }

    pub fn from_env() -> Result<Option<Self>, String> {
        match std::env::var("LASTDB_ENGINE") {
            Ok(raw) => raw.parse::<Self>().map(Some).map_err(|_| {
                format!(
                    "invalid LASTDB_ENGINE '{raw}'; only 'laststore' is supported (sled was removed)"
                )
            }),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => {
                Err("invalid LASTDB_ENGINE; value is not valid UTF-8".to_string())
            }
        }
    }
}

impl std::str::FromStr for StorageEngine {
    type Err = ();

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "laststore" | "last_store" | "last-store" => Ok(Self::Laststore),
            // "sled" and anything else: fail loudly (sled was removed).
            _ => Err(()),
        }
    }
}

/// Configuration for cloud sync (Exemem encrypted S3 backup).
///
/// **Field persistence model:** only `api_url` and `p2p_sync` are serialized
/// to disk. `api_key`, `session_token`, and `user_hash` are per-device
/// secrets that live in the host application's credential store (e.g.
/// fold_db_node's `credentials.json` / `credentials.enc`) and are hydrated
/// into this struct at runtime — typically in the node-creation path
/// before the sync engine is built. They are marked
/// `#[serde(skip_serializing)]` so `node_config.json` is safe to back up
/// or share.
///
/// Existing config files that contain these fields (from before this
/// change) still deserialize cleanly — serde reads them, and the next
/// save strips them out.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CloudSyncConfig {
    /// Exemem API URL (sync routes at /api/sync/*). Persisted to disk.
    pub api_url: String,
    /// API key for authentication. Runtime-hydrated from the credential
    /// store; never persisted to `node_config.json`.
    #[serde(default, skip_serializing)]
    pub api_key: String,
    /// Session token for authenticated API access. Runtime-hydrated.
    #[serde(default, skip_serializing)]
    pub session_token: Option<String>,
    /// User hash derived from public key. Runtime-hydrated.
    #[serde(default, skip_serializing)]
    pub user_hash: Option<String>,
    /// Optional ephemeral peer-to-peer device sync (free; uses R2 with
    /// 24h lifecycle). When `None`, p2p sync is disabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p2p_sync: Option<P2pSyncConfig>,
}

/// Configuration for ephemeral peer-to-peer sync between a single user's
/// devices.
///
/// Local writes from this device are appended to a small encrypted log and
/// pushed to each peer's `<user_hash>/p2p/<me>__<peer>/<seq>.enc` mailbox.
/// A background loop also polls each peer's outgoing mailbox
/// (`<user_hash>/p2p/<peer>__<me>/`) every `poll_interval_ms` and applies
/// new entries to the local store.
///
/// R2 lifecycle (server-side) expires p2p objects after 24h, so this is a
/// best-effort low-latency channel layered on top of the durable log /
/// snapshot sync — explicit ACK/delete is intentionally not implemented;
/// the 24h lifecycle reclaims storage.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct P2pSyncConfig {
    /// Other device IDs belonging to the same user. The local device pushes
    /// every local write to each of these peers' inbound mailboxes and pulls
    /// from each peer's outbound mailbox.
    #[serde(default)]
    pub peer_device_ids: Vec<String>,
    /// How often to poll each peer's outbound mailbox, in milliseconds.
    /// Default: 30_000 (30s).
    #[serde(default = "default_p2p_poll_interval_ms")]
    pub poll_interval_ms: u64,
}

fn default_p2p_poll_interval_ms() -> u64 {
    30_000
}

impl Default for P2pSyncConfig {
    fn default() -> Self {
        Self {
            peer_device_ids: Vec::new(),
            poll_interval_ms: default_p2p_poll_interval_ms(),
        }
    }
}

/// Storage configuration — always local Last Store, optionally with cloud sync.
///
/// Uses a custom deserializer to support both the new format and the legacy
/// `{"type": "local", ...}` / `{"type": "exemem", ...}` JSON formats.
#[derive(Clone, Debug, Serialize)]
pub struct DatabaseConfig {
    /// Path to the local database directory (Last Store root)
    pub path: PathBuf,
    /// Primary namespaced storage engine (Last Store only).
    #[serde(default, skip_serializing_if = "StorageEngine::is_default")]
    pub engine: StorageEngine,
    /// Optional cloud sync configuration (Exemem encrypted S3 backup)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloud_sync: Option<CloudSyncConfig>,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("data"),
            engine: StorageEngine::Laststore,
            cloud_sync: None,
        }
    }
}

impl DatabaseConfig {
    /// Create a local-only config
    pub fn local(path: PathBuf) -> Self {
        Self {
            path,
            engine: StorageEngine::Laststore,
            cloud_sync: None,
        }
    }

    /// Create a config with cloud sync enabled
    pub fn with_cloud_sync(path: PathBuf, cloud_sync: CloudSyncConfig) -> Self {
        Self {
            path,
            engine: StorageEngine::Laststore,
            cloud_sync: Some(cloud_sync),
        }
    }
}

/// Custom deserializer that handles both new format and legacy tagged enum format.
///
/// New format:
/// ```json
/// { "path": "/data", "cloud_sync": { "api_url": "...", "api_key": "..." } }
/// ```
///
/// Legacy formats (auto-migrated):
/// ```json
/// { "type": "local", "path": "/data" }
/// { "type": "exemem", "api_url": "...", "api_key": "..." }
/// ```
impl<'de> Deserialize<'de> for DatabaseConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;

        // Check if this is the legacy tagged format
        if let Some(type_tag) = value.get("type").and_then(|v| v.as_str()) {
            match type_tag {
                "local" => {
                    let path = value
                        .get("path")
                        .and_then(|v| v.as_str())
                        .map_or_else(|| PathBuf::from("data"), PathBuf::from);
                    Ok(Self {
                        path,
                        engine: StorageEngine::Laststore,
                        cloud_sync: None,
                    })
                }
                "exemem" => {
                    // `api_url` is the only field required on disk — the
                    // per-device secrets (api_key, session_token, user_hash)
                    // are runtime-hydrated from the credential store, so
                    // their absence is expected, not an error.
                    let api_url = value
                        .get("api_url")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| {
                            serde::de::Error::custom(
                                "exemem database config missing required string field 'api_url'",
                            )
                        })?
                        .to_string();
                    let api_key = value
                        .get("api_key")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string();
                    let session_token = value
                        .get("session_token")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);
                    let user_hash = value
                        .get("user_hash")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);

                    // Retained for backward compatibility: existing on-disk
                    // node_config.json files written by older versions of run.sh,
                    // org-test.sh, or smoke-test-dmg.sh use this shape and lack a
                    // `path` field. The path is recovered from FOLD_STORAGE_PATH.
                    // All write-side consumers have been migrated to the new
                    // `{path, cloud_sync}` format; this branch can be dropped once
                    // it's safe to assume no user configs remain in the old shape.
                    let path = std::env::var("FOLD_STORAGE_PATH")
                        .map_or_else(|_| PathBuf::from("data"), PathBuf::from);

                    Ok(Self {
                        path,
                        engine: StorageEngine::Laststore,
                        cloud_sync: Some(CloudSyncConfig {
                            api_url,
                            api_key,
                            session_token,
                            user_hash,
                            p2p_sync: None,
                        }),
                    })
                }
                other => Err(serde::de::Error::custom(format!(
                    "Unknown database type: '{other}'. Supported: 'local', 'exemem'"
                ))),
            }
        } else {
            // New format: direct struct fields
            let path = value
                .get("path")
                .and_then(|v| v.as_str())
                .map_or_else(|| PathBuf::from("data"), PathBuf::from);
            let engine = value
                .get("engine")
                .or_else(|| value.get("storage_engine"))
                .map(|v| serde_json::from_value::<StorageEngine>(v.clone()))
                .transpose()
                .map_err(serde::de::Error::custom)?
                .unwrap_or_default();

            // If `cloud_sync` is present, it must parse — silently dropping a
            // malformed block (e.g. missing `api_url`) would turn a broken
            // cloud-mode config into an apparently-healthy local-mode one.
            let cloud_sync = value
                .get("cloud_sync")
                .map(|v| serde_json::from_value::<CloudSyncConfig>(v.clone()))
                .transpose()
                .map_err(serde::de::Error::custom)?;

            Ok(Self {
                path,
                engine,
                cloud_sync,
            })
        }
    }
}
