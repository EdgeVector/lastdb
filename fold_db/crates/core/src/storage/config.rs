use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Local primary storage engine for the FoldDB namespaced data path.
///
/// Last Store names the native packed engine. Normal construction has no
/// old-format backend selection or migration fallback.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageEngine {
    /// Native packed storage (the sole product engine).
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

/// Native local storage configuration. Cloud intent still decodes, but the
/// factory refuses activation until the native cloud gate is complete.
///
/// The decoder accepts the current fields directly and refuses obsolete
/// tagged configurations instead of converting their storage or cloud intent.
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
}

/// Decode the current configuration without a legacy conversion route.
/// ```json
/// { "path": "/data", "cloud_sync": { "api_url": "...", "api_key": "..." } }
/// ```
impl<'de> Deserialize<'de> for DatabaseConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        if !value.is_object() {
            return Err(serde::de::Error::custom(
                "database configuration must be an object",
            ));
        }
        if value.get("type").is_some() {
            return Err(serde::de::Error::custom(
                "obsolete tagged database configuration is unsupported; use path, engine, and cloud_sync fields",
            ));
        }
        current_config::<D::Error>(&value)
    }
}

/// Current fields retain their normal validation before factory activation.
fn current_config<E: serde::de::Error>(value: &serde_json::Value) -> Result<DatabaseConfig, E> {
    let path = value
        .get("path")
        .map(|value| serde_json::from_value::<PathBuf>(value.clone()))
        .transpose()
        .map_err(E::custom)?
        .unwrap_or_else(|| PathBuf::from("data"));
    let engine = value
        .get("engine")
        .or_else(|| value.get("storage_engine"))
        .map(|v| serde_json::from_value::<StorageEngine>(v.clone()))
        .transpose()
        .map_err(E::custom)?
        .unwrap_or_default();

    // If `cloud_sync` is present, it must parse — silently dropping a
    // malformed block (e.g. missing `api_url`) would turn a broken
    // cloud-mode config into an apparently-healthy local-mode one.
    let cloud_sync = value
        .get("cloud_sync")
        .map(|v| serde_json::from_value::<CloudSyncConfig>(v.clone()))
        .transpose()
        .map_err(E::custom)?;

    Ok(DatabaseConfig {
        path,
        engine,
        cloud_sync,
    })
}
