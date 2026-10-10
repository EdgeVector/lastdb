//! Install-owned schema resolver wiring for Mini schema resolution.
//!
//! Loads optional `{home}/schema_resolver.json` (+ env overrides), builds a
//! [`LocalFirstSchemaResolver`] in LiveOnly mode. Pack-backed modes are still
//! parsed for compatibility, but Mini no longer ships an in-process embedder.

use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use app_identity_crypto::{Env, SigningKey};
use folddb_profile::endpoints::Environment;
use schema_service_client::types::{SchemaResolveProposal, SchemaResolveResult};
use schema_service_client::{
    DisabledEmbeddingModel, FacadeProposal, FacadeResolveItem, LocalFirstMode,
    LocalFirstSchemaResolver, NoopPackStore, SchemaServiceClient, SharedSurfacePublishRequest,
    SharedSurfacePublishResult,
};
use schema_service_core::{
    schema_resolver_enforce_gate_failures, schema_resolver_enforce_gate_pass,
    SchemaResolverEnforceGateReport,
};

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

/// File name under the node home.
pub const SCHEMA_RESOLVER_CONFIG_FILE: &str = "schema_resolver.json";

fn schema_service_signature_env() -> Env {
    match Environment::from_env() {
        Environment::Dev => Env::Dev,
        Environment::Prod => Env::Prod,
    }
}

pub(crate) fn schema_service_client_with_node_identity(
    home: &Path,
    client: SchemaServiceClient,
) -> Result<SchemaServiceClient, String> {
    let seed = lastdb_identity::load_seed(home)?
        .ok_or_else(|| format!("missing node identity at {}/identity.key", home.display()))?;
    Ok(client.with_node_identity(
        SigningKey::from_bytes(&seed),
        schema_service_signature_env(),
    ))
}

fn schema_publish_client(
    home: &Path,
    schema_service_url: &str,
    timeout: Duration,
) -> Result<SchemaServiceClient, String> {
    schema_service_client_with_node_identity(
        home,
        SchemaServiceClient::new_with_timeout(schema_service_url, timeout),
    )
}

const OWNER_SOCKET_SCHEMA_SERVICE_MAX_TIMEOUT_SECS: u64 = 20;

fn owner_socket_schema_service_timeout(cfg: &MiniSchemaResolverConfig) -> Duration {
    Duration::from_secs(
        cfg.request_timeout_seconds
            .clamp(1, OWNER_SOCKET_SCHEMA_SERVICE_MAX_TIMEOUT_SECS),
    )
}

async fn with_owner_socket_schema_service_timeout<T, F>(
    timeout: Duration,
    operation: &'static str,
    fut: F,
) -> Result<T, String>
where
    F: Future<Output = Result<T, String>>,
{
    tokio::time::timeout(timeout, fut)
        .await
        .map_err(|_| format!("{operation} timed out after {}s", timeout.as_secs()))?
}

/// Install-owned Mini resolver config (JSON on disk + env overlays).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct MiniSchemaResolverConfig {
    /// Master switch for pack-backed local resolution.
    pub enabled: bool,
    /// Requested mode: `live_only` | `shadow` | `enforce_existing_only`.
    pub mode: String,
    /// Operator gate: enforce mode is ignored unless this is true.
    pub allow_enforce: bool,
    /// Kill switch: force LiveOnly regardless of other settings.
    pub kill_switch: bool,
    /// Pack object base URL (https://… or http://localhost for tests).
    pub base_url: String,
    /// `dev` or `prod` (maps to pack Env).
    pub channel: String,
    pub refresh_interval_seconds: u64,
    pub refresh_jitter_seconds: u64,
    pub request_timeout_seconds: u64,
    pub max_download_bytes: u64,
    pub max_config_age_seconds: Option<i64>,
    /// Machine-produced enforce eligibility report. Defaults fail closed.
    pub enforce_gate: SchemaResolverEnforceGateReport,
    /// Must match pack embedder id (default: FastEmbed production id).
    pub expected_embedder_id: String,
    /// Cache root relative to home or absolute.
    pub cache_dir: String,
    pub trusted_keys: Vec<TrustedKeyConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TrustedKeyConfig {
    pub key_id: String,
    pub public_key_b64: String,
}

impl Default for MiniSchemaResolverConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: "live_only".to_string(),
            allow_enforce: false,
            kill_switch: false,
            base_url: String::new(),
            channel: "prod".to_string(),
            refresh_interval_seconds: 3600,
            refresh_jitter_seconds: 60,
            request_timeout_seconds: 10,
            max_download_bytes: 134_217_728,
            max_config_age_seconds: Some(604_800),
            enforce_gate: SchemaResolverEnforceGateReport::default(),
            expected_embedder_id: "fastembed/all-MiniLM-L6-v2".to_string(),
            cache_dir: "schema-resolver".to_string(),
            trusted_keys: Vec::new(),
        }
    }
}

impl MiniSchemaResolverConfig {
    pub fn path_for_home(home: &Path) -> PathBuf {
        home.join(SCHEMA_RESOLVER_CONFIG_FILE)
    }

    /// Load from disk (missing file → defaults) then apply env overlays.
    pub fn load_for_home(home: &Path) -> Self {
        let path = Self::path_for_home(home);
        let mut cfg = if path.exists() {
            match fs::read(&path) {
                Ok(bytes) if !bytes.iter().all(u8::is_ascii_whitespace) => {
                    match serde_json::from_slice::<Self>(&bytes) {
                        Ok(c) => c,
                        Err(e) => {
                            warn!(
                                target: "lastdb_node::schema_resolver",
                                error = %e,
                                path = %path.display(),
                                "schema_resolver.json parse failed; using defaults"
                            );
                            Self::default()
                        }
                    }
                }
                Ok(_) => Self::default(),
                Err(e) => {
                    warn!(
                        target: "lastdb_node::schema_resolver",
                        error = %e,
                        path = %path.display(),
                        "schema_resolver.json read failed; using defaults"
                    );
                    Self::default()
                }
            }
        } else {
            Self::default()
        };
        cfg.apply_env_overlays();
        cfg
    }

    fn apply_env_overlays(&mut self) {
        if let Ok(v) = std::env::var("SCHEMA_RESOLVER_ENABLED") {
            self.enabled = parse_bool(&v).unwrap_or(self.enabled);
        }
        if let Ok(v) = std::env::var("SCHEMA_RESOLVER_MODE") {
            if !v.trim().is_empty() {
                self.mode = v.trim().to_ascii_lowercase();
            }
        }
        if let Ok(v) = std::env::var("SCHEMA_RESOLVER_ALLOW_ENFORCE") {
            self.allow_enforce = parse_bool(&v).unwrap_or(self.allow_enforce);
        }
        if let Ok(v) = std::env::var("SCHEMA_RESOLVER_KILL_SWITCH") {
            self.kill_switch = parse_bool(&v).unwrap_or(self.kill_switch);
        }
        if let Ok(v) = std::env::var("SCHEMA_RESOLVER_BASE_URL") {
            if !v.trim().is_empty() {
                self.base_url = v.trim().to_string();
            }
        }
        if let Ok(v) = std::env::var("SCHEMA_RESOLVER_CHANNEL") {
            if !v.trim().is_empty() {
                self.channel = v.trim().to_ascii_lowercase();
            }
        }
        if let Ok(v) = std::env::var("SCHEMA_RESOLVER_EXPECTED_EMBEDDER_ID") {
            if !v.trim().is_empty() {
                self.expected_embedder_id = v.trim().to_string();
            }
        }
        // SCHEMA_RESOLVER_TRUSTED_KEY=key_id:base64pubkey (repeatable via
        // comma-separated list).
        if let Ok(v) = std::env::var("SCHEMA_RESOLVER_TRUSTED_KEYS") {
            let mut keys = Vec::new();
            for part in v.split(',') {
                let part = part.trim();
                if part.is_empty() {
                    continue;
                }
                if let Some((id, pk)) = part.split_once(':') {
                    if !id.is_empty() && !pk.is_empty() {
                        keys.push(TrustedKeyConfig {
                            key_id: id.to_string(),
                            public_key_b64: pk.to_string(),
                        });
                    }
                }
            }
            if !keys.is_empty() {
                self.trusted_keys = keys;
            }
        }
    }

    /// Resolve the effective facade mode after kill-switch / enforce gates.
    pub fn effective_mode(&self) -> LocalFirstMode {
        if self.kill_switch || !self.enabled {
            return LocalFirstMode::LiveOnly;
        }
        match self.mode.trim().to_ascii_lowercase().as_str() {
            "shadow" => LocalFirstMode::Shadow,
            "enforce_existing_only" | "enforce" => {
                if self.allow_enforce && schema_resolver_enforce_gate_pass(&self.enforce_gate) {
                    LocalFirstMode::EnforceExistingOnly
                } else {
                    let reason = if self.allow_enforce {
                        schema_resolver_enforce_gate_failures(&self.enforce_gate).join(",")
                    } else {
                        "allow_enforce=false".to_string()
                    };
                    warn!(
                        target: "lastdb_node::schema_resolver",
                        %reason,
                        "enforce mode requested but gate is not eligible; clamping to shadow"
                    );
                    LocalFirstMode::Shadow
                }
            }
            _ => LocalFirstMode::LiveOnly,
        }
    }

    pub fn pack_wiring_ready(&self) -> bool {
        self.enabled
            && !self.kill_switch
            && !self.base_url.trim().is_empty()
            && !self.trusted_keys.is_empty()
            && !self.expected_embedder_id.trim().is_empty()
    }
}

fn parse_bool(s: &str) -> Option<bool> {
    env_flag::parse(s)
}

// ---------------------------------------------------------------------------
// Public entries: direct declare and publish/attach with wired facade
// ---------------------------------------------------------------------------

/// Resolve a direct Mini declare through the install-owned local-first facade.
///
/// LiveOnly preserves the historical service resolve path. Shadow and
/// EnforceExistingOnly use the same resolver pack/runtime wiring as
/// shared-surface publish/attach, so direct declare cannot bypass the pack
/// policy when `enforce_existing_only` is enabled.
pub async fn resolve_direct_declare_with_host_config(
    home: &Path,
    schema_service_url: &str,
    proposal_id: String,
    proposal: SchemaResolveProposal,
) -> Result<SchemaResolveResult, String> {
    let items = resolve_facade_with_host_config(
        home,
        schema_service_url,
        vec![FacadeProposal {
            proposal_id,
            proposal,
        }],
    )
    .await?;
    items
        .into_iter()
        .next()
        .map(|item| item.result)
        .ok_or_else(|| "schema resolver returned empty direct-declare batch".to_string())
}

async fn resolve_facade_with_host_config(
    home: &Path,
    schema_service_url: &str,
    proposals: Vec<FacadeProposal>,
) -> Result<Vec<FacadeResolveItem>, String> {
    let cfg = MiniSchemaResolverConfig::load_for_home(home);
    let mut mode = cfg.effective_mode();
    let request_timeout = owner_socket_schema_service_timeout(&cfg);

    // Pack-backed modes need wiring plus a local proposal embedder. Mini no
    // longer ships that in-process FastEmbed path, so configured pack modes
    // clamp to LiveOnly.
    let want_pack = matches!(
        mode,
        LocalFirstMode::Shadow | LocalFirstMode::EnforceExistingOnly
    ) && cfg.pack_wiring_ready();

    if !want_pack {
        if matches!(
            mode,
            LocalFirstMode::Shadow | LocalFirstMode::EnforceExistingOnly
        ) {
            info!(
                target: "lastdb_node::schema_resolver",
                mode = ?mode,
                pack_ready = cfg.pack_wiring_ready(),
                "schema resolve falling back to LiveOnly (pack not configured or kill switch)"
            );
            mode = LocalFirstMode::LiveOnly;
        }
        return with_owner_socket_schema_service_timeout(
            request_timeout,
            "live schema resolve",
            resolve_live_only(schema_service_url, mode, proposals, request_timeout),
        )
        .await;
    }

    warn!(
        target: "lastdb_node::schema_resolver",
        "schema resolve pack mode requested but Mini no longer ships in-process semantic search; LiveOnly"
    );
    with_owner_socket_schema_service_timeout(
        request_timeout,
        "live schema resolve",
        resolve_live_only(
            schema_service_url,
            LocalFirstMode::LiveOnly,
            proposals,
            request_timeout,
        ),
    )
    .await
}

/// Run shared-surface publish/attach with install-owned mode + optional pack.
pub async fn publish_attach_with_host_config(
    home: &Path,
    schema_service_url: &str,
    body: SharedSurfacePublishRequest,
) -> Result<SharedSurfacePublishResult, String> {
    let cfg = MiniSchemaResolverConfig::load_for_home(home);
    let mut mode = cfg.effective_mode();
    let request_timeout = owner_socket_schema_service_timeout(&cfg);

    // Pack-backed modes need wiring plus a local proposal embedder. Mini no
    // longer ships that in-process FastEmbed path, so configured pack modes
    // clamp to LiveOnly.
    let want_pack = matches!(
        mode,
        LocalFirstMode::Shadow | LocalFirstMode::EnforceExistingOnly
    ) && cfg.pack_wiring_ready();

    if !want_pack {
        if matches!(
            mode,
            LocalFirstMode::Shadow | LocalFirstMode::EnforceExistingOnly
        ) {
            info!(
                target: "lastdb_node::schema_resolver",
                mode = ?mode,
                pack_ready = cfg.pack_wiring_ready(),
                "shared-surface falling back to LiveOnly (pack not configured or kill switch)"
            );
            mode = LocalFirstMode::LiveOnly;
        }
        return with_owner_socket_schema_service_timeout(
            request_timeout,
            "live shared-surface publish/attach",
            publish_live_only(home, schema_service_url, mode, body, request_timeout),
        )
        .await;
    }

    warn!(
        target: "lastdb_node::schema_resolver",
        "shared-surface pack mode requested but Mini no longer ships in-process semantic search; LiveOnly"
    );
    with_owner_socket_schema_service_timeout(
        request_timeout,
        "live shared-surface publish/attach",
        publish_live_only(
            home,
            schema_service_url,
            LocalFirstMode::LiveOnly,
            body,
            request_timeout,
        ),
    )
    .await
}

async fn resolve_live_only(
    schema_service_url: &str,
    _mode: LocalFirstMode,
    proposals: Vec<FacadeProposal>,
    timeout: Duration,
) -> Result<Vec<FacadeResolveItem>, String> {
    // LiveOnly ignores pack; DisabledEmbeddingModel is fine.
    let client = SchemaServiceClient::new_with_timeout(schema_service_url, timeout);
    let facade = LocalFirstSchemaResolver::<NoopPackStore, _, _>::new(
        client,
        Arc::new(DisabledEmbeddingModel),
        LocalFirstMode::LiveOnly,
    );
    facade.resolve(proposals).await.map_err(|e| e.to_string())
}

async fn publish_live_only(
    home: &Path,
    schema_service_url: &str,
    _mode: LocalFirstMode,
    body: SharedSurfacePublishRequest,
    timeout: Duration,
) -> Result<SharedSurfacePublishResult, String> {
    // LiveOnly ignores pack; DisabledEmbeddingModel is fine.
    let client = schema_publish_client(home, schema_service_url, timeout)?;
    let facade = LocalFirstSchemaResolver::<NoopPackStore, _, _>::new(
        client,
        Arc::new(DisabledEmbeddingModel),
        LocalFirstMode::LiveOnly,
    );
    facade.publish_attach(body).await.map_err(|e| e.to_string())
}
