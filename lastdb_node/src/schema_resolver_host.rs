//! Install-owned schema resolver wiring for Mini schema resolution.
//!
//! Mini resolves through the live service. The optional home config retains
//! only its request timeout; retired pack settings are ignored. The facade
//! preserves proposal-ID matching and shared-surface publish/attach semantics.

use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;

use app_identity_crypto::{Env, SigningKey};
use folddb_profile::endpoints::Environment;
use schema_service_client::types::{SchemaResolveProposal, SchemaResolveResult};
use schema_service_client::{
    FacadeProposal, FacadeResolveItem, LiveSchemaResolver, SchemaServiceClient,
    SharedSurfacePublishRequest, SharedSurfacePublishResult,
};
use serde::{Deserialize, Serialize};
use tracing::warn;

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

/// Live schema-service settings from `{home}/schema_resolver.json`.
/// Unknown keys, including retired local-pack settings, are ignored.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct MiniSchemaResolverConfig {
    /// Per-request timeout, clamped to 1–20 seconds for owner-socket calls.
    pub request_timeout_seconds: u64,
}

impl Default for MiniSchemaResolverConfig {
    fn default() -> Self {
        Self {
            request_timeout_seconds: 10,
        }
    }
}

impl MiniSchemaResolverConfig {
    pub fn path_for_home(home: &Path) -> PathBuf {
        home.join(SCHEMA_RESOLVER_CONFIG_FILE)
    }

    /// Load from disk; missing, empty or invalid files use the default timeout.
    pub fn load_for_home(home: &Path) -> Self {
        let path = Self::path_for_home(home);
        if path.exists() {
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
        }
    }
}

/// Resolve a direct Mini declare through the live facade with its stable proposal ID.
pub async fn resolve_direct_declare_with_host_config(
    home: &Path,
    schema_service_url: &str,
    proposal_id: String,
    proposal: SchemaResolveProposal,
) -> Result<SchemaResolveResult, String> {
    let cfg = MiniSchemaResolverConfig::load_for_home(home);
    let request_timeout = owner_socket_schema_service_timeout(&cfg);
    let items = with_owner_socket_schema_service_timeout(
        request_timeout,
        "live schema resolve",
        resolve_live_only(
            schema_service_url,
            vec![FacadeProposal {
                proposal_id,
                proposal,
            }],
            request_timeout,
        ),
    )
    .await?;
    items
        .into_iter()
        .next()
        .map(|item| item.result)
        .ok_or_else(|| "schema resolver returned empty direct-declare batch".to_string())
}

/// Publish or attach through the live facade using the node signing identity.
pub async fn publish_attach_with_host_config(
    home: &Path,
    schema_service_url: &str,
    body: SharedSurfacePublishRequest,
) -> Result<SharedSurfacePublishResult, String> {
    let cfg = MiniSchemaResolverConfig::load_for_home(home);
    let request_timeout = owner_socket_schema_service_timeout(&cfg);
    with_owner_socket_schema_service_timeout(
        request_timeout,
        "live shared-surface publish/attach",
        publish_live_only(home, schema_service_url, body, request_timeout),
    )
    .await
}

async fn resolve_live_only(
    schema_service_url: &str,
    proposals: Vec<FacadeProposal>,
    timeout: Duration,
) -> Result<Vec<FacadeResolveItem>, String> {
    let client = SchemaServiceClient::new_with_timeout(schema_service_url, timeout);
    let facade = LiveSchemaResolver::new(client);
    facade.resolve(proposals).await.map_err(|e| e.to_string())
}

async fn publish_live_only(
    home: &Path,
    schema_service_url: &str,
    body: SharedSurfacePublishRequest,
    timeout: Duration,
) -> Result<SharedSurfacePublishResult, String> {
    let client = schema_publish_client(home, schema_service_url, timeout)?;
    let facade = LiveSchemaResolver::new(client);
    facade.publish_attach(body).await.map_err(|e| e.to_string())
}
