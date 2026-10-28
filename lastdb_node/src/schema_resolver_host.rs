//! Install-owned schema resolver wiring for Mini schema resolution.
//!
//! Loads the optional request timeout from `{home}/schema_resolver.json`.
//! All resolution uses the live service; retired pack settings are ignored.

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

/// Live schema-service request budget. Retired pack options in old files are ignored.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct MiniSchemaResolverConfig {
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
    pub fn load_for_home(home: &Path) -> Self {
        let path = Self::path_for_home(home);
        match fs::read(&path) {
            Ok(bytes) if bytes.iter().all(u8::is_ascii_whitespace) => Self::default(),
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|error| {
                warn!(target: "lastdb_node::schema_resolver", %error, path = %path.display(),
                    "schema_resolver.json parse failed; using defaults");
                Self::default()
            }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Self::default(),
            Err(error) => {
                warn!(target: "lastdb_node::schema_resolver", %error, path = %path.display(),
                    "schema_resolver.json read failed; using defaults");
                Self::default()
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Public entries: direct declare and publish/attach with wired facade
// ---------------------------------------------------------------------------

/// Resolve a direct Mini declare through the live service.
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
    let request_timeout = owner_socket_schema_service_timeout(&cfg);
    with_owner_socket_schema_service_timeout(
        request_timeout,
        "live schema resolve",
        resolve_live_only(schema_service_url, proposals, request_timeout),
    )
    .await
}

/// Run shared-surface publish/attach through the authenticated live client.
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
