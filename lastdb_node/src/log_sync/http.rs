//! Signed POST of one canonical body.
//!
//! The saved owner API key is sent only on native create. Every other route
//! stays on the node signature.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use cloud_backup_protocol::v2::share_access::ShareEnvironment;
use cloud_backup_protocol::{CloudScope, Id};
use fold_db::security::Ed25519KeyPair;
use mutation_log::LogError;
use reqwest::Url;

use crate::cloud::load_connected_home_context;
use crate::cloud_backup_client::{
    authorization_post, build_shared_http_client, read_bounded_bytes, PathPostTarget,
};
use crate::host::Host;
use crate::profile::endpoints::{exemem_api_url_for, Environment};

use super::wire;

pub(in crate::log_sync) mod paths {
    pub(crate) const CREATE: &str = "/api/cloud/backups/v2/mutation-log/create-database";
    pub(crate) const ACCESS: &str = "/api/cloud/backups/v2/mutation-log/access";
    pub(crate) const APPEND: &str = "/api/cloud/backups/v2/mutation-log/append";
    pub(crate) const HEAD: &str = "/api/cloud/backups/v2/mutation-log/head";
    pub(crate) const LIST: &str = "/api/cloud/backups/v2/mutation-log/list";
    pub(crate) use cloud_backup_protocol::v2::share_access::MUTATION_LOG_DELIVERY_OBJECT_PATH as DELIVERY_OBJECT;
}

/// Publisher account scope from the saved connection. Receivers do not call this.
pub(in crate::log_sync) async fn publisher_scope(
    host: &Host,
) -> Result<(CloudScope, Endpoint, &'static str), String> {
    let ctx = load_connected_home_context(&host.home, Some(Arc::clone(&host.keypair))).await?;
    if ctx.account_id.as_str() != host.user_hash {
        return Err("node account id does not match the node key".into());
    }
    let endpoint = Endpoint::open(ctx.api_url, ctx.env.as_str())?.load_owner_key(&host.home);
    Ok((ctx.scope, endpoint, ctx.env.as_str()))
}

#[derive(Clone, Copy)]
pub(crate) enum Proof {
    Data,
    SelfAccess,
}

pub(super) const JSON_LIMIT: u64 = 8 * 1024 * 1024;

/// One signed log POST. The limit on the reply is not part of the request.
#[derive(Clone, Copy)]
pub(crate) struct LogRequest<'a> {
    pub keypair: &'a Ed25519KeyPair,
    pub scope: &'a CloudScope,
    pub database_id: &'a Id,
    pub path: &'a str,
    pub body: &'a [u8],
    pub proof: Proof,
}

pub(crate) struct SelectedService {
    pub api_url: String,
    pub label: &'static str,
}

pub(crate) struct Endpoint {
    origin: String,
    environment: &'static str,
    http: reqwest::Client,
    owner_key: Option<zeroize::Zeroizing<String>>,
}

/// Saved connection environment and the endpoint that environment already uses.
pub(crate) fn selected_service(home: &Path) -> Result<SelectedService, String> {
    let saved = crate::cloud::load_saved_credentials(home)?
        .ok_or("log sync needs saved cloud credentials")?;
    saved_service(&saved)
}

fn saved_service(saved: &crate::cloud::SavedCredentials) -> Result<SelectedService, String> {
    let connection = saved
        .connection
        .as_ref()
        .ok_or("log sync needs the saved cloud connection")?;
    let (label, environment) = match connection.environment.as_str() {
        "dev" => ("dev", Environment::Dev),
        "prod" => ("prod", Environment::Prod),
        _ => return Err("cloud connection environment is not dev or prod".into()),
    };
    if saved.api_url != exemem_api_url_for(environment) {
        return Err("cloud endpoint does not match the saved connection".into());
    }
    let api = Url::parse(&saved.api_url).map_err(|_| "cloud endpoint is not a URL")?;
    if api.path() != "/" && !api.path().is_empty() {
        return Err("log sync requires an origin cloud endpoint".into());
    }
    Ok(SelectedService {
        api_url: saved.api_url.clone(),
        label,
    })
}

impl Endpoint {
    pub(crate) fn open(api_root: &str, environment: &'static str) -> Result<Self, String> {
        let url = Url::parse(api_root).map_err(|_| "cloud endpoint is not a URL")?;
        let host = url.host_str().ok_or("cloud endpoint has no host")?;
        let origin = match url.port() {
            Some(port) => format!("{}://{host}:{port}", url.scheme()),
            None => format!("{}://{host}", url.scheme()),
        };
        let http = build_shared_http_client().map_err(|_| "cannot build the log sync client")?;
        Ok(Self {
            origin,
            environment,
            http,
            owner_key: None,
        })
    }

    /// Remember the saved owner key. Create is the only route that sends it.
    pub(crate) fn load_owner_key(mut self, home: &Path) -> Self {
        if let Ok(Some(saved)) = crate::cloud::load_saved_credentials(home) {
            if !saved.api_key.is_empty() {
                self.owner_key = Some(saved.api_key);
            }
        }
        self
    }

    pub(crate) async fn post_json(&self, request: LogRequest<'_>) -> Result<Vec<u8>, LogError> {
        let path = request.path;
        self.exchange(request, JSON_LIMIT)
            .await
            .and_then(|(status, bytes)| self.accepted(path, status, bytes))
    }

    pub(crate) async fn post_exchange(
        &self,
        request: LogRequest<'_>,
        limit: u64,
    ) -> Result<(u16, Vec<u8>), LogError> {
        self.exchange(request, limit).await
    }

    fn accepted(&self, path: &str, status: u16, bytes: Vec<u8>) -> Result<Vec<u8>, LogError> {
        if (200..300).contains(&status) {
            return Ok(bytes);
        }
        if let Some(error) = wire::service_error(&bytes) {
            return Err(error);
        }
        tracing::warn!(
            target: "lastdbd::log_sync",
            operation = wire::operation(path),
            status,
            "mutation log request was refused"
        );
        Err(LogError::UnexpectedOutcome)
    }

    async fn exchange(
        &self,
        request: LogRequest<'_>,
        limit: u64,
    ) -> Result<(u16, Vec<u8>), LogError> {
        let now = fold_db::clock::unix_millis();
        let header = self
            .authorize(&request, now)
            .map_err(|_| LogError::UnexpectedOutcome)?;
        let url = format!("{}{}", self.origin, request.path);
        let mut http_request = self
            .http
            .post(url)
            .timeout(Duration::from_secs(30))
            .header(reqwest::header::AUTHORIZATION, header)
            .header(reqwest::header::CONTENT_TYPE, "application/json");
        if matches!(request.proof, Proof::SelfAccess) {
            http_request = http_request.header("X-LastDB-Self-Access", "1");
        }
        if request.path == paths::CREATE {
            let Some(key) = &self.owner_key else {
                return Err(LogError::UnexpectedOutcome);
            };
            http_request = http_request.header("X-API-Key", key.as_str());
        }
        let response = http_request
            .body(request.body.to_vec())
            .send()
            .await
            .map_err(|_| LogError::UnexpectedOutcome)?;
        let status = response.status().as_u16();
        let bytes = read_bounded_bytes(response, limit)
            .await
            .map_err(|_| LogError::UnexpectedOutcome)?;
        Ok((status, bytes))
    }

    fn authorize(&self, request: &LogRequest<'_>, now_ms: u64) -> Result<String, String> {
        let environment = ShareEnvironment::parse(self.environment)
            .map_err(|_| "cloud connection environment is not dev or prod".to_owned())?;
        authorization_post(
            request.keypair,
            PathPostTarget {
                scope: request.scope,
                database_id: request.database_id,
                environment,
                path: request.path,
            },
            request.body,
            now_ms,
        )
    }
}
