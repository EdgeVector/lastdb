//! Signed POST of one canonical body.
//!
//! The saved owner API key is sent only on native create. Every other route
//! stays on the node signature.

use std::path::Path;
use std::time::Duration;

use cloud_backup_protocol::v2::share_access::ShareEnvironment;
use cloud_backup_protocol::CloudScope;
use cloud_backup_protocol::Id;
use fold_db::security::Ed25519KeyPair;
use mutation_log::LogError;
use reqwest::Url;

use crate::node_read::{self, CanonicalPost, SignedPostTarget};
use crate::profile::endpoints::{exemem_api_url_for, Environment};

use super::wire;

#[derive(Clone, Copy)]
pub(crate) enum Proof {
    Canonical(CanonicalPost),
    SelfAccess(CanonicalPost),
    Data,
}

const JSON_LIMIT: usize = 8 * 1024 * 1024;

/// One signed log POST. The limit on the reply is not part of the request.
#[derive(Clone, Copy)]
pub(crate) struct LogRequest<'a> {
    pub keypair: &'a Ed25519KeyPair,
    pub scope: &'a CloudScope,
    pub database_id: &'a Id,
    pub path: &'a str,
    pub body: &'a [u8],
    pub member_revision: u64,
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
pub(crate) fn selected_service(home: &std::path::Path) -> Result<SelectedService, String> {
    let saved = crate::cloud::load_saved_credentials(home)?
        .ok_or("log sync needs saved cloud credentials")?;
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
        api_url: saved.api_url,
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
        // trace-egress: propagate — log sync posts to the enrolled service.
        let http = reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(15))
            .build()
            .map_err(|_| "cannot build the log sync client")?;
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
        self.exchange(request, JSON_LIMIT as u64)
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
            .header(reqwest::header::AUTHORIZATION, header)
            .header(reqwest::header::CONTENT_TYPE, "application/json");
        if matches!(request.proof, Proof::SelfAccess(_)) {
            http_request = http_request.header("X-LastDB-Self-Access", "1");
        }
        if request.path == super::paths::CREATE {
            let Some(key) = &self.owner_key else {
                return Err(LogError::UnexpectedOutcome);
            };
            http_request = http_request.header("X-API-Key", key.as_str());
        }
        let response =
            observability::propagation::inject_w3c(http_request.body(request.body.to_vec()))
                .send()
                .await
                .map_err(|_| LogError::UnexpectedOutcome)?;
        let status = response.status().as_u16();
        let bytes = read_limited(response, limit).await?;
        Ok((status, bytes))
    }

    fn authorize(&self, request: &LogRequest<'_>, now_ms: u64) -> Result<String, String> {
        match request.proof {
            Proof::Canonical(route) | Proof::SelfAccess(route) => {
                let environment = ShareEnvironment::parse(self.environment)
                    .map_err(|_| "cloud connection environment is not dev or prod".to_owned())?;
                node_read::authorization(
                    request.keypair,
                    SignedPostTarget {
                        scope: request.scope,
                        database_id: request.database_id,
                        environment,
                        route,
                    },
                    request.body,
                    request.member_revision,
                    now_ms,
                )
            }
            Proof::Data => {
                let environment = ShareEnvironment::parse(self.environment)
                    .map_err(|_| "cloud connection environment is not dev or prod".to_owned())?;
                node_read::authorization_post(
                    request.keypair,
                    node_read::PathPostTarget {
                        scope: request.scope,
                        database_id: request.database_id,
                        environment,
                        path: request.path,
                    },
                    request.body,
                    request.member_revision,
                    now_ms,
                )
            }
        }
    }
}

async fn read_limited(response: reqwest::Response, limit: u64) -> Result<Vec<u8>, LogError> {
    let length = response.content_length().unwrap_or(0);
    if length > limit {
        return Err(LogError::UnexpectedOutcome);
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|_| LogError::UnexpectedOutcome)?;
    if bytes.len() as u64 > limit {
        return Err(LogError::UnexpectedOutcome);
    }
    Ok(bytes.to_vec())
}
