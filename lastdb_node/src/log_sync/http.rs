//! Signed POST of one canonical body. The API key is not attached to these routes.

use std::time::Duration;

use cloud_backup_protocol::CloudScope;
use cloud_backup_protocol::Id;
use fold_db::security::Ed25519KeyPair;
use mutation_log::LogError;
use reqwest::Url;

use crate::profile::endpoints::{exemem_api_url_for, Environment};

use super::sign;
use super::wire;

const JSON_LIMIT: usize = 8 * 1024 * 1024;
const SNAPSHOT_LIMIT: u64 = 1024 * 1024 * 1024;

pub(crate) struct SelectedService {
    pub api_url: String,
    pub label: &'static str,
}

pub(crate) struct Endpoint {
    origin: String,
    environment: &'static str,
    http: reqwest::Client,
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

pub(crate) fn environment_label(home: &std::path::Path) -> Result<&'static str, String> {
    Ok(selected_service(home)?.label)
}

impl Endpoint {
    pub(crate) fn open(api_root: &str, environment: &'static str) -> Result<Self, String> {
        let url = Url::parse(api_root).map_err(|_| "cloud endpoint is not a URL")?;
        let host = url.host_str().ok_or("cloud endpoint has no host")?;
        let origin = match url.port() {
            Some(port) => format!("{}://{host}:{port}", url.scheme()),
            None => format!("{}://{host}", url.scheme()),
        };
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
        })
    }

    pub(crate) async fn post_json(
        &self,
        keypair: &Ed25519KeyPair,
        scope: &CloudScope,
        database_id: &Id,
        path: &str,
        body: &[u8],
        member_revision: u64,
    ) -> Result<Vec<u8>, LogError> {
        self.exchange(
            keypair,
            scope,
            database_id,
            path,
            body,
            member_revision,
            JSON_LIMIT as u64,
        )
        .await
        .and_then(|(status, bytes)| self.accepted(path, status, bytes))
    }

    pub(crate) async fn post_bytes(
        &self,
        keypair: &Ed25519KeyPair,
        scope: &CloudScope,
        database_id: &Id,
        path: &str,
        body: &[u8],
        member_revision: u64,
    ) -> Result<Vec<u8>, LogError> {
        self.exchange(
            keypair,
            scope,
            database_id,
            path,
            body,
            member_revision,
            SNAPSHOT_LIMIT,
        )
        .await
        .and_then(|(status, bytes)| self.accepted(path, status, bytes))
    }

    pub(crate) async fn post_exchange(
        &self,
        keypair: &Ed25519KeyPair,
        scope: &CloudScope,
        database_id: &Id,
        path: &str,
        body: &[u8],
        member_revision: u64,
        limit: u64,
    ) -> Result<(u16, Vec<u8>), LogError> {
        self.exchange(
            keypair,
            scope,
            database_id,
            path,
            body,
            member_revision,
            limit,
        )
        .await
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
        keypair: &Ed25519KeyPair,
        scope: &CloudScope,
        database_id: &Id,
        path: &str,
        body: &[u8],
        member_revision: u64,
        limit: u64,
    ) -> Result<(u16, Vec<u8>), LogError> {
        let now = fold_db::clock::unix_millis();
        let header = sign::authorization(
            keypair,
            scope,
            database_id,
            path,
            body,
            member_revision,
            now,
            self.environment,
        )
        .map_err(|_| LogError::UnexpectedOutcome)?;
        let url = format!("{}{path}", self.origin);
        let response = self
            .http
            .post(url)
            .header(reqwest::header::AUTHORIZATION, header)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.to_vec())
            .send()
            .await
            .map_err(|_| LogError::UnexpectedOutcome)?;
        let status = response.status().as_u16();
        let bytes = read_limited(response, limit).await?;
        Ok((status, bytes))
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
