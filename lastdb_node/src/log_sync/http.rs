//! Signed POST of one canonical body. The API key is not attached to these routes.

use std::time::Duration;

use cloud_backup_protocol::CloudScope;
use cloud_backup_protocol::Id;
use fold_db::security::Ed25519KeyPair;
use mutation_log::LogError;
use reqwest::Url;

use super::sign;
use super::wire;

const JSON_LIMIT: usize = 8 * 1024 * 1024;
const SNAPSHOT_LIMIT: u64 = 1024 * 1024 * 1024;

pub(crate) struct Endpoint {
    origin: String,
    http: reqwest::Client,
}

impl Endpoint {
    pub(crate) fn open(api_root: &str) -> Result<Self, String> {
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
        Ok(Self { origin, http })
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
        self.send(
            keypair,
            scope,
            database_id,
            path,
            body,
            member_revision,
            JSON_LIMIT as u64,
        )
        .await
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
        self.send(
            keypair,
            scope,
            database_id,
            path,
            body,
            member_revision,
            SNAPSHOT_LIMIT,
        )
        .await
    }

    async fn send(
        &self,
        keypair: &Ed25519KeyPair,
        scope: &CloudScope,
        database_id: &Id,
        path: &str,
        body: &[u8],
        member_revision: u64,
        limit: u64,
    ) -> Result<Vec<u8>, LogError> {
        let now = fold_db::clock::unix_millis();
        let header = sign::authorization(
            keypair,
            scope,
            database_id,
            path,
            body,
            member_revision,
            now,
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
        let status = response.status();
        let bytes = read_limited(response, limit).await?;
        if status.is_success() {
            return Ok(bytes);
        }
        if let Some(error) = wire::service_error(&bytes) {
            return Err(error);
        }
        tracing::warn!(
            target: "lastdbd::log_sync",
            operation = wire::operation(path),
            status = status.as_u16(),
            "mutation log request was refused"
        );
        Err(LogError::UnexpectedOutcome)
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
