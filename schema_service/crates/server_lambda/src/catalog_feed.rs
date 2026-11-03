//! The catalog feed trigger and its HTTPS source (advisory Schema Service,
//! section 8).
//!
//! An EventBridge rule invokes the `live` alias every five minutes with a
//! synthetic API Gateway v2 event for `POST /v1/internal/catalog-feed`. API
//! Gateway lists its routes one by one and this path is not among them, and
//! the handler also requires the rule's sentinel `apiId`, so no public
//! request reaches the feed. `SCHEMA_CATALOG_FEED` turns it on per
//! environment.
//!
//! Lists are read anonymously from the registry's list base. Manifests are
//! read from `api.github.com` only: no credential is sent, a redirect is
//! followed only to the same host, and every body has a size cap.

use std::time::Duration;

use async_trait::async_trait;
use folddb_profile::endpoints::{registry_index_url_for, Environment};
use lambda_http::request::RequestContext;
use lambda_http::{Body, Error, Request, Response};
use reqwest::header::{ACCEPT, ETAG, IF_NONE_MATCH};
use reqwest::{redirect, Client, StatusCode};
use schema_service_core::catalog_feed::{
    interface_list_path, CatalogFeedSource, ListFetch, ManifestFetch, MAX_LIST_BYTES,
    MAX_MANIFEST_BYTES,
};
use schema_service_server_shared::state::SchemaServiceState;
use serde_json::json;

use crate::http::json_response;

/// The path the schedule invokes.
pub(crate) const CATALOG_FEED_PATH: &str = "/v1/internal/catalog-feed";
/// The `requestContext.apiId` the schedule's event carries.
const SCHEDULE_API_ID: &str = "catalog-feed-schedule";
/// On (`1`, `true`, `yes`, `on`) where the feed runs. Off when unset.
const FEED_ENV: &str = "SCHEMA_CATALOG_FEED";
/// Optional list base. The default is the environment's registry list base.
const LIST_BASE_ENV: &str = "SCHEMA_CATALOG_FEED_LIST_BASE";
const GITHUB_API_HOST: &str = "api.github.com";
const GITHUB_API_VERSION: &str = "2022-11-28";
const USER_AGENT: &str = "lastdb-schema-service-catalog-feed";
const HTTP_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_GITHUB_REDIRECTS: usize = 3;

/// `POST /v1/internal/catalog-feed`: one feed run.
pub(crate) async fn post_catalog_feed(
    state: &SchemaServiceState,
    event: &Request,
) -> Result<Response<Body>, Error> {
    if !from_schedule(event) {
        return json_response(
            404,
            &schema_service_core::error_only_body(&format!("Not found: POST {CATALOG_FEED_PATH}")),
        );
    }
    if !feed_enabled() {
        return json_response(200, &json!({ "enabled": false }));
    }
    let source = match list_base().and_then(HttpFeedSource::new) {
        Ok(source) => source,
        Err(error) => {
            tracing::error!(target: "schema_service::catalog_feed", error = %error, "catalog feed is misconfigured");
            return json_response(500, &schema_service_core::error_only_body(&error));
        }
    };
    match state.run_catalog_feed(&source).await {
        Ok(report) => {
            let summary = serde_json::to_string(&report).unwrap_or_default();
            tracing::info!(target: "schema_service::catalog_feed", report = %summary, "catalog feed run complete");
            json_response(200, &json!({ "enabled": true, "report": report }))
        }
        Err(error) => {
            tracing::error!(target: "schema_service::catalog_feed", error = %error, "catalog feed run failed");
            json_response(
                500,
                &schema_service_core::error_only_body(&error.to_string()),
            )
        }
    }
}

/// True only for the schedule's synthetic event.
fn from_schedule(event: &Request) -> bool {
    matches!(
        event.extensions().get::<RequestContext>(),
        Some(RequestContext::ApiGatewayV2(context))
            if context.apiid.as_deref() == Some(SCHEDULE_API_ID)
    )
}

/// `SCHEMA_CATALOG_FEED` with the `env_flag` vocabulary: trim, ASCII
/// lowercase, `1`/`true`/`yes`/`on`. Matches `var_truthy` in
/// `server_shared::schema_config`.
fn feed_enabled() -> bool {
    std::env::var(FEED_ENV).ok().is_some_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

/// The list base: the override, or this environment's registry list base.
fn list_base() -> Result<String, String> {
    let base = match std::env::var(LIST_BASE_ENV) {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => {
            let name = std::env::var("ENVIRONMENT")
                .map_err(|_| "ENVIRONMENT is not set; the feed cannot pick a list base")?;
            let environment =
                Environment::try_from_str(name.trim()).map_err(|error| error.to_string())?;
            registry_index_url_for(environment).to_string()
        }
    };
    if base.starts_with("https://") {
        Ok(base.trim_end_matches('/').to_string())
    } else {
        Err("the catalog feed list base must use HTTPS".into())
    }
}

/// Follow a redirect only to `api.github.com` over HTTPS.
fn github_redirect(attempt: redirect::Attempt<'_>) -> redirect::Action {
    let same_host =
        attempt.url().scheme() == "https" && attempt.url().host_str() == Some(GITHUB_API_HOST);
    if attempt.previous().len() > MAX_GITHUB_REDIRECTS || !same_host {
        attempt.stop()
    } else {
        attempt.follow()
    }
}

/// Read a body up to `cap` bytes. `None` when the body is larger.
async fn read_capped(
    mut response: reqwest::Response,
    cap: usize,
) -> Result<Option<Vec<u8>>, String> {
    let url = response.url().to_string();
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| format!("GET {url}: {error}"))?
    {
        if bytes.len().saturating_add(chunk.len()) > cap {
            return Ok(None);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(Some(bytes))
}

/// The feed's network side over HTTPS.
struct HttpFeedSource {
    lists: Client,
    github: Client,
    list_base: String,
}

impl HttpFeedSource {
    fn new(list_base: String) -> Result<Self, String> {
        // trace-egress: skip-s3 (anonymous static list objects; the bucket
        // does not honour traceparent and no credential is sent)
        let lists = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .no_proxy()
            .redirect(redirect::Policy::none())
            .user_agent(USER_AGENT)
            .build()
            .map_err(|error| format!("list client: {error}"))?;
        // trace-egress: skip-3p (api.github.com; unauthenticated)
        let github = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .no_proxy()
            .https_only(true)
            .redirect(redirect::Policy::custom(github_redirect))
            .user_agent(USER_AGENT)
            .build()
            .map_err(|error| format!("GitHub client: {error}"))?;
        Ok(Self {
            lists,
            github,
            list_base,
        })
    }

    async fn get_signature(&self, url: &str) -> Result<Vec<u8>, String> {
        let url = format!("{url}.sig");
        let response = self
            .lists
            .get(&url)
            .send()
            .await
            .map_err(|error| format!("GET {url}: {error}"))?;
        let status = response.status();
        if !status.is_success() {
            return Err(format!("GET {url} returned {}", status.as_u16()));
        }
        read_capped(response, MAX_LIST_BYTES)
            .await?
            .ok_or_else(|| format!("GET {url}: the signature exceeds {MAX_LIST_BYTES} bytes"))
    }
}

#[async_trait]
impl CatalogFeedSource for HttpFeedSource {
    async fn fetch_list(
        &self,
        channel: &str,
        api_version: u32,
        etag: Option<&str>,
    ) -> Result<ListFetch, String> {
        let url = format!(
            "{}/{}",
            self.list_base,
            interface_list_path(channel, api_version)
        );
        let mut request = self.lists.get(&url);
        if let Some(etag) = etag {
            request = request.header(IF_NONE_MATCH, etag);
        }
        let response = request
            .send()
            .await
            .map_err(|error| format!("GET {url}: {error}"))?;
        let status = response.status();
        if status == StatusCode::NOT_MODIFIED {
            return Ok(ListFetch::NotModified);
        }
        // The bucket refuses anonymous listing, so a missing key reads 403.
        if status == StatusCode::FORBIDDEN || status == StatusCode::NOT_FOUND {
            return Ok(ListFetch::Absent);
        }
        if !status.is_success() {
            return Err(format!("GET {url} returned {}", status.as_u16()));
        }
        let etag = response
            .headers()
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let body = read_capped(response, MAX_LIST_BYTES)
            .await?
            .ok_or_else(|| format!("GET {url}: the list exceeds {MAX_LIST_BYTES} bytes"))?;
        let signature = self.get_signature(&url).await?;
        Ok(ListFetch::Fetched {
            body,
            signature,
            etag,
        })
    }

    async fn fetch_manifest(
        &self,
        owner: &str,
        repo: &str,
        sha: &str,
    ) -> Result<ManifestFetch, String> {
        let url = format!(
            "https://{GITHUB_API_HOST}/repos/{owner}/{repo}/contents/lastdb-app.json?ref={sha}"
        );
        let response = self
            .github
            .get(&url)
            .header(ACCEPT, "application/vnd.github.raw+json")
            .header("X-GitHub-Api-Version", GITHUB_API_VERSION)
            .send()
            .await
            .map_err(|error| format!("GET {url}: {error}"))?;
        let status = response.status();
        let exhausted = response
            .headers()
            .get("x-ratelimit-remaining")
            .is_some_and(|value| value.as_bytes() == b"0");
        if status == StatusCode::TOO_MANY_REQUESTS || (status == StatusCode::FORBIDDEN && exhausted)
        {
            return Ok(ManifestFetch::RateLimited);
        }
        if status.is_redirection()
            || [
                StatusCode::NOT_FOUND,
                StatusCode::GONE,
                StatusCode::UNAVAILABLE_FOR_LEGAL_REASONS,
            ]
            .contains(&status)
        {
            return Ok(ManifestFetch::Missing(format!(
                "no readable lastdb-app.json at the repository root (HTTP {})",
                status.as_u16()
            )));
        }
        if !status.is_success() {
            return Err(format!("GET {url} returned {}", status.as_u16()));
        }
        Ok(match read_capped(response, MAX_MANIFEST_BYTES).await? {
            Some(bytes) => ManifestFetch::Found(bytes),
            None => ManifestFetch::Missing(format!(
                "lastdb-app.json exceeds {MAX_MANIFEST_BYTES} bytes"
            )),
        })
    }
}
