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
//!
//! The requests go through the connector the AWS SDK clients already use
//! (`aws_smithy_http_client`: hyper 1, rustls with aws-lc, native roots), so
//! the feed adds no second HTTP client to the Lambda. The bootstrap zip has a
//! 15 MiB bar, and a second client stack (reqwest) took it over.

use std::time::Duration;

use ::http::StatusCode;
use async_trait::async_trait;
use aws_smithy_async::future::timeout::Timeout;
use aws_smithy_async::rt::sleep::{default_async_sleep, AsyncSleep, SharedAsyncSleep};
use aws_smithy_http_client::proxy::ProxyConfig;
use aws_smithy_http_client::tls::{rustls_provider::CryptoMode, Provider};
use aws_smithy_http_client::Connector;
use aws_smithy_runtime_api::client::http::{HttpConnector, HttpConnectorSettings};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_types::byte_stream::ByteStream;
use folddb_profile::endpoints::{registry_index_url_for, Environment};
use lambda_http::request::RequestContext;
use lambda_http::{Body, Error, Request, Response};
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
const GITHUB_API_PREFIX: &str = "https://api.github.com/";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
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

/// A redirect target on `api.github.com` over HTTPS, or `None`.
fn same_host_redirect(location: &str) -> Option<String> {
    location
        .starts_with(GITHUB_API_PREFIX)
        .then(|| location.to_string())
}

/// The status of a response as an `http` status code.
fn status_of(url: &str, response: &HttpResponse) -> Result<StatusCode, String> {
    StatusCode::from_u16(response.status().as_u16()).map_err(|error| format!("GET {url}: {error}"))
}

/// The feed's network side over HTTPS.
struct HttpFeedSource {
    connector: Connector,
    sleep: SharedAsyncSleep,
    list_base: String,
}

impl HttpFeedSource {
    fn new(list_base: String) -> Result<Self, String> {
        let sleep = default_async_sleep().ok_or("no async sleep is available for timeouts")?;
        let settings = HttpConnectorSettings::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(HTTP_TIMEOUT)
            .build();
        // trace-egress: skip-3p (anonymous list objects and api.github.com;
        // no credential is sent and neither host reads traceparent)
        let connector = Connector::builder()
            .tls_provider(Provider::Rustls(CryptoMode::AwsLc))
            .connector_settings(settings)
            .sleep_impl(sleep.clone())
            .proxy_config(ProxyConfig::disabled())
            .build();
        Ok(Self {
            connector,
            sleep,
            list_base,
        })
    }

    /// One GET within [`HTTP_TIMEOUT`]. Redirects are not followed.
    async fn get(
        &self,
        url: &str,
        headers: &[(&'static str, &str)],
    ) -> Result<HttpResponse, String> {
        let mut request = HttpRequest::get(url).map_err(|error| format!("GET {url}: {error}"))?;
        request.headers_mut().insert("user-agent", USER_AGENT);
        for (name, value) in headers {
            request
                .headers_mut()
                .try_insert(*name, (*value).to_string())
                .map_err(|error| format!("GET {url}: header {name}: {error}"))?;
        }
        Timeout::new(self.connector.call(request), self.sleep.sleep(HTTP_TIMEOUT))
            .await
            .map_err(|error| format!("GET {url}: {error}"))?
            .map_err(|error| format!("GET {url}: {error}"))
    }

    /// Read a body up to `cap` bytes within [`HTTP_TIMEOUT`]. `None` when
    /// the body is larger.
    async fn read_capped(
        &self,
        url: &str,
        response: HttpResponse,
        cap: usize,
    ) -> Result<Option<Vec<u8>>, String> {
        let mut stream = ByteStream::new(response.into_body());
        let read = async move {
            let mut bytes = Vec::new();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|error| format!("GET {url}: {error}"))?;
                if bytes.len().saturating_add(chunk.len()) > cap {
                    return Ok(None);
                }
                bytes.extend_from_slice(&chunk);
            }
            Ok::<_, String>(Some(bytes))
        };
        Timeout::new(read, self.sleep.sleep(HTTP_TIMEOUT))
            .await
            .map_err(|error| format!("GET {url}: body: {error}"))?
    }

    async fn get_signature(&self, url: &str) -> Result<Vec<u8>, String> {
        let url = format!("{url}.sig");
        let response = self.get(&url, &[]).await?;
        let status = status_of(&url, &response)?;
        if !status.is_success() {
            return Err(format!("GET {url} returned {}", status.as_u16()));
        }
        self.read_capped(&url, response, MAX_LIST_BYTES)
            .await?
            .ok_or_else(|| format!("GET {url}: the signature exceeds {MAX_LIST_BYTES} bytes"))
    }

    /// The outcome of a final (not redirected) manifest response.
    async fn manifest_outcome(
        &self,
        url: &str,
        status: StatusCode,
        response: HttpResponse,
    ) -> Result<ManifestFetch, String> {
        let exhausted = response.headers().get("x-ratelimit-remaining") == Some("0");
        if status == StatusCode::TOO_MANY_REQUESTS || (status == StatusCode::FORBIDDEN && exhausted)
        {
            return Ok(ManifestFetch::RateLimited);
        }
        if [
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
        Ok(
            match self.read_capped(url, response, MAX_MANIFEST_BYTES).await? {
                Some(bytes) => ManifestFetch::Found(bytes),
                None => ManifestFetch::Missing(format!(
                    "lastdb-app.json exceeds {MAX_MANIFEST_BYTES} bytes"
                )),
            },
        )
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
        let conditional: Vec<(&'static str, &str)> = etag
            .map(|etag| ("if-none-match", etag))
            .into_iter()
            .collect();
        let response = self.get(&url, &conditional).await?;
        let status = status_of(&url, &response)?;
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
        let etag = response.headers().get("etag").map(str::to_string);
        let body = self
            .read_capped(&url, response, MAX_LIST_BYTES)
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
        let headers = [
            ("accept", "application/vnd.github.raw+json"),
            ("x-github-api-version", GITHUB_API_VERSION),
        ];
        let mut url = format!(
            "https://{GITHUB_API_HOST}/repos/{owner}/{repo}/contents/lastdb-app.json?ref={sha}"
        );
        for _ in 0..=MAX_GITHUB_REDIRECTS {
            let response = self.get(&url, &headers).await?;
            let status = status_of(&url, &response)?;
            if !status.is_redirection() {
                return self.manifest_outcome(&url, status, response).await;
            }
            let Some(next) = response
                .headers()
                .get("location")
                .and_then(same_host_redirect)
            else {
                return Ok(ManifestFetch::Missing(format!(
                    "the repository redirects away from {GITHUB_API_HOST} (HTTP {})",
                    status.as_u16()
                )));
            };
            url = next;
        }
        Ok(ManifestFetch::Missing(format!(
            "more than {MAX_GITHUB_REDIRECTS} redirects"
        )))
    }
}
