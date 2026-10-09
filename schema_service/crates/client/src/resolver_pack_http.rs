//! HTTPS (and localhost-HTTP) object store for signed resolver packs.
//!
//! Origin and trust roots come only from installation-owned
//! [`ResolverBootstrapConfig`]; downloaded documents never redirect the client.

use async_trait::async_trait;
use reqwest::header::{HeaderValue, ETAG, IF_NONE_MATCH};
use reqwest::StatusCode;
use schema_service_core::{
    validate_bootstrap_base_url, ObjectFetchResult, ResolverBootstrapConfig,
    ResolverBootstrapConfigError, ResolverPackFetchError, ResolverPackObjectStore,
};
use std::time::Duration;

/// HTTP object store that GETs `{base_url}/{key}` with size/timeout limits.
#[derive(Debug, Clone)]
pub struct HttpResolverPackStore {
    client: reqwest::Client,
    base_url: String,
    max_download_bytes: u64,
}

impl HttpResolverPackStore {
    pub fn new(bootstrap: &ResolverBootstrapConfig) -> Result<Self, ResolverPackFetchError> {
        bootstrap
            .validate_bootstrap()
            .map_err(map_bootstrap_error)?;
        if bootstrap.base_url.is_empty() {
            return Err(ResolverPackFetchError::InvalidUrl(
                "HttpResolverPackStore requires a non-empty base_url".to_string(),
            ));
        }
        let base_url = bootstrap.base_url.trim_end_matches('/').to_string();
        let timeout = Duration::from_secs(bootstrap.request_timeout_seconds.max(1));
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| ResolverPackFetchError::Transport(e.to_string()))?;
        Ok(Self {
            client,
            base_url,
            max_download_bytes: bootstrap.max_download_bytes,
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn object_url(&self, key: &str) -> Result<String, ResolverPackFetchError> {
        join_object_url(&self.base_url, key)
    }

    async fn fetch(
        &self,
        key: &str,
        if_none_match: Option<&str>,
    ) -> Result<ObjectFetchResult, ResolverPackFetchError> {
        let url = self.object_url(key)?;
        let mut request = self.client.get(&url);
        if let Some(etag) = if_none_match {
            if let Ok(value) = HeaderValue::from_str(etag) {
                request = request.header(IF_NONE_MATCH, value);
            }
        }

        let response = request.send().await.map_err(|e| map_reqwest_error(&e))?;
        let status = response.status();

        if status == StatusCode::NOT_MODIFIED {
            let etag = response_etag(&response);
            return Ok(ObjectFetchResult::NotModified { etag });
        }
        if status == StatusCode::NOT_FOUND {
            return Ok(ObjectFetchResult::NotFound);
        }
        if !status.is_success() {
            return Err(ResolverPackFetchError::HttpStatus(status.as_u16()));
        }

        if let Some(len) = response.content_length() {
            if len > self.max_download_bytes {
                return Err(ResolverPackFetchError::ResponseTooLarge);
            }
        }

        let etag = response_etag(&response);
        let bytes = response.bytes().await.map_err(|e| map_reqwest_error(&e))?;
        if bytes.len() as u64 > self.max_download_bytes {
            return Err(ResolverPackFetchError::ResponseTooLarge);
        }
        Ok(ObjectFetchResult::Found {
            bytes: bytes.to_vec(),
            etag,
        })
    }
}

#[async_trait]
impl ResolverPackObjectStore for HttpResolverPackStore {
    async fn get_object(&self, key: &str) -> Result<Option<Vec<u8>>, ResolverPackFetchError> {
        match self.fetch(key, None).await? {
            ObjectFetchResult::NotFound | ObjectFetchResult::NotModified { .. } => Ok(None),
            ObjectFetchResult::Found { bytes, .. } => Ok(Some(bytes)),
        }
    }

    async fn get_object_conditional(
        &self,
        key: &str,
        if_none_match: Option<&str>,
    ) -> Result<ObjectFetchResult, ResolverPackFetchError> {
        self.fetch(key, if_none_match).await
    }
}

fn map_bootstrap_error(err: ResolverBootstrapConfigError) -> ResolverPackFetchError {
    match err {
        ResolverBootstrapConfigError::OriginRejected(msg) => {
            ResolverPackFetchError::OriginRejected(msg)
        }
        ResolverBootstrapConfigError::InvalidUrl(msg) => ResolverPackFetchError::InvalidUrl(msg),
    }
}

fn map_reqwest_error(err: &reqwest::Error) -> ResolverPackFetchError {
    if err.is_timeout() {
        return ResolverPackFetchError::Timeout;
    }
    if err.is_body() {
        // Incomplete body streams surface as body errors.
        return ResolverPackFetchError::Truncated;
    }
    ResolverPackFetchError::Transport(err.to_string())
}

fn response_etag(response: &reqwest::Response) -> Option<String> {
    response
        .headers()
        .get(ETAG)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

/// Join base URL and object key without introducing a second origin.
pub fn join_object_url(base_url: &str, key: &str) -> Result<String, ResolverPackFetchError> {
    let base = base_url.trim_end_matches('/');
    if base.is_empty() {
        return Err(ResolverPackFetchError::InvalidUrl(
            "empty base_url".to_string(),
        ));
    }
    validate_bootstrap_base_url(base).map_err(map_bootstrap_error)?;
    let key = key.trim_start_matches('/');
    if key.is_empty() {
        return Err(ResolverPackFetchError::InvalidUrl(
            "empty object key".to_string(),
        ));
    }
    // Reject absolute URLs / scheme smuggling in the key.
    if key.contains("://") || key.starts_with("//") {
        return Err(ResolverPackFetchError::OriginRejected(
            "object key must be a relative path".to_string(),
        ));
    }
    Ok(format!("{base}/{key}"))
}

/// True when a content-length header would exceed the configured cap.
pub fn content_length_exceeds_cap(
    content_length_header: Option<&str>,
    max_download_bytes: u64,
) -> bool {
    content_length_header
        .and_then(|v| v.parse::<u64>().ok())
        .is_some_and(|len| len > max_download_bytes)
}
