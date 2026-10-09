use super::*;

// ─── Registry client ──────────────────────────────────────────────────────

/// The anonymous read side of the `/v2` registry. No credential: public
/// reads stay anonymous.
#[derive(Debug, Clone)]
pub struct ReleaseRegistryClient {
    base_url: String,
}

/// A channel read: the desired release id and the generation that read saw.
#[derive(Debug, Clone, Deserialize)]
pub struct ChannelRead {
    pub app_id: String,
    pub channel: String,
    pub release_id: String,
    /// Keep this. A later channel write that carries a stale generation
    /// fails with a conflict.
    pub generation: u64,
}

/// A release read: the manifest plus its registry annotations.
#[derive(Debug, Clone, Deserialize)]
pub struct ReleaseRead {
    pub release_id: String,
    pub manifest: ReleaseManifest,
    pub publisher_dev_pubkey: String,
    #[serde(default)]
    pub revoked: bool,
}

impl ReleaseRegistryClient {
    #[must_use]
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
        }
    }

    fn client() -> Result<reqwest::Client, String> {
        // trace-egress: propagate (public app registry reads + signed
        // artifact download; every caller of this builder is a first-party
        // registry or R2 fetch, classified at each call site below)
        reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .map_err(|e| format!("failed to build HTTP client: {e}"))
    }

    /// `GET /v2/apps/{app_id}/channels/{channel}` — anonymous.
    ///
    /// # Errors
    /// Returns the transport or status failure.
    pub async fn get_channel(&self, app_id: &str, channel: &str) -> Result<ChannelRead, String> {
        let url = format!("{}/v2/apps/{app_id}/channels/{channel}", self.base_url);
        // trace-egress: propagate (public app registry channel read)
        let response = Self::client()?
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("GET {url} failed: {e}"))?;
        let status = response.status().as_u16();
        let body = response
            .text()
            .await
            .map_err(|e| format!("GET {url}: body read failed: {e}"))?;
        if status != 200 {
            return Err(format!("GET {url} returned {status}: {body}"));
        }
        serde_json::from_str(&body).map_err(|e| format!("GET {url}: response does not parse: {e}"))
    }

    /// `GET /v2/releases/{release_id}` — anonymous.
    ///
    /// # Errors
    /// Returns the transport or status failure.
    pub async fn get_release(&self, release_id: &str) -> Result<ReleaseRead, String> {
        let url = format!("{}/v2/releases/{release_id}", self.base_url);
        // trace-egress: propagate (public app registry release read)
        let response = Self::client()?
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("GET {url} failed: {e}"))?;
        let status = response.status().as_u16();
        let body = response
            .text()
            .await
            .map_err(|e| format!("GET {url}: body read failed: {e}"))?;
        if status != 200 {
            return Err(format!("GET {url} returned {status}: {body}"));
        }
        serde_json::from_str(&body).map_err(|e| format!("GET {url}: response does not parse: {e}"))
    }

    /// `GET /v2/apps/{app_id}` — anonymous.
    ///
    /// # Errors
    /// Returns the transport or status failure.
    pub async fn get_app(&self, app_id: &str) -> Result<Value, String> {
        let url = format!("{}/v2/apps/{app_id}", self.base_url);
        // trace-egress: propagate (public app registry app read)
        let response = Self::client()?
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("GET {url} failed: {e}"))?;
        let status = response.status().as_u16();
        let body = response
            .text()
            .await
            .map_err(|e| format!("GET {url}: body read failed: {e}"))?;
        if status != 200 {
            return Err(format!("GET {url} returned {status}: {body}"));
        }
        serde_json::from_str(&body).map_err(|e| format!("GET {url}: response does not parse: {e}"))
    }

    /// Download the artifact bytes. A `file://` URL reads from disk so a
    /// hermetic proof run needs no object store.
    ///
    /// # Errors
    /// Returns the transport, status, or filesystem failure.
    pub async fn fetch_artifact(&self, url: &str) -> Result<Vec<u8>, String> {
        if let Some(path) = url.strip_prefix("file://") {
            return std::fs::read(path).map_err(|e| format!("failed to read {path}: {e}"));
        }
        // trace-egress: propagate (signed release artifact download)
        let response = Self::client()?
            .get(url)
            .send()
            .await
            .map_err(|e| format!("GET {url} failed: {e}"))?;
        let status = response.status().as_u16();
        if status != 200 {
            return Err(format!("GET {url} returned {status}"));
        }
        response
            .bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| format!("GET {url}: body read failed: {e}"))
    }
}
