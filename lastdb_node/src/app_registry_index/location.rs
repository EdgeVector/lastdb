//! Index location (URL or directory) and the fetch of a channel's files.

use std::path::PathBuf;

use super::{validate_channel, DEFAULT_INDEX_BASE_URL, HTTP_TIMEOUT, INDEX_LOCATION_ENV};

// ─── Location / fetch ─────────────────────────────────────────────────────

/// Where a channel's index files live.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexLocation {
    /// `<base>/<channel>.json` over HTTP(S).
    Url(String),
    /// `<dir>/<channel>.json` on the local filesystem.
    Dir(PathBuf),
}

impl IndexLocation {
    /// Address of the list for one exact interface version.
    #[must_use]
    pub fn describe_interface(&self, channel: &str, api: u32) -> String {
        let path = app_identity_crypto::interface_list_path(channel, api);
        match self {
            Self::Url(base) => format!("{base}/{path}"),
            Self::Dir(dir) => dir.join(path).display().to_string(),
        }
    }

    /// Download one list and its detached signature. No credentials are sent.
    ///
    /// # Errors
    /// An invalid channel, insecure URL, or a failed transport or filesystem read.
    pub async fn fetch_interface(
        &self,
        channel: &str,
        api: u32,
    ) -> Result<(Vec<u8>, Vec<u8>), String> {
        validate_channel(channel)?;
        let address = self.describe_interface(channel, api);
        match self {
            Self::Dir(_) => {
                let bytes = std::fs::read(&address).map_err(|e| format!("{address}: {e}"))?;
                let sig = std::fs::read(format!("{address}.sig"))
                    .map_err(|e| format!("{address}.sig: {e}"))?;
                Ok((bytes, sig))
            }
            Self::Url(base) => {
                if !base.starts_with("https://") {
                    return Err("registry list URL must use HTTPS".into());
                }
                // trace-egress: propagate (anonymous static registry list endpoint)
                let client = reqwest::Client::builder()
                    .timeout(HTTP_TIMEOUT)
                    .no_proxy()
                    .redirect(reqwest::redirect::Policy::none())
                    .build()
                    .map_err(|e| e.to_string())?;
                let bytes = fetch_url(&client, &address).await?;
                let sig = fetch_url(&client, &format!("{address}.sig")).await?;
                Ok((bytes, sig))
            }
        }
    }
    /// `--index` flag, then `$LASTDB_REGISTRY_INDEX`, then the public tap.
    /// A value that names an existing directory or starts with `file://` is a
    /// directory; an `http://` or `https://` value is a URL base.
    ///
    /// # Errors
    /// A `lastsecrets://` locator is not an index location.
    pub fn resolve(flag: Option<&str>) -> Result<Self, String> {
        let raw = flag
            .map(str::to_string)
            .or_else(|| std::env::var(INDEX_LOCATION_ENV).ok())
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_INDEX_BASE_URL.to_string());
        Self::from_raw(&raw)
    }

    /// # Errors
    /// A `lastsecrets://` locator is not an index location.
    pub fn from_raw(raw: &str) -> Result<Self, String> {
        let raw = raw.trim().trim_end_matches('/');
        if raw.starts_with("lastsecrets://") {
            return Err("registry index location does not accept a lastsecrets locator".into());
        }
        if let Some(path) = raw.strip_prefix("file://") {
            return Ok(Self::Dir(PathBuf::from(path)));
        }
        if raw.starts_with("http://") || raw.starts_with("https://") {
            return Ok(Self::Url(raw.to_string()));
        }
        Ok(Self::Dir(PathBuf::from(raw)))
    }
}

async fn fetch_url(client: &reqwest::Client, url: &str) -> Result<Vec<u8>, String> {
    // trace-egress: propagate (anonymous static index read; no credential)
    let mut response = client
        .get(url)
        .send()
        .await
        .map_err(|e| format!("GET {url} failed: {e}"))?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("GET {url} returned {}", status.as_u16()));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| format!("GET {url}: {e}"))?
    {
        if bytes.len().saturating_add(chunk.len()) > 2_097_152 {
            return Err(format!("GET {url}: registry object exceeds 2 MiB"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
