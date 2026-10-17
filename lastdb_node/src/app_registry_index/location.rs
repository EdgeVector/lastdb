//! Index location (URL or directory) and the fetch of a channel's files.

use std::path::PathBuf;

use super::{DEFAULT_INDEX_BASE_URL, HTTP_TIMEOUT, INDEX_LOCATION_ENV};

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
    /// `--index` flag, then `$LASTDB_REGISTRY_INDEX`, then the public tap.
    /// A value that names an existing directory or starts with `file://` is a
    /// directory; anything else is a URL base.
    #[must_use]
    pub fn resolve(flag: Option<&str>) -> Self {
        let raw = flag
            .map(str::to_string)
            .or_else(|| std::env::var(INDEX_LOCATION_ENV).ok())
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_INDEX_BASE_URL.to_string());
        Self::from_raw(&raw)
    }

    #[must_use]
    pub fn from_raw(raw: &str) -> Self {
        let raw = raw.trim().trim_end_matches('/');
        if let Some(path) = raw.strip_prefix("file://") {
            return Self::Dir(PathBuf::from(path));
        }
        if raw.starts_with("http://") || raw.starts_with("https://") {
            return Self::Url(raw.to_string());
        }
        Self::Dir(PathBuf::from(raw))
    }

    /// The index file's address for `channel`, for receipts and messages.
    #[must_use]
    pub fn describe(&self, channel: &str) -> String {
        match self {
            Self::Url(base) => format!("{base}/{channel}.json"),
            Self::Dir(dir) => dir.join(format!("{channel}.json")).display().to_string(),
        }
    }

    /// Fetch `<channel>.json` and `<channel>.json.sig`.
    ///
    /// # Errors
    /// Returns the transport or filesystem error, naming the address.
    pub async fn fetch(&self, channel: &str) -> Result<(Vec<u8>, Vec<u8>), String> {
        validate_channel(channel)?;
        match self {
            Self::Dir(dir) => {
                let index_path = dir.join(format!("{channel}.json"));
                let sig_path = dir.join(format!("{channel}.json.sig"));
                let index = std::fs::read(&index_path)
                    .map_err(|e| format!("failed to read {}: {e}", index_path.display()))?;
                let sig = std::fs::read(&sig_path)
                    .map_err(|e| format!("failed to read {}: {e}", sig_path.display()))?;
                Ok((index, sig))
            }
            Self::Url(base) => {
                // trace-egress: propagate (anonymous static index endpoint)
                let client = reqwest::Client::builder()
                    .timeout(HTTP_TIMEOUT)
                    .no_proxy()
                    .build()
                    .map_err(|e| format!("failed to build HTTP client: {e}"))?;
                let index = fetch_url(&client, &format!("{base}/{channel}.json")).await?;
                let sig = fetch_url(&client, &format!("{base}/{channel}.json.sig")).await?;
                Ok((index, sig))
            }
        }
    }
}

async fn fetch_url(client: &reqwest::Client, url: &str) -> Result<Vec<u8>, String> {
    // trace-egress: propagate (anonymous static index read; no credential)
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|e| format!("GET {url} failed: {e}"))?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("GET {url} returned {}", status.as_u16()));
    }
    response
        .bytes()
        .await
        .map(|b| b.to_vec())
        .map_err(|e| format!("GET {url}: failed to read body: {e}"))
}

/// A channel name is one path segment: lowercase letters, digits, `-`.
///
/// # Errors
/// Returns a message naming the offending channel.
pub fn validate_channel(channel: &str) -> Result<(), String> {
    let ok = !channel.is_empty()
        && channel.len() <= 32
        && channel
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if ok {
        Ok(())
    } else {
        Err(format!(
            "invalid channel '{channel}' (lowercase letters, digits, '-'; at most 32)"
        ))
    }
}
