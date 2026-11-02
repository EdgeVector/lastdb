//! Index location (URL or directory) and the fetch of a channel's files.

use std::path::PathBuf;

use super::{DEFAULT_INDEX_BASE_URL, HTTP_TIMEOUT, INDEX_LOCATION_ENV};

// ─── Location / fetch ─────────────────────────────────────────────────────

/// Where a channel's index files live.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexLocation {
    /// A private list base fetched from LastSecrets only at the point of use.
    Secret(String),
    /// `<base>/<channel>.json` over HTTP(S).
    Url(String),
    /// `<dir>/<channel>.json` on the local filesystem.
    Dir(PathBuf),
    /// Local git repository or path for dev installs.
    Local { path: PathBuf, rev: String },
}

impl IndexLocation {
    /// Address of the list for one exact interface version.
    #[must_use]
    pub fn describe_interface(&self, channel: &str, api: u32) -> String {
        let path = format!("{channel}/api/{api}.json");
        match self {
            Self::Secret(locator) => format!("{locator}/{path}"),
            Self::Url(base) => format!("{base}/{path}"),
            Self::Dir(dir) => dir.join(path).display().to_string(),
            Self::Local { path, rev } => format!("{}@{rev}", path.display()),
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
            Self::Secret(locator) => {
                let base = private_base(locator)?;
                Box::pin(Self::Url(base).fetch_interface(channel, api))
                    .await
                    .map_err(|_| "private registry list download failed".to_string())
            }
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
            Self::Local { .. } => Err("local app sources do not fetch registry lists".into()),
        }
    }
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
        if raw.starts_with("lastsecrets://") {
            return Self::Secret(raw.to_string());
        }
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
            Self::Secret(locator) => format!("{locator}/{channel}.json"),
            Self::Url(base) => format!("{base}/{channel}.json"),
            Self::Dir(dir) => dir.join(format!("{channel}.json")).display().to_string(),
            Self::Local { path, rev } => format!("{}@{}", path.display(), rev),
        }
    }

    /// Fetch `<channel>.json` and `<channel>.json.sig`.
    ///
    /// # Errors
    /// Returns the transport or filesystem error, naming the address.
    pub async fn fetch(&self, channel: &str) -> Result<(Vec<u8>, Vec<u8>), String> {
        validate_channel(channel)?;
        match self {
            Self::Secret(_) => Err("private indexes use exact-interface lists".into()),
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
            Self::Local { .. } => Err(
                "local install source does not fetch from an index (use resolve_local)".to_string(),
            ),
        }
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

/// A channel name is one path segment: lowercase letters, digits, `-`.
///
/// # Errors
/// Returns a message naming the offending channel.
pub fn validate_channel(channel: &str) -> Result<(), String> {
    let ok = channel == "stable" || channel == "next";
    if ok {
        Ok(())
    } else {
        Err(format!(
            "invalid channel '{channel}' (expected stable or next)"
        ))
    }
}

fn private_base(locator: &str) -> Result<String, String> {
    let slug = locator
        .strip_prefix("lastsecrets://")
        .ok_or("invalid private registry locator")?;
    if slug.is_empty()
        || !slug
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
    {
        return Err("invalid private registry locator".into());
    }
    let result = std::process::Command::new("lastsecrets")
        .args(["get", slug])
        .output()
        .map_err(|_| "private registry lookup failed")?;
    if !result.status.success() {
        return Err("private registry lookup failed".into());
    }
    let value = String::from_utf8(result.stdout).map_err(|_| "invalid private registry base")?;
    let base = value.trim().trim_end_matches('/');
    let url = reqwest::Url::parse(base).map_err(|_| "invalid private registry base")?;
    if base.len() > 2048
        || url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("invalid private registry base".into());
    }
    Ok(base.to_string())
}
