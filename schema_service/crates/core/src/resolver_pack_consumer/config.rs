//! Bootstrap configuration for the resolver pack consumer.

use std::path::PathBuf;

use super::*;

/// Installation-owned bootstrap configuration for the native resolver runtime.
///
/// Downloaded pack content never supplies origin or trust roots; those come
/// only from this install-owned config.
#[derive(Debug, Clone)]
pub struct ResolverBootstrapConfig {
    pub enabled: bool,
    pub env: Env,
    /// HTTPS origin for pack objects (empty allowed for injected-store tests).
    pub base_url: String,
    pub refresh_interval_seconds: u64,
    pub refresh_jitter_seconds: u64,
    pub request_timeout_seconds: u64,
    pub max_download_bytes: u64,
    /// Optional freshness ceiling for pack manifest age, in seconds.
    pub max_config_age_seconds: Option<i64>,
    pub expected_embedder_id: String,
    pub cache_dir: PathBuf,
    pub trusted_keys: Vec<TrustedResolverPackKey>,
    /// Test hook for deterministic stale-pack checks.
    pub now: Option<crate::clock::FrozenInstant>,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum ResolverBootstrapConfigError {
    #[error("resolver bootstrap base_url rejected: {0}")]
    OriginRejected(String),
    #[error("resolver bootstrap base_url is invalid: {0}")]
    InvalidUrl(String),
}

impl ResolverBootstrapConfig {
    pub fn validate_bootstrap(&self) -> Result<(), ResolverBootstrapConfigError> {
        validate_bootstrap_base_url(&self.base_url)
    }
}

/// Reject non-HTTPS origins except empty (injected store) and localhost HTTP for tests.
pub fn validate_bootstrap_base_url(base_url: &str) -> Result<(), ResolverBootstrapConfigError> {
    if base_url.is_empty() {
        return Ok(());
    }
    let trimmed = base_url.trim();
    if trimmed.is_empty() {
        return Ok(());
    }
    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("https://") {
        if trimmed.len() <= "https://".len() {
            return Err(ResolverBootstrapConfigError::InvalidUrl(
                "https URL missing host".to_string(),
            ));
        }
        return Ok(());
    }
    if lower.starts_with("http://127.0.0.1") || lower.starts_with("http://localhost") {
        return Ok(());
    }
    if lower.starts_with("http://") {
        return Err(ResolverBootstrapConfigError::OriginRejected(
            "http is only allowed for localhost/127.0.0.1 test origins".to_string(),
        ));
    }
    Err(ResolverBootstrapConfigError::OriginRejected(format!(
        "unsupported scheme or origin (require https://, empty, or localhost http): {trimmed}"
    )))
}
