//! Consumer and bootstrap configuration for the resolver pack consumer.

use super::*;

#[derive(Debug, Clone)]
pub struct ResolverPackConsumerConfig {
    /// Keeps the consumer dev/off-by-default until node wiring explicitly opts in.
    pub enabled: bool,
    pub env: Env,
    pub expected_embedder_id: String,
    pub trusted_keys: Vec<TrustedResolverPackKey>,
    /// Optional freshness ceiling for the manifest's `generated_at`.
    pub max_pack_age_seconds: Option<i64>,
    /// Test hook for deterministic stale-pack checks.
    pub now: Option<DateTime<Utc>>,
}

impl ResolverPackConsumerConfig {
    pub fn dev(
        expected_embedder_id: impl Into<String>,
        trusted_keys: Vec<TrustedResolverPackKey>,
    ) -> Self {
        Self {
            enabled: false,
            env: Env::Dev,
            expected_embedder_id: expected_embedder_id.into(),
            trusted_keys,
            max_pack_age_seconds: None,
            now: None,
        }
    }
}

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
    /// Maps to consumer `max_pack_age_seconds`.
    pub max_config_age_seconds: Option<i64>,
    pub expected_embedder_id: String,
    pub cache_dir: PathBuf,
    pub trusted_keys: Vec<TrustedResolverPackKey>,
    /// Test hook for deterministic stale-pack checks.
    pub now: Option<DateTime<Utc>>,
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

    /// Cache root scoped by format/contract/algorithm/embedder identity.
    pub fn cache_identity_dir(&self) -> PathBuf {
        let embedder_digest = artifact_sha256_hex(self.expected_embedder_id.as_bytes());
        self.cache_dir
            .join(format!("format-v{RESOLVER_PACK_FORMAT_VERSION}"))
            .join(format!("contract-v{SUPPORTED_RESOLVER_CONTRACT_VERSION}"))
            .join(format!(
                "native_component_cover-v{NATIVE_COMPONENT_COVER_ALGORITHM_VERSION}"
            ))
            .join(format!("embedder-sha256-{embedder_digest}"))
    }

    pub fn to_consumer_config(&self) -> ResolverPackConsumerConfig {
        ResolverPackConsumerConfig {
            enabled: self.enabled,
            env: self.env,
            expected_embedder_id: self.expected_embedder_id.clone(),
            trusted_keys: self.trusted_keys.clone(),
            max_pack_age_seconds: self.max_config_age_seconds,
            now: self.now,
        }
    }

    pub fn fs_cache(&self) -> FsResolverPackCache {
        FsResolverPackCache::new(self.cache_identity_dir())
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
