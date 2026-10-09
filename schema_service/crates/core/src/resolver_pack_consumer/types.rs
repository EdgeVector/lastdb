//! Fetch results, load outcomes, loaded pack and consumer error types.

use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectFetchMeta {
    pub etag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectFetchResult {
    NotFound,
    NotModified {
        etag: Option<String>,
    },
    Found {
        bytes: Vec<u8>,
        etag: Option<String>,
    },
}

#[async_trait]
pub trait ResolverPackObjectStore: Send + Sync {
    async fn get_object(&self, key: &str) -> Result<Option<Vec<u8>>, ResolverPackFetchError>;

    /// Optional conditional fetch. Default impl calls [`get_object`] and ignores
    /// `if_none_match` (returns `Found` without an etag).
    async fn get_object_conditional(
        &self,
        key: &str,
        _if_none_match: Option<&str>,
    ) -> Result<ObjectFetchResult, ResolverPackFetchError> {
        match self.get_object(key).await? {
            None => Ok(ObjectFetchResult::NotFound),
            Some(bytes) => Ok(ObjectFetchResult::Found { bytes, etag: None }),
        }
    }
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum ResolverPackFetchError {
    #[error("resolver-pack transport error: {0}")]
    Transport(String),
    #[error("resolver-pack fetch timed out")]
    Timeout,
    #[error("resolver-pack response truncated")]
    Truncated,
    #[error("resolver-pack response exceeds max download bytes")]
    ResponseTooLarge,
    #[error("resolver-pack HTTP status {0}")]
    HttpStatus(u16),
    #[error("resolver-pack invalid URL: {0}")]
    InvalidUrl(String),
    #[error("resolver-pack origin rejected: {0}")]
    OriginRejected(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolverPackLoadSource {
    Latest,
    LastKnownGood,
    /// Conditional GET returned 304 / not modified.
    NotModified,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolverPackFallbackReason {
    Disabled,
    MissingPack,
    StalePack,
    BadSignature,
    WrongPurpose,
    WrongEnv,
    UnsupportedAbi,
    HashMismatch,
    PolicyRejected,
    IncompatibleEmbedder,
    ArtifactVerificationFailed,
    ArtifactFetchFailed,
}

impl ResolverPackFallbackReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::MissingPack => "missing_pack",
            Self::StalePack => "stale_pack",
            Self::BadSignature => "bad_signature",
            Self::WrongPurpose => "wrong_purpose",
            Self::WrongEnv => "wrong_env",
            Self::UnsupportedAbi => "unsupported_abi",
            Self::HashMismatch => "hash_mismatch",
            Self::PolicyRejected => "policy_rejected",
            Self::IncompatibleEmbedder => "incompatible_embedder",
            Self::ArtifactVerificationFailed => "artifact_verification_failed",
            Self::ArtifactFetchFailed => "artifact_fetch_failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolverPackLoadOutcome {
    Loaded { source: ResolverPackLoadSource },
    LiveServiceFallback { reason: ResolverPackFallbackReason },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ImportedResolverPackEmbeddings {
    pub descriptive_names: BTreeMap<String, Vec<f32>>,
    pub schema_field_contexts: BTreeMap<String, Vec<f32>>,
    pub canonical_fields: BTreeMap<String, Vec<f32>>,
}

impl ImportedResolverPackEmbeddings {
    pub fn total(&self) -> usize {
        self.descriptive_names.len()
            + self.schema_field_contexts.len()
            + self.canonical_fields.len()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LoadedResolverPack {
    pub manifest: ResolverPackManifest,
    pub resolver_config: ResolverConfig,
    pub schema_snapshot: SchemaSnapshotArtifact,
    pub embedding_artifact: EmbeddingArtifact,
    pub registry_embeddings: ImportedResolverPackEmbeddings,
}

impl LoadedResolverPack {
    pub fn registry_embeddings_for_abi(&self) -> RegistryEmbeddings {
        RegistryEmbeddings {
            descriptive_names: self
                .registry_embeddings
                .descriptive_names
                .iter()
                .map(
                    |(target_id, vector)| crate::resolver_pack::EmbeddingVectorRecord {
                        target_id: target_id.clone(),
                        vector: vector.clone(),
                    },
                )
                .collect(),
            schema_field_contexts: self
                .registry_embeddings
                .schema_field_contexts
                .iter()
                .map(
                    |(target_id, vector)| crate::resolver_pack::EmbeddingVectorRecord {
                        target_id: target_id.clone(),
                        vector: vector.clone(),
                    },
                )
                .collect(),
            canonical_fields: self
                .registry_embeddings
                .canonical_fields
                .iter()
                .map(
                    |(target_id, vector)| crate::resolver_pack::EmbeddingVectorRecord {
                        target_id: target_id.clone(),
                        vector: vector.clone(),
                    },
                )
                .collect(),
        }
    }

    pub fn route_decision(&self, decision: ResolverDecision) -> ResolverPackResolutionRoute {
        match decision {
            ResolverDecision::UseExisting if self.resolver_config.permissions.use_existing => {
                ResolverPackResolutionRoute::UseLocal
            }
            ResolverDecision::UseComponents if self.resolver_config.permissions.use_components => {
                ResolverPackResolutionRoute::UseLocal
            }
            ResolverDecision::ExpandExistingIfAllowed
                if self.resolver_config.permissions.expand_existing =>
            {
                ResolverPackResolutionRoute::UseLocal
            }
            ResolverDecision::NeedsLiveSchemaService => {
                ResolverPackResolutionRoute::LiveServiceFallback {
                    reason: "needs_live_schema_service",
                }
            }
            ResolverDecision::Ambiguous => ResolverPackResolutionRoute::LiveServiceFallback {
                reason: "ambiguous_candidates",
            },
            ResolverDecision::Reject => ResolverPackResolutionRoute::LiveServiceFallback {
                reason: "resolver_reject",
            },
            _ => ResolverPackResolutionRoute::LiveServiceFallback {
                reason: "policy_requires_live_check",
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolverPackResolutionRoute {
    UseLocal,
    LiveServiceFallback { reason: &'static str },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResolverPackTelemetrySnapshot {
    pub fetch_outcomes: BTreeMap<String, u64>,
    pub verification_outcomes: BTreeMap<String, u64>,
    pub import_outcomes: BTreeMap<String, u64>,
    pub fallback_reasons: BTreeMap<String, u64>,
    pub resolution_routes: BTreeMap<String, u64>,
}

#[derive(Debug, Error)]
pub enum ResolverPackConsumerError {
    #[error("resolver-pack fetch failed: {0}")]
    Fetch(#[from] ResolverPackFetchError),
    #[error("resolver-pack cache I/O failed: {0}")]
    CacheIo(#[from] std::io::Error),
    #[error("resolver-pack manifest parse failed: {0}")]
    ManifestJson(#[from] serde_json::Error),
    #[error("resolver-pack verification failed: {0}")]
    Verify(#[from] ResolverPackVerifyError),
    #[error("resolver-pack artifact missing at key {0}")]
    MissingArtifact(String),
    #[error("resolver-pack manifest generated_at is stale")]
    StaleManifest,
    #[error("resolver-pack manifest generated_at parse failed: {0}")]
    BadGeneratedAt(String),
    #[error("resolver-pack embedding vector for {class} target {target:?} has length {actual}, expected {expected}")]
    EmbeddingDimensionMismatch {
        class: &'static str,
        target: String,
        expected: u32,
        actual: usize,
    },
}
