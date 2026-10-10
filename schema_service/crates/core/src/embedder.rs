//! Embedder abstraction for the schema service.
//!
//! `schema_service_core` defines the trait and a lightweight error type so
//! the crate stays free of fastembed/ONNX dependencies. Callers (the dev
//! binary in `schema_service_server_http`, the Lambda in
//! `schema_service_server_lambda`, tests) construct an implementation
//! (e.g. a `FastEmbedModel` adapter in `schema_service_server_shared`) and
//! inject it via `SchemaServiceState::new`.

use std::fmt;

/// Trait for embedding text into a fixed-dimension float vector.
pub trait Embedder: Send + Sync {
    fn embed_text(&self, text: &str) -> Result<Vec<f32>, EmbedError>;

    /// Stable identifier for the embedding model behind this `Embedder`.
    /// Surfaced as `embedder_version` in the snapshot envelope and
    /// matched on import so callers cannot silently mix vectors from
    /// different models. Default is `"unknown"` for implementations that
    /// haven't opted in.
    fn embedder_id(&self) -> &'static str {
        "unknown"
    }
}

/// Runtime no-embedder implementation for default local/dev builds.
///
/// Schema matching paths that can use exact names, resolver packs, imported
/// artifacts, or injected test embedders still work. Paths that truly need a
/// local embedding model degrade explicitly instead of initializing FastEmbed.
pub struct DisabledEmbeddingModel;

impl Embedder for DisabledEmbeddingModel {
    fn embedder_id(&self) -> &'static str {
        "disabled/no-embedder"
    }

    fn embed_text(&self, _text: &str) -> Result<Vec<f32>, EmbedError> {
        Err(EmbedError::InitFailed(
            "schema semantic embedder is disabled; use resolver packs/artifacts or enable an explicit FastEmbed feature".to_string(),
        ))
    }
}

/// Lightweight error returned by [`Embedder::embed_text`]. Implementations
/// map their native errors (fastembed, HTTP, etc.) into one of these
/// variants.
#[derive(Debug, Clone)]
pub enum EmbedError {
    /// The embedder could not initialize (model download / load failure).
    InitFailed(String),
    /// The embedder was initialized but failed to produce an embedding
    /// for the given input.
    EmbedFailed(String),
}

impl fmt::Display for EmbedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InitFailed(msg) => write!(f, "embedder init failed: {msg}"),
            Self::EmbedFailed(msg) => write!(f, "embedding failed: {msg}"),
        }
    }
}

impl std::error::Error for EmbedError {}

/// Cosine similarity lives once in `schema_core_resolver`.
pub use schema_core_resolver::cosine_similarity;
