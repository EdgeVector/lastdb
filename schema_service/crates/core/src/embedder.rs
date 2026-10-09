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

/// L2-normalize `vec` in place. No-op if the norm is zero (e.g. all-zero input).
#[cfg(any(test, feature = "test-utils"))]
fn l2_normalize_in_place(vec: &mut [f32]) {
    let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in vec.iter_mut() {
            *x /= norm;
        }
    }
}

/// Mock embedder for tests — deterministic, no download required.
/// Hash-based approach assigns each unique input a near-orthogonal direction,
/// keeping different field names at low cosine similarity (< 0.5).
#[cfg(any(test, feature = "test-utils"))]
pub struct MockEmbeddingModel;

#[cfg(any(test, feature = "test-utils"))]
impl Embedder for MockEmbeddingModel {
    fn embedder_id(&self) -> &'static str {
        "mock/hash-384"
    }

    fn embed_text(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        text.hash(&mut hasher);
        let hash = hasher.finish();

        let mut vec = vec![0.0f32; 384];
        for i in 0..4 {
            let idx = ((hash >> (i * 16)) & 0xFF) as usize % 384;
            let sign = if (hash >> (i * 8 + 4)) & 1 == 0 {
                1.0
            } else {
                -1.0
            };
            vec[idx] += sign * (4.0 - i as f32);
        }
        l2_normalize_in_place(&mut vec);
        Ok(vec)
    }
}

/// Scripted embedder for tests — returns pre-configured vectors for specific
/// inputs. Use when you need to control exact similarity values between
/// fields.
#[cfg(any(test, feature = "test-utils"))]
pub struct ScriptedEmbeddingModel {
    /// Map from input text → embedding vector. Falls back to
    /// [`MockEmbeddingModel`] for unknown inputs.
    pub responses: std::collections::HashMap<String, Vec<f32>>,
}

#[cfg(any(test, feature = "test-utils"))]
impl ScriptedEmbeddingModel {
    pub fn new(responses: std::collections::HashMap<String, Vec<f32>>) -> Self {
        Self { responses }
    }

    /// Unit vector pointing in the given direction index (out of 384 dims).
    /// Two vectors with nearby direction indices have high cosine similarity.
    pub fn unit_vec(direction: usize) -> Vec<f32> {
        let mut vec = vec![0.0f32; 384];
        vec[direction % 384] = 1.0;
        vec
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl Embedder for ScriptedEmbeddingModel {
    fn embedder_id(&self) -> &'static str {
        "scripted/test"
    }

    fn embed_text(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        if let Some(vec) = self.responses.get(text) {
            return Ok(vec.clone());
        }
        let mut vec = vec![0.0f32; 384];
        for (i, byte) in text.bytes().enumerate() {
            vec[i % 384] += byte as f32;
        }
        l2_normalize_in_place(&mut vec);
        Ok(vec)
    }
}
