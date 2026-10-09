//! Embedding vector records and pure vector math used by the native resolver.
//!
//! No embedder trait and no model download — callers supply precomputed vectors
//! (as Schema Service does for pack and live resolve).

use serde::{Deserialize, Serialize};

/// One embedding keyed by a stable target id (schema id, field id, …).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EmbeddingVectorRecord {
    pub target_id: String,
    pub vector: Vec<f32>,
}

/// Cosine similarity between two float vectors.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        0.0
    } else {
        dot / (norm_a * norm_b)
    }
}
