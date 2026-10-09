//! Phase C near-miss audit log for dual-signal schema canonicalization.
//!
//! Shadow mode (`SCHEMA_SHADOW_MODE=true`) runs the dual-signal algorithm
//! alongside the single-signal one on every registration. When the two
//! disagree the call is recorded as a [`NearMissRecord`] so ops can tune
//! τ_purpose before flipping the Phase B flag in production. Records are
//! persisted via [`crate::external_persistence::ExternalSchemaPersistence`]
//! and surfaced through `GET /v1/canonicalization-near-misses`.

use serde::{Deserialize, Serialize};

/// Decision label captured for both the single-signal and dual-signal
/// algorithms at the moment a registration was evaluated. Matches the
/// public-facing variants of `state::SchemaAddOutcome` minus the payload —
/// the audit log only needs the discriminator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NearMissDecision {
    /// Register as a new canonical schema.
    Added,
    /// Merge into the existing canonical (descriptive_name / Jaccard).
    Expanded,
    /// Identity hash already in the registry.
    AlreadyExists,
    /// 409: descriptive_name taken by a schema the dual-signal gate rejected.
    DescriptiveNameConflict,
}

/// Persisted record of a registration where the dual-signal algorithm would
/// have produced a different outcome than the single-signal one. Records are
/// append-only and content-addressed by `registration_id`. See the module
/// docstring for the shadow-mode workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NearMissRecord {
    /// UUID v4 generated at the registration call site. Used as the storage
    /// key so a repeated submission of the same record is a no-op.
    pub registration_id: String,
    /// `Schema::get_identity_hash()` of the proposed schema.
    pub candidate_schema_hash: String,
    /// `Schema::get_identity_hash()` of the existing canonical the candidate
    /// was being compared against.
    pub existing_canonical_hash: String,
    /// What the single-signal algorithm (current production behavior) decided.
    pub single_signal_decision: NearMissDecision,
    /// What the dual-signal algorithm would have decided if it were
    /// authoritative.
    pub dual_signal_decision: NearMissDecision,
    /// Cosine similarity between descriptive_name embeddings at decision time.
    pub struct_similarity: f32,
    /// Cosine similarity between `"{descriptive_name} — {purpose_statement}"`
    /// embeddings at decision time (the Phase B purpose signal).
    pub purpose_similarity: f32,
    /// RFC 3339 UTC timestamp.
    pub timestamp: String,
}
