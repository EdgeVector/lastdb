//! Stable wire types for proposal resolution and shared-surface publishing.

use super::*;

// ---------------------------------------------------------------------------
// Resolution-path metadata
// ---------------------------------------------------------------------------

/// Resolution path. Publish-attach serializes this as `{"kind":"live"}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum FacadePath {
    Live,
}

// ---------------------------------------------------------------------------
// Resolve I/O types
// ---------------------------------------------------------------------------

/// Proposal with a required stable id (never key only by descriptive_name).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FacadeProposal {
    pub proposal_id: String,
    pub proposal: SchemaResolveProposal,
}

/// Per-proposal facade result, always keyed by `proposal_id`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FacadeResolveItem {
    pub proposal_id: String,
    pub result: SchemaResolveResult,
    pub path: FacadePath,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_reason: Option<&'static str>,
}

// ---------------------------------------------------------------------------
// Shared-surface publish/attach types (returned to Mini for local persistence)
// ---------------------------------------------------------------------------

/// Publish/attach request: core shared-surface envelope + proposal fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedSurfacePublishRequest {
    pub request: SharedSurfacePublishAttachRequest,
    pub descriptive_name: String,
    pub fields: Vec<String>,
    #[serde(default)]
    pub field_descriptions: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purpose_statement: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_identity_hash: Option<String>,
}

/// Outcome of a publish/attach attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SharedSurfacePublishOutcome {
    AttachedExisting,
    RegisteredLive,
    NeedsHumanOrLiveCreate,
    Rejected,
}

/// Attachment record Mini persists locally (not cloud-persisted in this PR).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedSurfaceAttachmentRecord {
    pub local_schema_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shared_schema_hash: Option<String>,
    pub surface: SharedSurfaceMetadata,
    /// RFC3339 timestamp.
    pub attached_at: String,
    /// `"local_reuse" | "live_resolve" | "live_register"`.
    pub source: String,
}

/// Result returned to Mini after publish/attach.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedSurfacePublishResult {
    pub local_schema_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shared_schema_hash: Option<String>,
    pub outcome: SharedSurfacePublishOutcome,
    pub path: FacadePath,
    pub attachment: SharedSurfaceAttachmentRecord,
}
