//! Local-first modes, path metadata and facade request/result types.

use super::*;

// ---------------------------------------------------------------------------
// Modes & path metadata
// ---------------------------------------------------------------------------

/// Local-first resolver operating mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalFirstMode {
    /// Always call live; if a local pack is active, evaluate locally for
    /// comparison only. Caller always receives live results.
    Shadow,
    /// Return local UseExisting/UseComponents when high-confidence
    /// (route = UseLocal); otherwise fall back to live for that proposal.
    EnforceExistingOnly,
    /// Kill switch: never use the local pack; always live.
    LiveOnly,
}

/// How a single proposal was resolved for the caller.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum FacadePath {
    Local,
    Live,
    Shadow {
        local: Option<LocalShadowSummary>,
        /// Boxed: `SchemaResolveResult` is large (full reuse match payload).
        live: Box<SchemaResolveResult>,
        disagreement: DisagreementClass,
    },
}

/// Bounded local-side summary for shadow comparison (no proposal content).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalShadowSummary {
    pub decision: ResolverDecision,
    pub confidence: f32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_schema_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub component_schema_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<String>,
}

/// Classification of local vs live disagreement (telemetry only).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DisagreementClass {
    None,
    /// Local reuse, live novel or different identity.
    UnsafeLocalReuse,
    /// Local miss/fallback, live reuse.
    MissedReuse,
    /// Both reuse-ish but different ids/components.
    EquivalentDifferentPlan,
    Other,
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
    pub local_decision: Option<ResolverDecision>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_reason: Option<&'static str>,
    /// Pack format version when a pack was active (no proposal content).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pack_format_version: Option<u32>,
    /// Resolver config format version when a pack was active.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_format_version: Option<u32>,
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
