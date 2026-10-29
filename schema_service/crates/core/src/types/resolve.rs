use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::reuse::SchemaReuseMatch;

/// One proposed schema for the stateless resolve endpoint.
///
/// This intentionally carries only proposal metadata. `POST
/// /v1/schemas/resolve` is a read path: it never registers this shape as a
/// schema, never creates canonical fields, and never records shared discovery
/// state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaResolveProposal {
    pub descriptive_name: String,
    pub fields: Vec<String>,
    #[serde(default)]
    pub field_descriptions: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purpose_statement: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_app_id: Option<String>,
}

/// Stateless schema resolve request for local cache misses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaResolveRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_registry_version: Option<u64>,
    pub proposals: Vec<SchemaResolveProposal>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SchemaResolveOutcome {
    Reuse,
    Novel,
    CandidateEquivalent,
    Refresh,
}

/// Per-proposal stateless resolve decision.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaResolveResult {
    pub outcome: SchemaResolveOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_shared_schema_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r#match: Option<SchemaReuseMatch>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub candidates: Vec<SchemaReuseMatch>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub candidate_shared_schema_hashes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaResolveResponse {
    pub registry_version: u64,
    pub cache_stale: bool,
    pub results: HashMap<String, SchemaResolveResult>,
}
