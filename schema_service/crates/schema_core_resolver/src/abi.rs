//! Schema resolver input/output contract types.
//!
//! These pure types are shared by the native evaluator and pack consumer.
//! Executable WASM host code was removed in pack format_version 2; local
//! resolution is implemented by `native_schema_resolver`.

use serde::{Deserialize, Serialize};

use crate::embedding::EmbeddingVectorRecord;

pub const SCHEMA_RESOLVER_ABI_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProposalMetadata {
    pub proposal_id: String,
    pub descriptive_name: String,
    pub fields: Vec<ProposalFieldMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProposalFieldMetadata {
    pub proposal_field_id: String,
    pub field_name: String,
    pub field_type: String,
    pub required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ProposalEmbeddings {
    pub descriptive_name: Vec<f32>,
    pub field_contexts: Vec<EmbeddingVectorRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RegistryMetadata {
    pub schemas: Vec<RegistrySchemaHandle>,
    pub fields: Vec<RegistryFieldHandle>,
    pub canonical_fields: Vec<RegistryCanonicalFieldHandle>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RegistrySchemaHandle {
    pub schema_id: String,
    pub descriptive_name: String,
    pub lifecycle: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RegistryFieldHandle {
    pub field_id: String,
    pub schema_id: String,
    pub field_name: String,
    pub field_type: String,
    pub canonical_field_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RegistryCanonicalFieldHandle {
    pub canonical_field_id: String,
    pub field_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RegistryEmbeddings {
    pub descriptive_names: Vec<EmbeddingVectorRecord>,
    pub schema_field_contexts: Vec<EmbeddingVectorRecord>,
    pub canonical_fields: Vec<EmbeddingVectorRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ResolverOutput {
    pub abi_version: u32,
    pub decision: ResolverDecision,
    pub confidence: f32,
    pub evidence: ResolverEvidence,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub use_existing: Option<UseExistingResolution>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub use_components: Vec<ComponentResolution>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expand_existing_if_allowed: Option<ExpandExistingResolution>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fallback_reasons: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResolverDecision {
    UseExisting,
    UseComponents,
    ExpandExistingIfAllowed,
    Ambiguous,
    NeedsLiveSchemaService,
    Reject,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ResolverEvidence {
    pub schema_scores: Vec<SchemaScoreEvidence>,
    pub field_coverage: FieldCoverageEvidence,
    pub ambiguity_margin: Option<f32>,
    /// Proposal fields that were not safely mapped. These entries identify
    /// fields by stable proposal-local id and reason only; they intentionally
    /// omit raw field values or descriptions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub residue_fields: Vec<ResidueFieldEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SchemaScoreEvidence {
    pub schema_id: String,
    pub score: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FieldCoverageEvidence {
    pub covered_fields: u32,
    pub total_fields: u32,
    pub required_fields_covered: u32,
    pub required_fields_total: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResidueFieldEvidence {
    pub proposal_field_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct UseExistingResolution {
    pub schema_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ComponentResolution {
    pub proposal_field_id: String,
    pub schema_id: String,
    pub confidence: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExpandExistingResolution {
    pub schema_id: String,
    pub added_proposal_field_ids: Vec<String>,
}
