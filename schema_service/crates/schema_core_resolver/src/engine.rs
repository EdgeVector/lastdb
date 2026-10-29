//! Native `native_component_cover@1` evaluator.
//!
//! Production port of the 2026-07-06 **embedding-beam** component-cover eval
//! (`schema_service/eval/component_cover.mjs` + brain reference
//! `local-schema-field-embedding-component-cover-2026-07-06`).
//!
//! Local resolution may only **reuse** existing registry schemas / field
//! components. It never creates a canonical schema.

use std::collections::{HashMap, HashSet};

use crate::abi::{
    FieldCoverageEvidence, ProposalEmbeddings, ProposalMetadata, RegistryEmbeddings,
    RegistryFieldHandle, RegistryMetadata, ResidueFieldEvidence, ResolverDecision,
    ResolverEvidence, ResolverOutput, SchemaScoreEvidence, UseExistingResolution,
};
use crate::config::{
    ResolverConfig, ResolverConfigError, NATIVE_COMPONENT_COVER_ALGORITHM_ID,
    NATIVE_COMPONENT_COVER_ALGORITHM_VERSION,
};
use crate::embedding::cosine_similarity;

mod beam;
mod cover;
pub use cover::evaluate_native;
mod shortlist;

/// Input contract for the native evaluator (v1).
#[derive(Debug, Clone)]
pub struct NativeResolverInput {
    pub resolver_contract_version: u32,
    pub proposal_metadata: ProposalMetadata,
    pub proposal_embeddings: ProposalEmbeddings,
    pub registry_metadata: RegistryMetadata,
    pub registry_embeddings: RegistryEmbeddings,
    pub config: ResolverConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeResolverError {
    UnsupportedContractVersion(u32),
    Config(ResolverConfigError),
    OversizedProposalFields,
    OversizedRegistry,
    MissingEmbedding(String),
    DimensionMismatch,
    NonFiniteEmbedding,
    DuplicateId(String),
    WorkLimitExceeded,
}

impl std::fmt::Display for NativeResolverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedContractVersion(v) => {
                write!(f, "unsupported resolver_contract_version {v}")
            }
            Self::Config(e) => write!(f, "config: {e}"),
            Self::OversizedProposalFields => write!(f, "proposal exceeds max_proposal_fields"),
            Self::OversizedRegistry => write!(f, "registry exceeds max_registry_schemas"),
            Self::MissingEmbedding(id) => write!(f, "missing embedding for {id}"),
            Self::DimensionMismatch => write!(f, "embedding dimension mismatch"),
            Self::NonFiniteEmbedding => write!(f, "non-finite embedding value"),
            Self::DuplicateId(id) => write!(f, "duplicate id {id}"),
            Self::WorkLimitExceeded => write!(f, "work_limit_exceeded"),
        }
    }
}

impl std::error::Error for NativeResolverError {}

struct NativeComponentCover<'a> {
    input: &'a NativeResolverInput,
    work_units: u64,
    name_emb: HashMap<String, &'a [f32]>,
    field_emb: HashMap<String, &'a [f32]>,
    fields_by_schema: HashMap<String, Vec<&'a RegistryFieldHandle>>,
}

#[derive(Clone)]
struct FieldMatch {
    proposal_field_id: String,
    score: f32,
}

#[derive(Clone)]
struct SchemaCandidate {
    schema_id: String,
    score: f32,
    covered: Vec<FieldMatch>,
}

fn validate_vector(v: &[f32], max_dim: u32) -> Result<(), NativeResolverError> {
    if v.is_empty() || v.len() as u32 > max_dim {
        return Err(NativeResolverError::DimensionMismatch);
    }
    if v.iter().any(|x| !x.is_finite()) {
        return Err(NativeResolverError::NonFiniteEmbedding);
    }
    Ok(())
}

fn field_types_compatible(a: &str, b: &str) -> bool {
    let na = normalize_type(a);
    let nb = normalize_type(b);
    na == "any" || nb == "any" || na == nb
}

fn normalize_type(t: &str) -> String {
    t.trim().to_ascii_lowercase()
}

fn coverage_from_matches(
    covered: &[FieldMatch],
    proposal: &ProposalMetadata,
) -> FieldCoverageEvidence {
    let covered_ids: HashSet<&str> = covered
        .iter()
        .map(|m| m.proposal_field_id.as_str())
        .collect();
    let required_total = proposal.fields.iter().filter(|f| f.required).count() as u32;
    let required_covered = proposal
        .fields
        .iter()
        .filter(|f| f.required && covered_ids.contains(f.proposal_field_id.as_str()))
        .count() as u32;
    FieldCoverageEvidence {
        covered_fields: covered_ids.len() as u32,
        total_fields: proposal.fields.len() as u32,
        required_fields_covered: required_covered,
        required_fields_total: required_total,
    }
}

fn residue_from_matches(
    covered: &[FieldMatch],
    proposal: &ProposalMetadata,
) -> Vec<ResidueFieldEvidence> {
    let covered_ids: HashSet<&str> = covered
        .iter()
        .map(|m| m.proposal_field_id.as_str())
        .collect();
    proposal
        .fields
        .iter()
        .filter(|f| !covered_ids.contains(f.proposal_field_id.as_str()))
        .map(|f| ResidueFieldEvidence {
            proposal_field_id: f.proposal_field_id.clone(),
            reason: "unmatched_or_ambiguous_field".into(),
        })
        .collect()
}

fn use_existing_output(
    candidates: &[SchemaCandidate],
    evidence_scores: Vec<SchemaScoreEvidence>,
    coverage: FieldCoverageEvidence,
    proposal: &ProposalMetadata,
) -> ResolverOutput {
    let best = &candidates[0];
    ResolverOutput {
        abi_version: 1,
        decision: ResolverDecision::UseExisting,
        confidence: best.score,
        evidence: ResolverEvidence {
            schema_scores: evidence_scores,
            field_coverage: coverage,
            ambiguity_margin: candidates.get(1).map(|c| best.score - c.score),
            residue_fields: residue_from_matches(&best.covered, proposal),
        },
        use_existing: Some(UseExistingResolution {
            schema_id: best.schema_id.clone(),
        }),
        use_components: vec![],
        expand_existing_if_allowed: None,
        fallback_reasons: vec![],
    }
}
