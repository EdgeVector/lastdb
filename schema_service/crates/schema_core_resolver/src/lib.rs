//! # schema_core_resolver
//!
//! Encapsulated schema **match** engine shared by Schema Service and Schema Mini.
//!
//! ## Scope
//!
//! - Same native algorithm as Schema Service (`native_component_cover@1`).
//! - Callers pass a complete catalog snapshot (registry + embeddings) and a
//!   [`ResolverConfig`] on every call.
//! - Decide: **resolved** against the catalog, or **unresolvable** (a new schema
//!   may be created by the caller via Schema Service).
//!
//! ## Non-goals
//!
//! - HTTP, identity, PoW, app registry
//! - Catalog storage / sync
//! - Minting schemas (callers create via Schema Service when unresolvable)
//! - Running an embedding model (callers supply vectors)
//!
//! ## Product mapping
//!
//! ```text
//! resolve(catalog, proposal, config) -> Resolved | Unresolvable
//! Unresolvable  =>  caller may create a new schema via Schema Service
//! ```

pub mod abi;
pub mod config;
pub mod embedding;
pub mod engine;

pub use abi::{
    ComponentResolution, ExpandExistingResolution, FieldCoverageEvidence, ProposalEmbeddings,
    ProposalFieldMetadata, ProposalMetadata, RegistryCanonicalFieldHandle, RegistryEmbeddings,
    RegistryFieldHandle, RegistryMetadata, RegistrySchemaHandle, ResidueFieldEvidence,
    ResolverDecision, ResolverEvidence, ResolverOutput, SchemaScoreEvidence, UseExistingResolution,
    SCHEMA_RESOLVER_ABI_VERSION,
};
pub use config::{
    AlgorithmSelector, CandidateGenerationConfig, LimitsConfig, PermissionsConfig, ResolverConfig,
    ResolverConfigError, RolloutConfig, RolloutMode, ScoringConfig,
    NATIVE_COMPONENT_COVER_ALGORITHM_ID, NATIVE_COMPONENT_COVER_ALGORITHM_VERSION,
    RESOLVER_CONFIG_FORMAT_VERSION,
};
pub use embedding::{cosine_similarity, EmbeddingVectorRecord};
pub use engine::{evaluate_native, NativeResolverError, NativeResolverInput};

use abi::ResolverDecision as Decision;

/// Top-level outcome for a complete-catalog resolve.
#[derive(Debug, Clone, PartialEq)]
pub enum ResolveVerdict {
    /// Safe reuse / composition / expand against the catalog.
    Resolved(ResolverOutput),
    /// No safe catalog match — caller may create a new schema (or handle
    /// ambiguity/reject using engine evidence).
    Unresolvable(Unresolvable),
}

/// Why resolve did not attach an existing catalog identity.
#[derive(Debug, Clone, PartialEq)]
pub struct Unresolvable {
    /// Engine decision that mapped to unresolvable.
    pub decision: ResolverDecision,
    /// Full engine output (scores, residue fields, fallback reasons).
    pub output: ResolverOutput,
}

impl Unresolvable {
    /// Engine found no safe existing match in this catalog.
    ///
    /// Under a complete catalog, the engine decision historically named
    /// `NeedsLiveSchemaService` means “no match here” — the product action is
    /// create via Schema Service, not a weaker second resolve.
    pub fn is_no_match(&self) -> bool {
        matches!(self.decision, Decision::NeedsLiveSchemaService)
    }
}

/// Map a raw engine decision into the product verdict for a complete catalog.
pub fn verdict_from_output(output: ResolverOutput) -> ResolveVerdict {
    match output.decision {
        Decision::UseExisting | Decision::UseComponents | Decision::ExpandExistingIfAllowed => {
            ResolveVerdict::Resolved(output)
        }
        Decision::Ambiguous | Decision::NeedsLiveSchemaService | Decision::Reject => {
            let decision = output.decision;
            ResolveVerdict::Unresolvable(Unresolvable { decision, output })
        }
    }
}

/// Run the shared native resolver and return a product verdict.
///
/// Identical algorithm to Schema Service when given the same catalog snapshot,
/// embeddings, config, and proposal.
pub fn resolve(input: &NativeResolverInput) -> Result<ResolveVerdict, NativeResolverError> {
    let output = evaluate_native(input)?;
    Ok(verdict_from_output(output))
}
