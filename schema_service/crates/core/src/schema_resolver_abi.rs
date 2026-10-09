//! Schema resolver ABI types — owned by `schema_core_resolver`.

pub use schema_core_resolver::abi::{
    ComponentResolution, ExpandExistingResolution, FieldCoverageEvidence, ProposalEmbeddings,
    ProposalFieldMetadata, ProposalMetadata, RegistryCanonicalFieldHandle, RegistryEmbeddings,
    RegistryFieldHandle, RegistryMetadata, RegistrySchemaHandle, ResidueFieldEvidence,
    ResolverDecision, ResolverEvidence, ResolverOutput, SchemaScoreEvidence, UseExistingResolution,
    SCHEMA_RESOLVER_ABI_VERSION,
};
pub use schema_core_resolver::EmbeddingVectorRecord;
