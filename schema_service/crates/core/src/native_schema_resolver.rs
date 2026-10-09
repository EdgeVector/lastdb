//! Native schema match engine — owned by `schema_core_resolver`.
//!
//! Re-exported here so existing `schema_service_core::native_schema_resolver`
//! paths keep working.

pub use schema_core_resolver::engine::{evaluate_native, NativeResolverError, NativeResolverInput};
// Re-export config/abi types tests and callers often pull through this module.
pub use schema_core_resolver::{
    ComponentResolution, ProposalEmbeddings, ProposalFieldMetadata, ProposalMetadata,
    RegistryEmbeddings, RegistryFieldHandle, RegistryMetadata, RegistrySchemaHandle,
    ResidueFieldEvidence, ResolverConfig, ResolverDecision, ResolverEvidence, ResolverOutput,
    SchemaScoreEvidence, UseExistingResolution, SCHEMA_RESOLVER_ABI_VERSION,
};
