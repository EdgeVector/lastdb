//! Resolver configuration — owned by `schema_core_resolver`.

pub use schema_core_resolver::config::{
    AlgorithmSelector, CandidateGenerationConfig, ComponentScoringConfig, CoverageConfig,
    FieldScoringConfig, LimitsConfig, PermissionsConfig, ResolverConfig, ResolverConfigError,
    RolloutConfig, RolloutMode, SchemaScoreWeights, SchemaScoringConfig, ScoringConfig,
    SimilarityMetric, HARD_MAX_BEAM_WIDTH, HARD_MAX_COMPONENTS_PER_RESOLUTION,
    HARD_MAX_COMPONENT_CANDIDATES, HARD_MAX_EMBEDDING_DIMENSIONS, HARD_MAX_FIELD_CANDIDATES,
    HARD_MAX_PROPOSAL_FIELDS, HARD_MAX_REGISTRY_SCHEMAS, HARD_MAX_SCHEMA_CANDIDATES,
    HARD_MAX_WORK_UNITS, NATIVE_COMPONENT_COVER_ALGORITHM_ID,
    NATIVE_COMPONENT_COVER_ALGORITHM_VERSION, RESOLVER_CONFIG_FORMAT_VERSION,
};
