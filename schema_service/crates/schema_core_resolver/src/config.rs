//! Signed declarative configuration for `native_component_cover@1`.
//!
//! Machine-readable baseline: `docs/resolver_config_v1.schema.json`.
//! Empirical thresholds draw from the embedding-beam component-cover eval
//! ([[local-schema-field-embedding-component-cover-2026-07-06]]).

use serde::{Deserialize, Serialize};
use std::fmt;

pub const RESOLVER_CONFIG_FORMAT_VERSION: u32 = 1;
pub const NATIVE_COMPONENT_COVER_ALGORITHM_ID: &str = "native_component_cover";
pub const NATIVE_COMPONENT_COVER_ALGORITHM_VERSION: u32 = 1;

/// Compiled hard ceilings. Downloaded config may only request lower values.
pub const HARD_MAX_PROPOSAL_FIELDS: u32 = 512;
pub const HARD_MAX_REGISTRY_SCHEMAS: u32 = 100_000;
pub const HARD_MAX_EMBEDDING_DIMENSIONS: u32 = 2048;
pub const HARD_MAX_WORK_UNITS: u64 = 2_000_000;
pub const HARD_MAX_SCHEMA_CANDIDATES: u32 = 256;
pub const HARD_MAX_FIELD_CANDIDATES: u32 = 64;
pub const HARD_MAX_COMPONENT_CANDIDATES: u32 = 128;
pub const HARD_MAX_COMPONENTS_PER_RESOLUTION: u32 = 8;
pub const HARD_MAX_BEAM_WIDTH: u32 = 32;

const WEIGHT_EPS: f32 = 1.0e-4;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ResolverConfig {
    pub format_version: u32,
    pub resolver_contract_version: u32,
    pub algorithm: AlgorithmSelector,
    pub policy_version: String,
    pub rollout: RolloutConfig,
    pub candidate_generation: CandidateGenerationConfig,
    pub scoring: ScoringConfig,
    pub permissions: PermissionsConfig,
    pub limits: LimitsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AlgorithmSelector {
    pub id: String,
    pub version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RolloutConfig {
    pub requested_mode: RolloutMode,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RolloutMode {
    Disabled,
    Shadow,
    Enforce,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CandidateGenerationConfig {
    pub schema_candidates: u32,
    pub field_candidates_per_schema: u32,
    pub component_candidates: u32,
    pub max_components_per_resolution: u32,
    pub require_compatible_field_types: bool,
    pub allow_deprecated_schemas: bool,
    /// Beam width for one-to-many cover (embedding-beam used 12).
    #[serde(default = "default_beam_width")]
    pub beam_width: u32,
}

fn default_beam_width() -> u32 {
    12
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ScoringConfig {
    pub similarity_metric: SimilarityMetric,
    pub schema: SchemaScoringConfig,
    pub field: FieldScoringConfig,
    pub coverage: CoverageConfig,
    pub components: ComponentScoringConfig,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SimilarityMetric {
    Cosine,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SchemaScoringConfig {
    pub descriptive_name_min_similarity: f32,
    pub use_existing_min_score: f32,
    pub ambiguity_margin: f32,
    pub weights: SchemaScoreWeights,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SchemaScoreWeights {
    pub descriptive_name: f32,
    pub field_match: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FieldScoringConfig {
    pub context_min_similarity: f32,
    pub canonical_min_similarity: f32,
    pub ambiguity_margin: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CoverageConfig {
    pub min_fields: f32,
    pub min_required_fields: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ComponentScoringConfig {
    pub min_score: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PermissionsConfig {
    pub use_existing: bool,
    pub use_components: bool,
    pub expand_existing: bool,
    pub create_canonical_schema: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LimitsConfig {
    pub max_proposal_fields: u32,
    pub max_registry_schemas: u32,
    pub max_embedding_dimensions: u32,
    pub max_work_units: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolverConfigError {
    UnsupportedFormatVersion(u32),
    UnsupportedAlgorithm { id: String, version: u32 },
    NonFinite(String),
    OutOfRange(String),
    WeightsDoNotSumToOne,
    ExceedsHardLimit(String),
    CanonicalCreationForbidden,
    EmptyPolicyVersion,
}

impl fmt::Display for ResolverConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedFormatVersion(v) => {
                write!(f, "unsupported resolver config format_version {v}")
            }
            Self::UnsupportedAlgorithm { id, version } => {
                write!(f, "unsupported algorithm {id}@{version}")
            }
            Self::NonFinite(field) => write!(f, "non-finite value in {field}"),
            Self::OutOfRange(field) => write!(f, "out-of-range value in {field}"),
            Self::WeightsDoNotSumToOne => {
                write!(f, "schema scoring weights must sum to 1.0")
            }
            Self::ExceedsHardLimit(field) => write!(f, "{field} exceeds compiled hard limit"),
            Self::CanonicalCreationForbidden => {
                write!(f, "create_canonical_schema must be false")
            }
            Self::EmptyPolicyVersion => write!(f, "policy_version must be non-empty"),
        }
    }
}

impl std::error::Error for ResolverConfigError {}

impl ResolverConfig {
    /// Conservative embedding-beam-inspired defaults for fixtures / shadow mode.
    ///
    /// Not production-calibrated enforcement thresholds — those require the
    /// disagreement/evaluation gates in the engineering plan.
    pub fn embedding_beam_shadow_defaults() -> Self {
        Self {
            format_version: RESOLVER_CONFIG_FORMAT_VERSION,
            resolver_contract_version: 1,
            algorithm: AlgorithmSelector {
                id: NATIVE_COMPONENT_COVER_ALGORITHM_ID.to_string(),
                version: NATIVE_COMPONENT_COVER_ALGORITHM_VERSION,
            },
            policy_version: "embedding-beam-shadow-2026-07-06".to_string(),
            rollout: RolloutConfig {
                requested_mode: RolloutMode::Shadow,
            },
            candidate_generation: CandidateGenerationConfig {
                // Eval used top candidates; keep bounded for native work units.
                schema_candidates: 80,
                field_candidates_per_schema: 24,
                component_candidates: 80,
                max_components_per_resolution: 4,
                require_compatible_field_types: true,
                allow_deprecated_schemas: false,
                beam_width: 12,
            },
            scoring: ScoringConfig {
                similarity_metric: SimilarityMetric::Cosine,
                schema: SchemaScoringConfig {
                    // embedding-beam minCandidateScore ~0.26 on composite score;
                    // for pure name shortlist we stay higher to cut false positives.
                    descriptive_name_min_similarity: 0.26,
                    use_existing_min_score: 0.92,
                    ambiguity_margin: 0.04,
                    weights: SchemaScoreWeights {
                        descriptive_name: 0.16, // intent 0.10 + name 0.06 from eval
                        field_match: 0.84,      // fieldCoverage 0.58 + fieldQuality 0.26
                    },
                },
                field: FieldScoringConfig {
                    // embedding-beam minFieldScore 0.46; production starts stricter.
                    context_min_similarity: 0.46,
                    canonical_min_similarity: 0.95,
                    ambiguity_margin: 0.03,
                },
                coverage: CoverageConfig {
                    min_fields: 0.95,
                    min_required_fields: 1.0,
                },
                components: ComponentScoringConfig { min_score: 0.26 },
            },
            permissions: PermissionsConfig {
                use_existing: true,
                // Component cover is the point of embedding-beam; enable in shadow.
                use_components: true,
                expand_existing: false,
                create_canonical_schema: false,
            },
            limits: LimitsConfig {
                max_proposal_fields: 256,
                max_registry_schemas: 50_000,
                max_embedding_dimensions: 1024,
                max_work_units: 250_000,
            },
        }
    }

    pub fn validate(&self) -> Result<(), ResolverConfigError> {
        if self.format_version != RESOLVER_CONFIG_FORMAT_VERSION {
            return Err(ResolverConfigError::UnsupportedFormatVersion(
                self.format_version,
            ));
        }
        if self.algorithm.id != NATIVE_COMPONENT_COVER_ALGORITHM_ID
            || self.algorithm.version != NATIVE_COMPONENT_COVER_ALGORITHM_VERSION
        {
            return Err(ResolverConfigError::UnsupportedAlgorithm {
                id: self.algorithm.id.clone(),
                version: self.algorithm.version,
            });
        }
        if self.policy_version.trim().is_empty() {
            return Err(ResolverConfigError::EmptyPolicyVersion);
        }
        if self.permissions.create_canonical_schema {
            return Err(ResolverConfigError::CanonicalCreationForbidden);
        }

        let unit = |name: &str, v: f32| -> Result<(), ResolverConfigError> {
            if !v.is_finite() {
                return Err(ResolverConfigError::NonFinite(name.to_string()));
            }
            if !(0.0..=1.0).contains(&v) {
                return Err(ResolverConfigError::OutOfRange(name.to_string()));
            }
            Ok(())
        };

        unit(
            "descriptive_name_min_similarity",
            self.scoring.schema.descriptive_name_min_similarity,
        )?;
        unit(
            "use_existing_min_score",
            self.scoring.schema.use_existing_min_score,
        )?;
        unit(
            "schema.ambiguity_margin",
            self.scoring.schema.ambiguity_margin,
        )?;
        unit(
            "context_min_similarity",
            self.scoring.field.context_min_similarity,
        )?;
        unit(
            "canonical_min_similarity",
            self.scoring.field.canonical_min_similarity,
        )?;
        unit(
            "field.ambiguity_margin",
            self.scoring.field.ambiguity_margin,
        )?;
        unit("coverage.min_fields", self.scoring.coverage.min_fields)?;
        unit(
            "coverage.min_required_fields",
            self.scoring.coverage.min_required_fields,
        )?;
        unit("components.min_score", self.scoring.components.min_score)?;
        unit(
            "weights.descriptive_name",
            self.scoring.schema.weights.descriptive_name,
        )?;
        unit(
            "weights.field_match",
            self.scoring.schema.weights.field_match,
        )?;

        let weight_sum =
            self.scoring.schema.weights.descriptive_name + self.scoring.schema.weights.field_match;
        if (weight_sum - 1.0).abs() > WEIGHT_EPS {
            return Err(ResolverConfigError::WeightsDoNotSumToOne);
        }

        let lim = |name: &str, v: u32, hard: u32| -> Result<(), ResolverConfigError> {
            if v == 0 || v > hard {
                return Err(ResolverConfigError::ExceedsHardLimit(name.to_string()));
            }
            Ok(())
        };
        lim(
            "schema_candidates",
            self.candidate_generation.schema_candidates,
            HARD_MAX_SCHEMA_CANDIDATES,
        )?;
        lim(
            "field_candidates_per_schema",
            self.candidate_generation.field_candidates_per_schema,
            HARD_MAX_FIELD_CANDIDATES,
        )?;
        lim(
            "component_candidates",
            self.candidate_generation.component_candidates,
            HARD_MAX_COMPONENT_CANDIDATES,
        )?;
        lim(
            "max_components_per_resolution",
            self.candidate_generation.max_components_per_resolution,
            HARD_MAX_COMPONENTS_PER_RESOLUTION,
        )?;
        lim(
            "beam_width",
            self.candidate_generation.beam_width,
            HARD_MAX_BEAM_WIDTH,
        )?;
        lim(
            "max_proposal_fields",
            self.limits.max_proposal_fields,
            HARD_MAX_PROPOSAL_FIELDS,
        )?;
        lim(
            "max_registry_schemas",
            self.limits.max_registry_schemas,
            HARD_MAX_REGISTRY_SCHEMAS,
        )?;
        lim(
            "max_embedding_dimensions",
            self.limits.max_embedding_dimensions,
            HARD_MAX_EMBEDDING_DIMENSIONS,
        )?;
        if self.limits.max_work_units == 0 || self.limits.max_work_units > HARD_MAX_WORK_UNITS {
            return Err(ResolverConfigError::ExceedsHardLimit(
                "max_work_units".to_string(),
            ));
        }
        Ok(())
    }
}
