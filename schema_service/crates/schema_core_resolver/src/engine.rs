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
    ComponentResolution, FieldCoverageEvidence, ProposalEmbeddings, ProposalMetadata,
    RegistryEmbeddings, RegistryFieldHandle, RegistryMetadata, RegistrySchemaHandle,
    ResidueFieldEvidence, ResolverDecision, ResolverEvidence, ResolverOutput, SchemaScoreEvidence,
    UseExistingResolution,
};
use crate::config::{
    ResolverConfig, ResolverConfigError, NATIVE_COMPONENT_COVER_ALGORITHM_ID,
    NATIVE_COMPONENT_COVER_ALGORITHM_VERSION,
};
use crate::embedding::cosine_similarity;

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

/// Algorithm registry entry point.
pub fn evaluate_native(input: &NativeResolverInput) -> Result<ResolverOutput, NativeResolverError> {
    input
        .config
        .validate()
        .map_err(NativeResolverError::Config)?;
    if input.config.algorithm.id != NATIVE_COMPONENT_COVER_ALGORITHM_ID
        || input.config.algorithm.version != NATIVE_COMPONENT_COVER_ALGORITHM_VERSION
    {
        return Err(NativeResolverError::Config(
            ResolverConfigError::UnsupportedAlgorithm {
                id: input.config.algorithm.id.clone(),
                version: input.config.algorithm.version,
            },
        ));
    }
    if input.resolver_contract_version != input.config.resolver_contract_version {
        return Err(NativeResolverError::UnsupportedContractVersion(
            input.resolver_contract_version,
        ));
    }
    NativeComponentCover::new(input)?.evaluate()
}

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

impl<'a> NativeComponentCover<'a> {
    fn new(input: &'a NativeResolverInput) -> Result<Self, NativeResolverError> {
        let cfg = &input.config;
        let proposal = &input.proposal_metadata;
        if proposal.fields.len() as u32 > cfg.limits.max_proposal_fields {
            return Err(NativeResolverError::OversizedProposalFields);
        }
        if input.registry_metadata.schemas.len() as u32 > cfg.limits.max_registry_schemas {
            return Err(NativeResolverError::OversizedRegistry);
        }

        let mut name_emb = HashMap::new();
        for rec in &input.registry_embeddings.descriptive_names {
            validate_vector(&rec.vector, cfg.limits.max_embedding_dimensions)?;
            if name_emb
                .insert(rec.target_id.clone(), rec.vector.as_slice())
                .is_some()
            {
                return Err(NativeResolverError::DuplicateId(rec.target_id.clone()));
            }
        }
        let mut field_emb = HashMap::new();
        for rec in &input.registry_embeddings.schema_field_contexts {
            validate_vector(&rec.vector, cfg.limits.max_embedding_dimensions)?;
            if field_emb
                .insert(rec.target_id.clone(), rec.vector.as_slice())
                .is_some()
            {
                return Err(NativeResolverError::DuplicateId(rec.target_id.clone()));
            }
        }
        validate_vector(
            &input.proposal_embeddings.descriptive_name,
            cfg.limits.max_embedding_dimensions,
        )?;
        for rec in &input.proposal_embeddings.field_contexts {
            validate_vector(&rec.vector, cfg.limits.max_embedding_dimensions)?;
        }

        let dim = input.proposal_embeddings.descriptive_name.len();
        if dim == 0 {
            return Err(NativeResolverError::DimensionMismatch);
        }
        for v in name_emb.values().chain(field_emb.values()) {
            if v.len() != dim {
                return Err(NativeResolverError::DimensionMismatch);
            }
        }
        for rec in &input.proposal_embeddings.field_contexts {
            if rec.vector.len() != dim {
                return Err(NativeResolverError::DimensionMismatch);
            }
        }

        let mut seen_schema = HashSet::new();
        for s in &input.registry_metadata.schemas {
            if !seen_schema.insert(s.schema_id.clone()) {
                return Err(NativeResolverError::DuplicateId(s.schema_id.clone()));
            }
        }
        let mut fields_by_schema: HashMap<String, Vec<&RegistryFieldHandle>> = HashMap::new();
        let mut seen_field = HashSet::new();
        for f in &input.registry_metadata.fields {
            if !seen_field.insert(f.field_id.clone()) {
                return Err(NativeResolverError::DuplicateId(f.field_id.clone()));
            }
            fields_by_schema
                .entry(f.schema_id.clone())
                .or_default()
                .push(f);
        }
        // Stable field order for determinism.
        for fields in fields_by_schema.values_mut() {
            fields.sort_by(|a, b| a.field_id.cmp(&b.field_id));
        }

        Ok(Self {
            input,
            work_units: 0,
            name_emb,
            field_emb,
            fields_by_schema,
        })
    }

    fn charge(&mut self, n: u64) -> Result<(), NativeResolverError> {
        self.work_units = self.work_units.saturating_add(n);
        if self.work_units > self.input.config.limits.max_work_units {
            return Err(NativeResolverError::WorkLimitExceeded);
        }
        Ok(())
    }

    fn evaluate(mut self) -> Result<ResolverOutput, NativeResolverError> {
        let cfg = &self.input.config;
        let proposal = &self.input.proposal_metadata;
        let total_fields = proposal.fields.len() as u32;
        let required_total = proposal.fields.iter().filter(|f| f.required).count() as u32;

        if proposal.fields.is_empty() {
            return Ok(self.fallback(
                ResolverDecision::NeedsLiveSchemaService,
                0.0,
                FieldCoverageEvidence {
                    covered_fields: 0,
                    total_fields: 0,
                    required_fields_covered: 0,
                    required_fields_total: 0,
                },
                vec![],
                vec![],
                vec!["empty_proposal".into()],
            ));
        }

        let proposal_name_emb = self.input.proposal_embeddings.descriptive_name.as_slice();
        let proposal_field_emb: HashMap<&str, &[f32]> = self
            .input
            .proposal_embeddings
            .field_contexts
            .iter()
            .map(|r| (r.target_id.as_str(), r.vector.as_slice()))
            .collect();

        // Guardrail from the eval: field embeddings alone are insufficient.
        // We always require descriptive-name shortlist first.
        let mut candidates: Vec<SchemaCandidate> = Vec::new();
        let mut schema_list: Vec<&RegistrySchemaHandle> =
            self.input.registry_metadata.schemas.iter().collect();
        schema_list.sort_by(|a, b| a.schema_id.cmp(&b.schema_id));

        for schema in schema_list {
            if !cfg.candidate_generation.allow_deprecated_schemas
                && schema.lifecycle.eq_ignore_ascii_case("deprecated")
            {
                continue;
            }
            self.charge(1)?;
            let Some(schema_name_vec) = self.name_emb.get(&schema.schema_id).copied() else {
                // Missing name embedding → skip candidate (force live path later if none).
                continue;
            };
            let name_sim = cosine_similarity(proposal_name_emb, schema_name_vec);
            if !name_sim.is_finite() {
                return Err(NativeResolverError::NonFiniteEmbedding);
            }
            if name_sim < cfg.scoring.schema.descriptive_name_min_similarity {
                continue;
            }

            // Clone handles so we can charge work units without borrow conflicts.
            let schema_fields: Vec<&RegistryFieldHandle> = self
                .fields_by_schema
                .get(&schema.schema_id)
                .cloned()
                .unwrap_or_default();
            let mut covered = Vec::new();
            for pf in &proposal.fields {
                self.charge(1)?;
                let Some(p_emb) = proposal_field_emb.get(pf.proposal_field_id.as_str()) else {
                    return Err(NativeResolverError::MissingEmbedding(
                        pf.proposal_field_id.clone(),
                    ));
                };
                let mut best: Option<FieldMatch> = None;
                let mut second = 0.0f32;
                for sf in &schema_fields {
                    if cfg.candidate_generation.require_compatible_field_types
                        && !field_types_compatible(&pf.field_type, &sf.field_type)
                    {
                        continue;
                    }
                    self.charge(1)?;
                    let Some(s_emb) = self.field_emb.get(&sf.field_id).copied() else {
                        continue;
                    };
                    let score = cosine_similarity(p_emb, s_emb);
                    if !score.is_finite() {
                        return Err(NativeResolverError::NonFiniteEmbedding);
                    }
                    match &best {
                        None => {
                            best = Some(FieldMatch {
                                proposal_field_id: pf.proposal_field_id.clone(),
                                score,
                            });
                        }
                        Some(b) if score > b.score => {
                            second = b.score;
                            best = Some(FieldMatch {
                                proposal_field_id: pf.proposal_field_id.clone(),
                                score,
                            });
                        }
                        Some(b) if score > second && score <= b.score => second = score,
                        _ => {}
                    }
                }
                if let Some(b) = best {
                    let margin_ok =
                        b.score - second >= cfg.scoring.field.ambiguity_margin || second == 0.0;
                    if b.score >= cfg.scoring.field.context_min_similarity && margin_ok {
                        covered.push(b);
                    }
                }
            }

            let field_match_score = if total_fields == 0 {
                0.0
            } else {
                covered.iter().map(|m| m.score).sum::<f32>() / total_fields as f32
            };
            let score = cfg.scoring.schema.weights.descriptive_name * name_sim
                + cfg.scoring.schema.weights.field_match * field_match_score;
            candidates.push(SchemaCandidate {
                schema_id: schema.schema_id.clone(),
                score,
                covered,
            });
        }

        // Deterministic sort: score desc, then schema_id asc.
        candidates.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.schema_id.cmp(&b.schema_id))
        });
        let shortlist_n = cfg.candidate_generation.schema_candidates as usize;
        if candidates.len() > shortlist_n {
            candidates.truncate(shortlist_n);
        }

        if candidates.is_empty() {
            return Ok(self.fallback(
                ResolverDecision::NeedsLiveSchemaService,
                0.0,
                FieldCoverageEvidence {
                    covered_fields: 0,
                    total_fields,
                    required_fields_covered: 0,
                    required_fields_total: required_total,
                },
                vec![],
                proposal
                    .fields
                    .iter()
                    .map(|f| ResidueFieldEvidence {
                        proposal_field_id: f.proposal_field_id.clone(),
                        reason: "no_candidate_schema".into(),
                    })
                    .collect(),
                vec!["no_candidate_schema".into()],
            ));
        }

        let evidence_scores: Vec<SchemaScoreEvidence> = candidates
            .iter()
            .take(8)
            .map(|c| SchemaScoreEvidence {
                schema_id: c.schema_id.clone(),
                score: c.score,
            })
            .collect();

        // Ambiguity among top single-schema candidates.
        if candidates.len() >= 2 {
            let margin = candidates[0].score - candidates[1].score;
            if margin < cfg.scoring.schema.ambiguity_margin
                && candidates[0].score >= cfg.scoring.schema.use_existing_min_score * 0.9
            {
                return Ok(self.fallback(
                    ResolverDecision::Ambiguous,
                    candidates[0].score,
                    coverage_from_matches(&candidates[0].covered, proposal),
                    evidence_scores,
                    residue_from_matches(&candidates[0].covered, proposal),
                    vec!["ambiguous_schema_scores".into()],
                ));
            }
        }

        let best = &candidates[0];
        let coverage = coverage_from_matches(&best.covered, proposal);
        let field_cov = if total_fields == 0 {
            0.0
        } else {
            coverage.covered_fields as f32 / total_fields as f32
        };
        let req_cov = if required_total == 0 {
            1.0
        } else {
            coverage.required_fields_covered as f32 / required_total as f32
        };

        // Single-schema reuse (high confidence).
        if cfg.permissions.use_existing
            && best.score >= cfg.scoring.schema.use_existing_min_score
            && field_cov >= cfg.scoring.coverage.min_fields
            && req_cov >= cfg.scoring.coverage.min_required_fields
        {
            return Ok(ResolverOutput {
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
            });
        }

        // Bounded beam component cover (embedding-beam).
        if cfg.permissions.use_components {
            if let Some(output) =
                self.try_component_cover(&candidates, &evidence_scores, proposal)?
            {
                return Ok(output);
            }
        }

        Ok(self.fallback(
            ResolverDecision::NeedsLiveSchemaService,
            best.score,
            coverage,
            evidence_scores,
            residue_from_matches(&best.covered, proposal),
            vec![
                "insufficient_local_confidence".into(),
                format!("best_score={:.4}", best.score),
            ],
        ))
    }

    fn try_component_cover(
        &mut self,
        candidates: &[SchemaCandidate],
        evidence_scores: &[SchemaScoreEvidence],
        proposal: &ProposalMetadata,
    ) -> Result<Option<ResolverOutput>, NativeResolverError> {
        let cfg = &self.input.config;
        let total_fields = proposal.fields.len();
        if total_fields == 0 {
            return Ok(None);
        }

        let component_pool: Vec<&SchemaCandidate> = candidates
            .iter()
            .filter(|c| c.score >= cfg.scoring.components.min_score && !c.covered.is_empty())
            .take(cfg.candidate_generation.component_candidates as usize)
            .collect();
        if component_pool.is_empty() {
            return Ok(None);
        }

        #[derive(Clone)]
        struct BeamState {
            chosen: Vec<SchemaCandidate>,
            covered: HashSet<String>,
            score: f32,
        }

        let mut beam = vec![BeamState {
            chosen: vec![],
            covered: HashSet::new(),
            score: 0.0,
        }];
        let max_depth = cfg.candidate_generation.max_components_per_resolution as usize;
        let beam_width = cfg.candidate_generation.beam_width as usize;

        for _depth in 0..max_depth {
            let mut next = beam.clone();
            for state in &beam {
                for c in &component_pool {
                    self.charge(1)?;
                    // Skip if already chose this schema id.
                    if state.chosen.iter().any(|x| x.schema_id == c.schema_id) {
                        continue;
                    }
                    let newly: Vec<FieldMatch> = c
                        .covered
                        .iter()
                        .filter(|m| !state.covered.contains(&m.proposal_field_id))
                        .cloned()
                        .collect();
                    if newly.is_empty() {
                        continue;
                    }
                    let mut covered = state.covered.clone();
                    for m in &newly {
                        covered.insert(m.proposal_field_id.clone());
                    }
                    let mut chosen = state.chosen.clone();
                    let mut piece = (*c).clone();
                    piece.covered = newly;
                    chosen.push(piece);
                    // embedding-beam score: coverage + 0.18 * avg candidate score - 0.05 * depth
                    let avg = chosen.iter().map(|x| x.score).sum::<f32>() / chosen.len() as f32;
                    let score = covered.len() as f32 / total_fields as f32 + 0.18 * avg
                        - 0.05 * (chosen.len().saturating_sub(1) as f32);
                    next.push(BeamState {
                        chosen,
                        covered,
                        score,
                    });
                }
            }
            next.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| {
                        // Prefer fewer components, then stable schema id chain.
                        a.chosen.len().cmp(&b.chosen.len()).then_with(|| {
                            let a_ids: String =
                                a.chosen.iter().map(|c| c.schema_id.as_str()).collect();
                            let b_ids: String =
                                b.chosen.iter().map(|c| c.schema_id.as_str()).collect();
                            a_ids.cmp(&b_ids)
                        })
                    })
            });
            if next.len() > beam_width {
                next.truncate(beam_width);
            }
            beam = next;
        }

        let Some(best) = beam
            .into_iter()
            .filter(|s| !s.chosen.is_empty())
            .max_by(|a, b| {
                a.score
                    .partial_cmp(&b.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
        else {
            return Ok(None);
        };

        let field_cov = best.covered.len() as f32 / total_fields as f32;
        if field_cov < cfg.scoring.coverage.min_fields {
            return Ok(None);
        }

        // Required-field coverage across union.
        let required_ids: HashSet<&str> = proposal
            .fields
            .iter()
            .filter(|f| f.required)
            .map(|f| f.proposal_field_id.as_str())
            .collect();
        let required_total = required_ids.len() as u32;
        let required_covered = required_ids
            .iter()
            .filter(|id| best.covered.contains(**id))
            .count() as u32;
        let req_cov = if required_total == 0 {
            1.0
        } else {
            required_covered as f32 / required_total as f32
        };
        if req_cov < cfg.scoring.coverage.min_required_fields {
            return Ok(None);
        }

        let mut use_components = Vec::new();
        for c in &best.chosen {
            for m in &c.covered {
                use_components.push(ComponentResolution {
                    proposal_field_id: m.proposal_field_id.clone(),
                    schema_id: c.schema_id.clone(),
                    confidence: m.score,
                });
            }
        }
        // Stable order by proposal_field_id.
        use_components.sort_by(|a, b| a.proposal_field_id.cmp(&b.proposal_field_id));

        let residue: Vec<ResidueFieldEvidence> = proposal
            .fields
            .iter()
            .filter(|f| !best.covered.contains(&f.proposal_field_id))
            .map(|f| ResidueFieldEvidence {
                proposal_field_id: f.proposal_field_id.clone(),
                reason: "uncovered_by_components".into(),
            })
            .collect();

        // If there is residue, prefer the live service rather than resolving
        // wholly locally. Keep the field-to-component mappings in the output
        // so the live registration path can mint only the uncovered residual
        // instead of registering the original mega-schema.
        if !residue.is_empty() {
            let mut output = self.fallback(
                ResolverDecision::NeedsLiveSchemaService,
                best.score,
                FieldCoverageEvidence {
                    covered_fields: best.covered.len() as u32,
                    total_fields: total_fields as u32,
                    required_fields_covered: required_covered,
                    required_fields_total: required_total,
                },
                evidence_scores.to_vec(),
                residue,
                vec!["component_cover_residue".into()],
            );
            output.use_components = use_components;
            return Ok(Some(output));
        }

        Ok(Some(ResolverOutput {
            abi_version: 1,
            decision: ResolverDecision::UseComponents,
            confidence: best.score.clamp(0.0, 1.0),
            evidence: ResolverEvidence {
                schema_scores: evidence_scores.to_vec(),
                field_coverage: FieldCoverageEvidence {
                    covered_fields: best.covered.len() as u32,
                    total_fields: total_fields as u32,
                    required_fields_covered: required_covered,
                    required_fields_total: required_total,
                },
                ambiguity_margin: None,
                residue_fields: vec![],
            },
            use_existing: None,
            use_components,
            expand_existing_if_allowed: None,
            fallback_reasons: vec![],
        }))
    }

    fn fallback(
        &self,
        decision: ResolverDecision,
        confidence: f32,
        field_coverage: FieldCoverageEvidence,
        schema_scores: Vec<SchemaScoreEvidence>,
        residue_fields: Vec<ResidueFieldEvidence>,
        fallback_reasons: Vec<String>,
    ) -> ResolverOutput {
        ResolverOutput {
            abi_version: 1,
            decision,
            confidence,
            evidence: ResolverEvidence {
                schema_scores,
                field_coverage,
                ambiguity_margin: None,
                residue_fields,
            },
            use_existing: None,
            use_components: vec![],
            expand_existing_if_allowed: None,
            fallback_reasons,
        }
    }
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
