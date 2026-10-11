//! Bounded beam component cover (embedding-beam).

use std::collections::HashSet;

use super::{FieldMatch, NativeComponentCover, NativeResolverError, SchemaCandidate};
use crate::abi::{
    ComponentResolution, FieldCoverageEvidence, ProposalMetadata, ResidueFieldEvidence,
    ResolverDecision, ResolverEvidence, ResolverOutput, SchemaScoreEvidence,
};

#[derive(Clone)]
struct BeamState {
    chosen: Vec<SchemaCandidate>,
    covered: HashSet<String>,
    score: f32,
}

impl<'a> NativeComponentCover<'a> {
    pub(super) fn try_component_cover(
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

        let beam = self.run_beam(&component_pool, total_fields)?;
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
        Ok(self.component_output(&best, evidence_scores, proposal))
    }

    fn run_beam(
        &mut self,
        component_pool: &[&SchemaCandidate],
        total_fields: usize,
    ) -> Result<Vec<BeamState>, NativeResolverError> {
        let cfg = &self.input.config;
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
                for c in component_pool {
                    self.charge(1)?;
                    if let Some(extended) = extend_state(state, c, total_fields) {
                        next.push(extended);
                    }
                }
            }
            sort_beam(&mut next);
            next.truncate(beam_width);
            beam = next;
        }
        Ok(beam)
    }

    /// Turn the best beam state into a resolution, or `None` when coverage
    /// is below the configured minimums.
    fn component_output(
        &self,
        best: &BeamState,
        evidence_scores: &[SchemaScoreEvidence],
        proposal: &ProposalMetadata,
    ) -> Option<ResolverOutput> {
        let cfg = &self.input.config;
        let total_fields = proposal.fields.len();
        let field_cov = best.covered.len() as f32 / total_fields as f32;
        if field_cov < cfg.scoring.coverage.min_fields {
            return None;
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
            return None;
        }

        let use_components = use_components_of(best);
        let residue: Vec<ResidueFieldEvidence> = proposal
            .fields
            .iter()
            .filter(|f| !best.covered.contains(&f.proposal_field_id))
            .map(|f| ResidueFieldEvidence {
                proposal_field_id: f.proposal_field_id.clone(),
                reason: "uncovered_by_components".into(),
            })
            .collect();
        let field_coverage = FieldCoverageEvidence {
            covered_fields: best.covered.len() as u32,
            total_fields: total_fields as u32,
            required_fields_covered: required_covered,
            required_fields_total: required_total,
        };

        // If there is residue, prefer the live service rather than resolving
        // wholly locally. Keep the field-to-component mappings in the output
        // so the live registration path can mint only the uncovered residual
        // instead of registering the original mega-schema.
        if !residue.is_empty() {
            let mut output = self.fallback(
                ResolverDecision::NeedsLiveSchemaService,
                best.score,
                field_coverage,
                evidence_scores.to_vec(),
                residue,
                vec!["component_cover_residue".into()],
            );
            output.use_components = use_components;
            return Some(output);
        }

        Some(ResolverOutput {
            abi_version: 1,
            decision: ResolverDecision::UseComponents,
            confidence: best.score.clamp(0.0, 1.0),
            evidence: ResolverEvidence {
                schema_scores: evidence_scores.to_vec(),
                field_coverage,
                ambiguity_margin: None,
                residue_fields: vec![],
            },
            use_existing: None,
            use_components,
            expand_existing_if_allowed: None,
            fallback_reasons: vec![],
        })
    }
}

/// Add candidate `c` to `state`, if it covers at least one new field.
fn extend_state(state: &BeamState, c: &SchemaCandidate, total_fields: usize) -> Option<BeamState> {
    // Skip if already chose this schema id.
    if state.chosen.iter().any(|x| x.schema_id == c.schema_id) {
        return None;
    }
    let newly: Vec<FieldMatch> = c
        .covered
        .iter()
        .filter(|m| !state.covered.contains(&m.proposal_field_id))
        .cloned()
        .collect();
    if newly.is_empty() {
        return None;
    }
    let mut covered = state.covered.clone();
    for m in &newly {
        covered.insert(m.proposal_field_id.clone());
    }
    let mut chosen = state.chosen.clone();
    let mut piece = c.clone();
    piece.covered = newly;
    chosen.push(piece);
    // embedding-beam score: coverage + 0.18 * avg candidate score - 0.05 * depth
    let avg = chosen.iter().map(|x| x.score).sum::<f32>() / chosen.len() as f32;
    let score = covered.len() as f32 / total_fields as f32 + 0.18 * avg
        - 0.05 * (chosen.len().saturating_sub(1) as f32);
    Some(BeamState {
        chosen,
        covered,
        score,
    })
}

fn sort_beam(next: &mut [BeamState]) {
    next.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                // Prefer fewer components, then stable schema id chain.
                a.chosen.len().cmp(&b.chosen.len()).then_with(|| {
                    let a_ids: String = a.chosen.iter().map(|c| c.schema_id.as_str()).collect();
                    let b_ids: String = b.chosen.iter().map(|c| c.schema_id.as_str()).collect();
                    a_ids.cmp(&b_ids)
                })
            })
    });
}

fn use_components_of(best: &BeamState) -> Vec<ComponentResolution> {
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
    use_components
}
