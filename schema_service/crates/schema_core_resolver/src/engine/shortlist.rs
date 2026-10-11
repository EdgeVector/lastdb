//! Schema shortlist: descriptive-name gate, then per-field best-match cover.

use std::collections::HashMap;

use super::{
    cosine_similarity, field_types_compatible, FieldMatch, NativeComponentCover,
    NativeResolverError, SchemaCandidate,
};
use crate::abi::{
    FieldCoverageEvidence, ProposalFieldMetadata, RegistryFieldHandle, RegistrySchemaHandle,
    ResidueFieldEvidence, ResolverDecision, ResolverOutput,
};

impl<'a> NativeComponentCover<'a> {
    /// Score every registry schema against the proposal and return the
    /// deterministic, truncated shortlist.
    pub(super) fn shortlist_candidates(
        &mut self,
    ) -> Result<Vec<SchemaCandidate>, NativeResolverError> {
        let cfg = &self.input.config;
        let total_fields = self.input.proposal_metadata.fields.len() as u32;
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
            let covered = self.cover_proposal_fields(&schema_fields, &proposal_field_emb)?;

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
        candidates.truncate(cfg.candidate_generation.schema_candidates as usize);
        Ok(candidates)
    }

    /// Best unambiguous registry-field match for each proposal field.
    fn cover_proposal_fields(
        &mut self,
        schema_fields: &[&RegistryFieldHandle],
        proposal_field_emb: &HashMap<&str, &[f32]>,
    ) -> Result<Vec<FieldMatch>, NativeResolverError> {
        let cfg = &self.input.config;
        let mut covered = Vec::new();
        for pf in &self.input.proposal_metadata.fields {
            self.charge(1)?;
            let Some(p_emb) = proposal_field_emb.get(pf.proposal_field_id.as_str()) else {
                return Err(NativeResolverError::MissingEmbedding(
                    pf.proposal_field_id.clone(),
                ));
            };
            let (best, second) = self.best_field_match(pf, p_emb, schema_fields)?;
            if let Some(b) = best {
                let margin_ok =
                    b.score - second >= cfg.scoring.field.ambiguity_margin || second == 0.0;
                if b.score >= cfg.scoring.field.context_min_similarity && margin_ok {
                    covered.push(b);
                }
            }
        }
        Ok(covered)
    }

    /// Top match and runner-up score for one proposal field.
    fn best_field_match(
        &mut self,
        pf: &ProposalFieldMetadata,
        p_emb: &[f32],
        schema_fields: &[&RegistryFieldHandle],
    ) -> Result<(Option<FieldMatch>, f32), NativeResolverError> {
        let cfg = &self.input.config;
        let mut best: Option<FieldMatch> = None;
        let mut second = 0.0f32;
        for sf in schema_fields {
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
        Ok((best, second))
    }
}

impl<'a> NativeComponentCover<'a> {
    pub(super) fn empty_proposal_fallback(&self) -> ResolverOutput {
        self.fallback(
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
        )
    }

    pub(super) fn no_candidate_fallback(&self) -> ResolverOutput {
        let proposal = &self.input.proposal_metadata;
        self.fallback(
            ResolverDecision::NeedsLiveSchemaService,
            0.0,
            FieldCoverageEvidence {
                covered_fields: 0,
                total_fields: proposal.fields.len() as u32,
                required_fields_covered: 0,
                required_fields_total: proposal.fields.iter().filter(|f| f.required).count() as u32,
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
        )
    }
}
