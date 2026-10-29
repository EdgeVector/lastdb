use super::*;

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

    pub(super) fn charge(&mut self, n: u64) -> Result<(), NativeResolverError> {
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
            return Ok(self.empty_proposal_fallback());
        }

        let candidates = self.shortlist_candidates()?;
        if candidates.is_empty() {
            return Ok(self.no_candidate_fallback());
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
            return Ok(use_existing_output(
                &candidates,
                evidence_scores,
                coverage,
                proposal,
            ));
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

    pub(super) fn fallback(
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
