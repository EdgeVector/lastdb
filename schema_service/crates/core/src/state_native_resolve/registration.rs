//! Registration-time component cover.

use super::*;

impl SchemaServiceState {
    /// Evaluate registration-time component cover for an incoming add-schema
    /// proposal. Returns `None` when native cover is disabled/unavailable or
    /// when the proposal does not meet the configured coverage threshold.
    ///
    /// Unlike `POST /v1/schemas/resolve`, this helper surfaces partial-cover
    /// residue so the mutation path can register only the uncovered fields and
    /// avoid minting a fat whole-schema identity.
    // lint:fn-size-ok verbatim move from state_native_resolve.rs; splitting this function is separate work
    pub fn try_native_component_cover_registration(
        &self,
        schema: &Schema,
    ) -> FoldDbResult<Option<SchemaRegistrationComposition>> {
        if !native_component_cover_resolve_enabled() {
            return Ok(None);
        }

        let Some(fields) = schema.fields.as_ref().filter(|fields| !fields.is_empty()) else {
            return Ok(None);
        };
        if let Some(template_cover) = self.try_starter_template_cover_registration(schema)? {
            return Ok(Some(template_cover));
        }
        let proposal = SchemaResolveProposal {
            descriptive_name: schema
                .descriptive_name
                .clone()
                .unwrap_or_else(|| schema.name.clone()),
            fields: fields.clone(),
            field_descriptions: schema.field_descriptions.clone(),
            purpose_statement: schema.purpose_statement.clone(),
            identity_hash: schema.identity_hash.clone(),
            owner_app_id: schema.owner_app_id.clone(),
        };

        let mut built = match self.build_native_resolver_input(&proposal) {
            Ok(b) => b,
            Err(e) => {
                tracing::debug!(
                    target: "schema_service::schema",
                    descriptive_name = %proposal.descriptive_name,
                    error = %e,
                    "native_component_cover_registration: input build failed"
                );
                return Ok(None);
            }
        };
        built.input.config.scoring.coverage.min_fields = built
            .input
            .config
            .scoring
            .coverage
            .min_fields
            .min(REGISTRATION_COMPONENT_COVER_MIN_FIELDS);
        built.input.config.scoring.coverage.min_required_fields = built
            .input
            .config
            .scoring
            .coverage
            .min_required_fields
            .min(REGISTRATION_COMPONENT_COVER_MIN_FIELDS);
        let output = match evaluate_native(&built.input) {
            Ok(output) => output,
            Err(e) => {
                tracing::debug!(
                    target: "schema_service::schema",
                    descriptive_name = %proposal.descriptive_name,
                    error = %e,
                    "native_component_cover_registration: evaluate failed"
                );
                return Ok(None);
            }
        };

        let field_by_id = proposal
            .fields
            .iter()
            .enumerate()
            .map(|(i, field)| (proposal_field_id(i, field), field.clone()))
            .collect::<HashMap<_, _>>();

        match output.decision {
            ResolverDecision::UseExisting => {
                let Some(use_existing) = output.use_existing else {
                    return Ok(None);
                };
                Ok(Some(SchemaRegistrationComposition {
                    matched_shared_schema_hash: Some(use_existing.schema_id),
                    covered_components: Vec::new(),
                    residue_fields: Vec::new(),
                    field_coverage: output.evidence.field_coverage,
                    confidence: Some(output.confidence),
                }))
            }
            ResolverDecision::UseComponents | ResolverDecision::NeedsLiveSchemaService => {
                if output.use_components.is_empty() {
                    return Ok(None);
                }
                let residue_fields = output
                    .evidence
                    .residue_fields
                    .iter()
                    .filter_map(|r| field_by_id.get(&r.proposal_field_id).cloned())
                    .collect::<Vec<_>>();
                let covered_components =
                    registration_components_from_resolutions(&output.use_components, &field_by_id);
                Ok(Some(SchemaRegistrationComposition {
                    matched_shared_schema_hash: None,
                    covered_components,
                    residue_fields,
                    field_coverage: output.evidence.field_coverage,
                    confidence: Some(output.confidence),
                }))
            }
            ResolverDecision::ExpandExistingIfAllowed
            | ResolverDecision::Ambiguous
            | ResolverDecision::Reject => Ok(None),
        }
    }

    pub(super) fn try_starter_template_cover_registration(
        &self,
        schema: &Schema,
    ) -> FoldDbResult<Option<SchemaRegistrationComposition>> {
        let Some(proposal_fields) = schema.fields.as_ref().filter(|fields| !fields.is_empty())
        else {
            return Ok(None);
        };

        let schemas = read_lock(&self.schemas, "schemas_cache")?;
        let mut best: Option<TemplateCoverCandidate> = None;

        for (identity, template) in schemas.iter() {
            if template.superseded_by.is_some() || !is_app_starter_template(template) {
                continue;
            }
            let template_fields = template.fields.clone().unwrap_or_default();
            if template_fields.is_empty() {
                continue;
            }

            let mut covered = Vec::new();
            for field in proposal_fields {
                if template_fields
                    .iter()
                    .any(|template_field| template_field == field)
                {
                    covered.push(field.clone());
                }
            }

            let coverage = covered.len() as f32 / proposal_fields.len() as f32;
            if coverage < STARTER_TEMPLATE_COVER_MIN_FIELDS {
                continue;
            }

            let schema_id = template
                .identity_hash
                .clone()
                .unwrap_or_else(|| identity.clone());
            let candidate = TemplateCoverCandidate {
                schema_id,
                covered,
                coverage,
                template_field_count: template_fields.len(),
            };
            if best
                .as_ref()
                .is_none_or(|current| candidate.better_than(current))
            {
                best = Some(candidate);
            }
        }

        let Some(best) = best else {
            return Ok(None);
        };

        let covered_set: HashSet<String> = best.covered.iter().cloned().collect();
        let residue_fields = proposal_fields
            .iter()
            .filter(|field| !covered_set.contains(*field))
            .cloned()
            .collect::<Vec<_>>();
        let covered_count = covered_set.len();
        let covered_components = best
            .covered
            .into_iter()
            .map(|field| SchemaRegistrationComponent {
                field,
                shared_schema_hash: best.schema_id.clone(),
                confidence: best.coverage,
            })
            .collect::<Vec<_>>();

        tracing::info!(
            target: "schema_service::schema",
            template_schema = %best.schema_id,
            field_coverage = best.coverage,
            residue_fields = residue_fields.len(),
            "starter_template_match: covered proposal through templates owner namespace"
        );

        Ok(Some(SchemaRegistrationComposition {
            matched_shared_schema_hash: None,
            covered_components,
            residue_fields,
            field_coverage: FieldCoverageEvidence {
                covered_fields: covered_count as u32,
                total_fields: proposal_fields.len() as u32,
                required_fields_covered: covered_count as u32,
                required_fields_total: proposal_fields.len() as u32,
            },
            confidence: Some(best.coverage),
        }))
    }

    /// Build the residual schema that should be minted for a partial component
    /// cover. Returns `None` for fully-covered proposals.
    pub fn residual_schema_for_registration(
        &self,
        schema: &Schema,
        composition: &SchemaRegistrationComposition,
    ) -> Option<Schema> {
        if composition.residue_fields.is_empty() {
            return None;
        }
        let residue: HashSet<&str> = composition
            .residue_fields
            .iter()
            .map(String::as_str)
            .collect();
        let mut residual = schema.clone();
        let fields = schema
            .fields
            .as_ref()?
            .iter()
            .filter(|field| residue.contains(field.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if fields.is_empty() {
            return None;
        }
        residual.fields = Some(fields);
        residual
            .field_descriptions
            .retain(|field, _| residue.contains(field.as_str()));
        residual
            .field_types
            .retain(|field, _| residue.contains(field.as_str()));
        residual
            .ref_fields
            .retain(|field, _| residue.contains(field.as_str()));
        residual.identity_hash = None;
        let base_name = schema
            .descriptive_name
            .as_deref()
            .filter(|name| !name.trim().is_empty())
            .unwrap_or(schema.name.as_str());
        residual.descriptive_name = Some(format!("{base_name} Residual"));
        residual.purpose_statement = schema.purpose_statement.as_ref().map(|purpose| {
            format!("{purpose} Residual fields not covered by existing shared components.")
        });
        residual.name = residual
            .descriptive_name
            .clone()
            .unwrap_or_else(|| "Residual Schema".to_string());
        Some(residual)
    }
}
