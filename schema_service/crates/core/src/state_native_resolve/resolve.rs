//! Embedding-beam native resolve.

use super::*;

impl SchemaServiceState {
    /// Try embedding-beam native resolve. Returns `Ok(Some(result))` when the
    /// native path produced a reuse-style outcome; `Ok(None)` when the caller
    /// should fall back to legacy matching.
    pub(crate) fn try_native_component_cover_resolve(
        &self,
        proposal: &SchemaResolveProposal,
    ) -> FoldDbResult<Option<SchemaResolveResult>> {
        if !native_component_cover_resolve_enabled() {
            return Ok(None);
        }

        let built = match self.build_native_resolver_input(proposal) {
            Ok(b) => b,
            Err(e) => {
                tracing::info!(
                    target: "schema_service::schema",
                    descriptive_name = %proposal.descriptive_name,
                    error = %e,
                    "native_component_cover: input build failed; legacy fallback"
                );
                return Ok(None);
            }
        };

        let output = match resolve_native(&built.input) {
            Ok(ResolveVerdict::Resolved(output)) => output,
            Ok(ResolveVerdict::Unresolvable(unresolvable)) => {
                tracing::debug!(
                    target: "schema_service::schema",
                    descriptive_name = %proposal.descriptive_name,
                    decision = ?unresolvable.decision,
                    "native_component_cover: unresolvable decision; legacy fallback"
                );
                return Ok(None);
            }
            Err(e) => {
                tracing::debug!(
                    target: "schema_service::schema",
                    descriptive_name = %proposal.descriptive_name,
                    error = %e,
                    "native_component_cover: evaluate failed; legacy fallback"
                );
                return Ok(None);
            }
        };

        match output.decision {
            ResolverDecision::UseExisting => {
                let Some(ref use_ex) = output.use_existing else {
                    return Ok(None);
                };
                let Some(reuse) =
                    self.schema_reuse_match_for_identity(proposal, &use_ex.schema_id)?
                else {
                    return Ok(None);
                };
                if !self.resolve_purpose_gate_passes(proposal, &reuse.schema.schema) {
                    return Ok(None);
                }
                let is_superset = reuse.is_superset;
                let shared_hash = reuse
                    .schema
                    .schema
                    .identity_hash
                    .clone()
                    .unwrap_or_else(|| reuse.schema.schema.name.clone());
                if is_superset {
                    Ok(Some(SchemaResolveResult {
                        outcome: SchemaResolveOutcome::Reuse,
                        matched_shared_schema_hash: Some(shared_hash),
                        r#match: Some(reuse),
                        candidates: Vec::new(),
                        candidate_shared_schema_hashes: Vec::new(),
                        confidence: Some(output.confidence),
                    }))
                } else {
                    Ok(Some(SchemaResolveResult {
                        outcome: SchemaResolveOutcome::CandidateEquivalent,
                        matched_shared_schema_hash: None,
                        r#match: None,
                        candidates: vec![reuse],
                        candidate_shared_schema_hashes: vec![shared_hash],
                        confidence: Some(output.confidence),
                    }))
                }
            }
            ResolverDecision::UseComponents => {
                if output.use_components.is_empty() {
                    return Ok(None);
                }
                let mut hashes: Vec<String> = output
                    .use_components
                    .iter()
                    .map(|c| c.schema_id.clone())
                    .collect();
                hashes.sort();
                hashes.dedup();
                // Prefer full reuse matches for components when available.
                let mut candidates = Vec::new();
                for h in &hashes {
                    if let Some(m) = self.schema_reuse_match_for_identity(proposal, h)? {
                        candidates.push(m);
                    }
                }
                // Every component must describe the same data as the
                // proposal (the register path's purpose gate). One that
                // does not turns the cover into a composition of unrelated
                // schemas that merely share generic fields; hand the
                // proposal to the gated legacy path instead.
                if candidates
                    .iter()
                    .any(|m| !self.resolve_purpose_gate_passes(proposal, &m.schema.schema))
                {
                    return Ok(None);
                }
                Ok(Some(SchemaResolveResult {
                    outcome: SchemaResolveOutcome::CandidateEquivalent,
                    matched_shared_schema_hash: None,
                    r#match: None,
                    candidates,
                    candidate_shared_schema_hashes: hashes,
                    confidence: Some(output.confidence),
                }))
            }
            ResolverDecision::ExpandExistingIfAllowed => {
                let Some(ref expand) = output.expand_existing_if_allowed else {
                    return Ok(None);
                };
                let Some(reuse) =
                    self.schema_reuse_match_for_identity(proposal, &expand.schema_id)?
                else {
                    return Ok(None);
                };
                if !self.resolve_purpose_gate_passes(proposal, &reuse.schema.schema) {
                    return Ok(None);
                }
                let shared_hash = reuse
                    .schema
                    .schema
                    .identity_hash
                    .clone()
                    .unwrap_or_else(|| reuse.schema.schema.name.clone());
                Ok(Some(SchemaResolveResult {
                    outcome: SchemaResolveOutcome::CandidateEquivalent,
                    matched_shared_schema_hash: None,
                    r#match: None,
                    candidates: vec![reuse],
                    candidate_shared_schema_hashes: vec![shared_hash],
                    confidence: Some(output.confidence),
                }))
            }
            ResolverDecision::Ambiguous
            | ResolverDecision::NeedsLiveSchemaService
            | ResolverDecision::Reject => {
                tracing::debug!(
                    target: "schema_service::schema",
                    descriptive_name = %proposal.descriptive_name,
                    decision = ?output.decision,
                    "native_component_cover: non-reuse decision; legacy fallback"
                );
                Ok(None)
            }
        }
    }

    pub(super) fn schema_reuse_match_for_identity(
        &self,
        proposal: &SchemaResolveProposal,
        identity: &str,
    ) -> FoldDbResult<Option<SchemaReuseMatch>> {
        let schemas = read_lock(&self.schemas, "schemas_cache")?;
        let Some(existing) = schemas.get(identity) else {
            return Ok(None);
        };
        let (active_schema, active_name) =
            match self.resolve_active_schema(existing, identity, &schemas) {
                Some(pair) => pair,
                None => (existing.clone(), identity.to_string()),
            };
        let matched_desc = active_schema
            .descriptive_name
            .clone()
            .unwrap_or_else(|| active_name.clone());
        Ok(Some(self.schema_reuse_match_from_proposal(
            proposal,
            active_schema,
            matched_desc,
            true,
        )))
    }
}
