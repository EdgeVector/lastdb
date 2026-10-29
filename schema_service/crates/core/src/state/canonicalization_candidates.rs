use super::*;
// lint:file-size-ok moved verbatim from add_schema.rs

impl SchemaServiceState {
    /// Collect every canonicalization candidate for `schema`, one seam at a
    /// time.
    pub(super) fn generate_canonicalization_candidates(
        &self,
        schema: &Schema,
        schema_name: &str,
    ) -> FoldDbResult<CandidateSet> {
        let mut set = CandidateSet::default();
        self.collect_identity_hash_candidate(schema, schema_name, &mut set)?;
        self.collect_descriptive_name_candidate(schema, &mut set)?;
        self.collect_field_overlap_candidate(schema, &mut set)?;
        self.collect_multi_key_sibling_candidate(schema, &mut set)?;
        self.collect_purpose_reuse_candidate(schema, &mut set)?;
        Ok(set)
    }

    pub(super) async fn canonicalization_gate_outcome(
        &self,
        incoming: &Schema,
        incoming_hash: &str,
        candidate: &MatchCandidate,
    ) -> CanonicalizationGateOutcome {
        // Multi-key siblings (same product fields, different lookup keys) always
        // take the map-fields + new-identity path. A purpose-signal veto must
        // not drop auto field_mappers and leave two unmapped keyed schemas.
        // Classic expand is still refused separately inside the Merge arm.
        if crate::state_expansion::is_cross_key_layout(incoming, &candidate.existing) {
            return CanonicalizationGateOutcome::Merge;
        }

        // The owning app named these two schemas differently: register the
        // incoming one as its own canonical instead of merging by field or
        // purpose similarity.
        if crate::state_canonicalization::same_app_distinct_name_veto(incoming, candidate) {
            tracing::info!(
                target: "schema_service::schema",
                owner_app_id = %incoming.owner_app_id.as_deref().unwrap_or(""),
                incoming_desc = %incoming.descriptive_name.as_deref().unwrap_or(""),
                existing_desc = %candidate.existing.descriptive_name.as_deref().unwrap_or(""),
                existing = %candidate.existing_hash,
                seam = ?candidate.seam,
                "Same-app distinct-name veto: registering a separate canonical",
            );
            return CanonicalizationGateOutcome::DeCollideAndRegister;
        }

        let strict_gate_allows_merge = if candidate.seam == MatchSeam::PurposeReuse {
            true
        } else {
            self.shadow_aware_dual_signal_check(
                incoming,
                &candidate.existing,
                incoming_hash,
                &candidate.existing_hash,
                candidate.seam.single_signal_decision(),
                candidate.seam.veto_decision(),
            )
            .await
        };

        gate_outcome_for_candidate(candidate, strict_gate_allows_merge)
    }
}
