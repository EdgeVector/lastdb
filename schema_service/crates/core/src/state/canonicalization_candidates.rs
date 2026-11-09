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

    pub(super) fn canonicalization_gate_outcome(
        &self,
        incoming: &Schema,
        candidate: &MatchCandidate,
    ) -> CanonicalizationGateOutcome {
        // Different keys always retain separate definitions. The Merge arm
        // recognizes this layout and registers it without value aliases;
        // classic expansion is refused there and in the race-condition guard.
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
            self.purpose_gate_allows_structural_merge(incoming, &candidate.existing)
        };

        gate_outcome_for_candidate(candidate, strict_gate_allows_merge)
    }

    /// Purpose gate for a structural canonicalization candidate.
    ///
    /// An embedder failure is a veto. A veto is logged and the caller
    /// registers a separate canonical.
    fn purpose_gate_allows_structural_merge(&self, incoming: &Schema, existing: &Schema) -> bool {
        let dual_allows = self
            .dual_signal_diagnostic(incoming, existing)
            .unwrap_or(false);
        if !dual_allows {
            tracing::info!(
                target: "schema_service::schema",
                incoming_desc = %incoming.descriptive_name.as_deref().unwrap_or(""),
                existing_desc = %existing.descriptive_name.as_deref().unwrap_or(""),
                "Dual-signal veto: structural match rejected by purpose-signal gate — registering as new canonical",
            );
        }
        dual_allows
    }
}
