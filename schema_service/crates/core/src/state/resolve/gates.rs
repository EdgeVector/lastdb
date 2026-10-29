use super::*;

impl SchemaServiceState {
    /// The register path's merge gates, applied to a resolve proposal.
    ///
    /// Mirrors `add_schema`: the second name gate
    /// ([`Self::schema_names_are_similar`]) and the purpose gate
    /// ([`Self::resolve_purpose_gate_passes`]).
    pub(crate) fn resolve_semantic_name_hit_passes_merge_gates(
        &self,
        proposal: &SchemaResolveProposal,
        matched_desc: &str,
        existing: &Schema,
    ) -> bool {
        self.schema_names_are_similar(&proposal.descriptive_name, matched_desc)
            && self.resolve_purpose_gate_passes(proposal, existing)
    }

    /// Whether a resolve proposal and an existing schema describe the same
    /// data, by the register path's purpose gate: the same
    /// `"{descriptive_name} — {purpose}"` blob and threshold as
    /// [`Self::dual_signal_diagnostic`], off only when
    /// `SCHEMA_DUAL_SIGNAL_CANONICALIZATION` turns it off. An embedder failure
    /// is a veto, as in the Phase B hot path.
    ///
    /// Shared generic fields (`repo`, `oid`, `schema_version`, a composite key
    /// described the same way) are not evidence of the same data: every
    /// LastGit schema carries them, and without this gate the native
    /// component cover composed `LastgitCiStatus` out of `LastgitCommitMeta`
    /// and `LastgitRef` at confidence 1.0 with 5 of 10 fields unmapped
    /// (DEV, 2026-09-22).
    /// Whether every proposal field is described the same way on `existing`:
    /// identical text, or an embedding cosine at the purpose threshold. A
    /// description present on one side only, or an embedder failure, is a
    /// disagreement.
    pub(crate) fn resolve_field_descriptions_agree(
        &self,
        proposal: &SchemaResolveProposal,
        existing: &Schema,
    ) -> bool {
        proposal.fields.iter().all(|field| {
            let ours = proposal.field_descriptions.get(field).map(|d| d.trim());
            let theirs = existing.field_descriptions.get(field).map(|d| d.trim());
            match (ours, theirs) {
                (None, None) => true,
                (Some(a), Some(b)) if a.eq_ignore_ascii_case(b) => true,
                (Some(a), Some(b)) => {
                    let (Ok(va), Ok(vb)) =
                        (self.embedder.embed_text(a), self.embedder.embed_text(b))
                    else {
                        return false;
                    };
                    cosine_similarity(&va, &vb)
                        >= crate::state_matching::PURPOSE_SIMILARITY_THRESHOLD
                }
                _ => false,
            }
        })
    }

    pub(crate) fn resolve_purpose_gate_passes(
        &self,
        proposal: &SchemaResolveProposal,
        existing: &Schema,
    ) -> bool {
        if !self.dual_signal_canonicalization_enabled() {
            return true;
        }
        let inc_blob = format!(
            "{} — {}",
            proposal.descriptive_name,
            proposal.purpose_statement.as_deref().unwrap_or("")
        );
        let ex_blob = format!(
            "{} — {}",
            existing.descriptive_name.as_deref().unwrap_or(""),
            existing.purpose_statement.as_deref().unwrap_or("")
        );
        if inc_blob == ex_blob {
            return true;
        }
        let (Ok(inc_vec), Ok(ex_vec)) = (
            self.embedder.embed_text(&inc_blob),
            self.embedder.embed_text(&ex_blob),
        ) else {
            return false;
        };
        let purpose_similarity = cosine_similarity(&inc_vec, &ex_vec);
        let passes = purpose_similarity >= crate::state_matching::PURPOSE_SIMILARITY_THRESHOLD;
        if !passes {
            tracing::info!(
                target: "schema_service::schema",
                incoming = %proposal.descriptive_name,
                existing = %existing.descriptive_name.as_deref().unwrap_or(""),
                purpose_similarity,
                "resolve: candidate vetoed by the purpose gate",
            );
        }
        passes
    }
}
