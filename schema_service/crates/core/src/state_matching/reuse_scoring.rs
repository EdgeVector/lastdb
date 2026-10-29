use super::reuse::normalize;
use super::*;

impl SchemaServiceState {
    /// Active, same-namespace, non-system canonicals that the reuse scan may
    /// merge the proposal into. Mirrors the superseded and namespace filters at
    /// the structural merge seams.
    pub(super) fn is_reuse_eligible(
        &self,
        incoming: &Schema,
        existing: &Schema,
        target_owner: Option<&str>,
        allow_same_name: bool,
    ) -> bool {
        // Active, same-namespace canonicals only — mirrors the superseded +
        // namespace filters at the structural merge seams so reuse can't
        // land on a retired or cross-app schema.
        if existing.superseded_by.is_some() {
            return false;
        }
        if crate::builtin_schemas::is_schema_org_leftover(existing) {
            return false;
        }
        if normalize(existing.owner_app_id.as_deref()) != target_owner {
            return false;
        }
        let Some(ex_desc) = existing.descriptive_name.as_deref() else {
            return false;
        };
        // Never reuse the proposal against an entry with its own
        // descriptive_name — that case is the exact-name / semantic-name
        // merge seam's job and was already handled (or vetoed) upstream.
        //
        // EXCEPTION (`allow_same_name`): when the upstream exact-name seam
        // *purpose-vetoed* the merge (strict τ 0.88) and fell through here,
        // the same-name candidate was NOT handled — without this exception
        // the proposal dead-ends at the final duplicate guard's 409. Let it
        // be reconsidered under the looser reuse gates so a same-name
        // same-concept pair (Photos, purpose 0.85) collapses to one
        // canonical instead of 409'ing. See
        // [`find_purpose_reuse_target_allow_same_name`].
        if !allow_same_name
            && normalize(Some(ex_desc)) == normalize(incoming.descriptive_name.as_deref())
        {
            return false;
        }
        // Never reuse INTO a system/infrastructure schema. The Phase-1
        // built-ins (Fingerprint, Edge, Identity, Persona, …) are
        // authoritative and are seeded through this same `add_schema`
        // path at startup; letting a fuzzy purpose match expand one
        // built-in into another would (a) break the
        // `builtin_schemas::seed` invariant that built-ins never expand
        // an existing schema — aborting service start — and (b) dissolve
        // a distinct system identity. A user schema that genuinely IS a
        // built-in concept already merges via the exact/semantic-name
        // seam upstream; this last-chance fuzzy path stays user→user.
        if self.is_system_schema(&existing.name) {
            return false;
        }
        if super::super::state_expansion::is_cross_key_layout(incoming, existing) {
            return false;
        }

        true
    }

    /// Struct signal: descriptive_name similarity between the proposal and a
    /// candidate. `None` skips the candidate (embedding missing or failed).
    pub(super) fn reuse_struct_similarity(
        &self,
        inc_desc: &str,
        inc_desc_vec: &[f32],
        existing: &Schema,
        ex_desc: &str,
        embeddings: &HashMap<String, Vec<f32>>,
    ) -> Option<f32> {
        let ex_desc_semantic = strip_decollision_suffix(ex_desc);
        let was_decollided = ex_desc_semantic != ex_desc;
        let struct_sim = if inc_desc.eq_ignore_ascii_case(ex_desc_semantic) {
            1.0
        } else if was_decollided {
            let ex_vec = self.embedder.embed_text(ex_desc_semantic).ok()?;
            cosine_similarity(inc_desc_vec, &ex_vec)
        } else {
            let key = crate::state::descriptive_name_key(existing.owner_app_id.as_deref(), ex_desc);
            let ex_vec = embeddings.get(&key)?;
            cosine_similarity(inc_desc_vec, ex_vec)
        };
        Some(struct_sim.max(lexical_name_similarity(inc_desc, ex_desc_semantic)))
    }

    /// Purpose signal: similarity of the `"{desc} — {purpose}"` blobs. `None`
    /// skips the candidate when an embedding fails.
    pub(super) fn reuse_purpose_similarity(
        &self,
        inc_blob: &str,
        existing: &Schema,
        ex_desc_semantic: &str,
    ) -> Option<f32> {
        let ex_blob = format!(
            "{ex_desc_semantic} — {}",
            existing.purpose_statement.as_deref().unwrap_or("")
        );
        if inc_blob == ex_blob {
            return Some(1.0);
        }
        let a = self.embedder.embed_text(inc_blob).ok()?;
        let b = self.embedder.embed_text(&ex_blob).ok()?;
        Some(cosine_similarity(&a, &b))
    }

    /// Field-fidelity guard (card `schema-eval-judge-recommendations`): honour
    /// the reuse only when most of the proposal's fields map into the
    /// candidate's field set; otherwise the proposal registers as NEW.
    pub(super) fn apply_field_fidelity_guard(
        &self,
        incoming: &Schema,
        best: BestReuseCandidate,
        field_fidelity: f32,
    ) -> Option<(String, String)> {
        // Field-fidelity guard (card `schema-eval-judge-recommendations`). The
        // purpose+struct gates above never inspected the field sets, and every
        // earlier seam that DID (exact-hash dedup, semantic-name merge, the
        // Jaccard ≥ 0.6 field-overlap fallback) already missed — so by
        // construction this candidate's fields do NOT strongly overlap the
        // proposal's. Reusing/expanding into it anyway is the over-generalization
        // the eval's LLM-judge flags as a field-fidelity failure: the proposal's
        // own fields get force-merged into (or dropped in favour of) a canonical
        // full of fields the input never had. Only honour the reuse when the
        // proposal genuinely IS an instance of the candidate concept — i.e. most
        // of its fields map into the candidate's field set (literal match or a
        // confident semantic rename). Otherwise this is a distinct concept that
        // merely phrases its purpose similarly; let it register its OWN canonical
        // (NEW) with exactly its input fields.
        let coverage = self.reuse_field_coverage(incoming, &best);
        if coverage < field_fidelity {
            tracing::info!(
                target: "schema_service::schema",
                incoming_desc = %incoming.descriptive_name.as_deref().unwrap_or(""),
                reuse_candidate = %best.desc,
                purpose_similarity = best.purpose_sim,
                field_coverage = coverage,
                field_fidelity_floor = field_fidelity,
                "Reuse-before-NEW: purpose+name matched but the proposal's fields don't correspond to the candidate (over-generalization) — registering NEW instead of reusing",
            );
            return None;
        }

        tracing::info!(
            target: "schema_service::schema",
            incoming_desc = %incoming.descriptive_name.as_deref().unwrap_or(""),
            reuse_target = %best.desc,
            purpose_similarity = best.purpose_sim,
            field_coverage = coverage,
            "Reuse-before-NEW: purpose-gated semantic overlap with an existing canonical — expanding instead of registering a near-duplicate",
        );
        Some((best.hash, best.desc))
    }
}
