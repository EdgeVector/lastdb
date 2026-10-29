use super::*;

impl SchemaServiceState {
    /// Decompose a proposal into its nested (`ref_fields`) components and, for
    /// each, decide whether an existing same-purpose canonical can be reused
    /// via a typed `SchemaRef`.
    ///
    /// **Advisory-only** — never mutates state and never changes the caller's
    /// add-schema outcome. Returns one [`ComponentReuseAdvice`] per
    /// `ref_fields` component, in deterministic (field-name-sorted) order so
    /// logs and tests are stable. A proposal with no `ref_fields` yields an
    /// empty vec (the common flat-proposal case — zero cost).
    ///
    /// Matching reuses the existing dual-signal machinery: a candidate
    /// canonical is a reuse target iff its purpose-statement embedding clears
    /// [`PURPOSE_SIMILARITY_THRESHOLD`] against the component — the same
    /// τ_purpose gate the whole-schema path uses, so a component reuse can
    /// never be *looser* than a whole-schema merge. Schemas are identified by
    /// PURPOSE, not field shape (`feedback_schema_semantic_distinction`).
    pub(crate) fn compositional_component_advice(
        &self,
        proposal: &Schema,
    ) -> Vec<ComponentReuseAdvice> {
        // Deterministic order for stable logs/tests.
        let mut components: Vec<(&String, &String)> = proposal.ref_fields.iter().collect();
        components.sort_by(|a, b| a.0.cmp(b.0));

        let mut advice: Vec<ComponentReuseAdvice> = components
            .into_iter()
            .map(|(field, child_schema)| {
                let (reuse_target, purpose_similarity) =
                    self.best_component_reuse(proposal, field, child_schema);
                ComponentReuseAdvice {
                    field: field.clone(),
                    proposed_child_schema: child_schema.clone(),
                    reuse_target,
                    purpose_similarity,
                }
            })
            .collect();

        if let Some(fields) = proposal.fields.as_ref() {
            let mut inferred_fields: Vec<&String> = fields
                .iter()
                .filter(|field| !proposal.ref_fields.contains_key(*field))
                .collect();
            inferred_fields.sort();

            for field in inferred_fields {
                let child_schema = inferred_component_name(field);
                let (reuse_target, purpose_similarity) =
                    self.best_component_reuse(proposal, field, &child_schema);
                if reuse_target.is_some() {
                    advice.push(ComponentReuseAdvice {
                        field: field.clone(),
                        proposed_child_schema: child_schema,
                        reuse_target,
                        purpose_similarity,
                    });
                }
            }
        }

        advice.sort_by(|a, b| a.field.cmp(&b.field));
        advice
    }

    /// Find the best existing canonical to reuse for one component.
    ///
    /// Builds a synthetic single-component schema (the child schema name as
    /// both `descriptive_name` and `purpose_statement` context — the proposal
    /// gives us the field's role and any description) and runs it through the
    /// existing [`Self::dual_signal_diagnostic`] purpose gate against every
    /// active, same-namespace canonical. Returns the highest-scoring candidate
    /// that clears [`PURPOSE_SIMILARITY_THRESHOLD`] (or the best sub-threshold
    /// score, with `reuse_target = None`, for tuning visibility).
    ///
    /// Read-only: takes the `schemas` read lock briefly and never persists.
    fn best_component_reuse(
        &self,
        proposal: &Schema,
        field: &str,
        child_schema: &str,
    ) -> (Option<String>, Option<f32>) {
        // The component's purpose: prefer the proposal's field description
        // (it states what this nested thing is *for*), else fall back to the
        // child schema name. This is the same "name + description" signal the
        // field-canonicalization path uses.
        let component_purpose = proposal
            .field_descriptions
            .get(field)
            .cloned()
            .unwrap_or_else(|| child_schema.replace('_', " "));

        let component = synthetic_component_schema(
            child_schema,
            &component_purpose,
            proposal.owner_app_id.as_deref(),
        );

        let Ok(schemas) = self.schemas.read() else {
            return (None, None);
        };

        fn normalize(s: Option<&str>) -> Option<&str> {
            s.filter(|x| !x.is_empty())
        }
        let target_owner = normalize(proposal.owner_app_id.as_deref());

        let mut best: Option<(String, f32)> = None;
        for existing in schemas.values() {
            // Only active canonicals in the same namespace are reuse targets —
            // mirrors `find_matching_descriptive_name`'s superseded + namespace
            // filters so a component can't reuse a retired or cross-app schema.
            if existing.superseded_by.is_some() {
                continue;
            }
            if existing.source != SchemaSource::User {
                continue;
            }
            if normalize(existing.owner_app_id.as_deref()) != target_owner {
                continue;
            }
            // Don't reuse the proposal against itself if it's already registered.
            if existing.descriptive_name.as_deref() == proposal.descriptive_name.as_deref() {
                continue;
            }
            let Some(diag) = self.dual_signal_diagnostic(&component, existing) else {
                let coverage = component_field_coverage(&component_purpose, existing);
                if coverage >= 0.6
                    && best
                        .as_ref()
                        .is_none_or(|(_, best_sim)| coverage > *best_sim)
                {
                    let name = existing
                        .descriptive_name
                        .clone()
                        .unwrap_or_else(|| existing.name.clone());
                    best = Some((name, coverage));
                }
                continue;
            };
            let sim = diag
                .purpose_similarity
                .max(component_field_coverage(&component_purpose, existing));
            if best.as_ref().is_none_or(|(_, best_sim)| sim > *best_sim) {
                let name = existing
                    .descriptive_name
                    .clone()
                    .unwrap_or_else(|| existing.name.clone());
                best = Some((name, sim));
            }
        }
        drop(schemas);

        match best {
            Some((name, sim)) if sim >= PURPOSE_SIMILARITY_THRESHOLD => (Some(name), Some(sim)),
            Some((_, sim)) => (None, Some(sim)),
            None => (None, None),
        }
    }

    /// Advisory (shadow-mode) compositional pass, called from
    /// [`Self::add_schema`]. When [`Self::compositional_decomposition_enabled`] is
    /// off this is a cheap no-op; when on it decomposes the proposal and logs
    /// the per-component reuse verdict WITHOUT changing the add-schema outcome.
    ///
    /// Returns the advice so callers/tests can assert on it; the live add path
    /// ignores the return today (advisory-only — the `apply` rewrite is a
    /// follow-on).
    pub(crate) fn log_compositional_advice(&self, proposal: &Schema) -> Vec<ComponentReuseAdvice> {
        if !self.compositional_decomposition_enabled() {
            return Vec::new();
        }
        let advice = self.compositional_component_advice(proposal);
        if advice.is_empty() {
            return advice;
        }
        let reused = advice.iter().filter(|a| a.reuses()).count();
        tracing::info!(
            target: "schema_service::schema",
            descriptive_name = %proposal.descriptive_name.as_deref().unwrap_or(""),
            components = advice.len(),
            reusable = reused,
            "compositional (advisory): proposal decomposed into nested components; \
             {reused}/{} could reuse an existing canonical via SchemaRef",
            advice.len(),
        );
        for a in &advice {
            if let Some(target) = &a.reuse_target {
                tracing::info!(
                    target: "schema_service::schema",
                    field = %a.field,
                    child = %a.proposed_child_schema,
                    reuse_target = %target,
                    purpose_similarity = a.purpose_similarity.unwrap_or(0.0),
                    "compositional (advisory): component would reuse existing canonical",
                );
            } else {
                tracing::debug!(
                    target: "schema_service::schema",
                    field = %a.field,
                    child = %a.proposed_child_schema,
                    purpose_similarity = a.purpose_similarity.unwrap_or(0.0),
                    "compositional (advisory): component has no reuse target — stays in residual",
                );
            }
        }
        advice
    }

    /// Compositional **apply** step (the follow-on to [`Self::log_compositional_advice`]).
    /// When [`Self::compositional_apply_enabled`] is on, decompose `schema` into its
    /// nested components and, for every component that clears the per-component
    /// reuse gate, rewrite its `ref_fields` entry to point at the matched
    /// existing canonical's descriptive name — so the persisted parent
    /// *references* the reused sub-schema instead of implying a fresh child.
    ///
    /// Returns the advice for the components that were actually rewritten
    /// (empty when apply is off or nothing reused). The caller emits
    /// [`crate::types::SchemaAddOutcome::Composed`] for a newly-registered
    /// parent iff this returns non-empty.
    ///
    /// **Purely additive.** Only `ref_fields` *targets* change; `fields`,
    /// `field_descriptions`, `schema_type`, and the identity-hash inputs are
    /// untouched (`ref_fields` is not part of `compute_identity_hash` —
    /// declarative_schemas.rs). So the dedup / expansion / dual-signal seams
    /// downstream behave byte-for-byte as they do today; the only difference is
    /// the reference topology of a parent that registers as new.
    pub(crate) fn apply_compositional_reuse(
        &self,
        schema: &mut Schema,
    ) -> Vec<ComponentReuseAdvice> {
        if !self.compositional_apply_enabled() {
            return Vec::new();
        }
        let advice = self.compositional_component_advice(schema);
        let mut applied = Vec::new();
        for a in &advice {
            if let Some(target) = &a.reuse_target {
                // Point the typed ref at the existing canonical to reuse it.
                // Idempotent when the proposal already named the canonical.
                schema.ref_fields.insert(a.field.clone(), target.clone());
                applied.push(a.clone());
            }
        }
        if !applied.is_empty() {
            tracing::info!(
                target: "schema_service::schema",
                descriptive_name = %schema.descriptive_name.as_deref().unwrap_or(""),
                reused = applied.len(),
                components = advice.len(),
                "compositional (apply): rewrote {}/{} ref_fields component(s) to reuse existing canonicals via SchemaRef",
                applied.len(),
                advice.len(),
            );
        }
        applied
    }
}
