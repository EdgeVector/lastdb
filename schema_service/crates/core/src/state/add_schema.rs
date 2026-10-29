use super::*;

impl SchemaServiceState {
    /// [`Self::add_schema_inner`] with a defensive assertion that
    /// `owner_app_id` never silently disappears between request and
    /// persisted record.
    ///
    /// The inner pipeline already namespaces every dedup / expansion /
    /// race-guard lookup by `(owner_app_id, descriptive_name)` (see
    /// `descriptive_name_key`), so on the happy path the persisted
    /// schema's `owner_app_id` is always the request's. This wrapper is
    /// the belt-and-suspenders check: an app-tagged publish that lands
    /// un-owned is a bug we want to learn about at publish time, not
    /// from a `/v1/snapshot` diff days later (which is exactly how the
    /// 2026-05-30 fbrain dogfood found it).
    pub async fn add_schema(
        &self,
        mut schema: Schema,
        mutation_mappers: HashMap<String, String>,
    ) -> FoldDbResult<SchemaAddOutcome> {
        // Supersession is service-owned state. Decoding a stored schema must
        // preserve it, but a new registration proposal cannot assign it.
        // Exact public reposts return stored metadata before this write path.
        schema.superseded_by = None;
        Self::validate_proposal_shape(&schema)?;
        self.validate_schema_field_vocabulary(&schema).await?;
        self.record_schema_write();
        let input_owner_app_id = schema
            .owner_app_id
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let outcome = self.add_schema_inner(schema, mutation_mappers).await?;
        if let Some(expected) = input_owner_app_id.as_deref() {
            let persisted = match &outcome {
                SchemaAddOutcome::Added(s, _)
                | SchemaAddOutcome::AlreadyExists(s, _)
                | SchemaAddOutcome::Expanded(_, s, _)
                | SchemaAddOutcome::Composed(s, _, _) => Some(s),
                // A conflict outcome is itself a hard failure with no
                // persisted body, so the invariant doesn't apply.
                SchemaAddOutcome::DescriptiveNameConflict(_) => None,
            };
            if let Some(persisted) = persisted {
                let got = persisted.owner_app_id.as_deref();
                if got != Some(expected) {
                    return Err(FoldDbError::Config(format!(
                        "owner_app_id dropped during add_schema: \
                         request specified {expected:?} but persisted schema has {got:?} \
                         (descriptive_name={:?}, canonical_name={:?})",
                        persisted.descriptive_name, persisted.name,
                    )));
                }
            }
        }
        Ok(outcome)
    }

    /// Validate and canonicalize a proposal in place, up to (not including)
    /// the identity hash. `Ok(Err(outcome))` is an early idempotent-repost
    /// result; `Ok(Ok(advice))` carries the compositional reuse advice.
    fn normalize_proposal(
        &self,
        schema: &mut Schema,
        mutation_mappers: &mut HashMap<String, String>,
    ) -> FoldDbResult<Result<Vec<crate::ComponentReuseAdvice>, SchemaAddOutcome>> {
        Self::validate_proposal_shape(schema)?;
        self.validate_proposal_field_names(schema)?;

        // Snapshot version bump (app_identity v3.1, Lane B2a). add_schema
        // has many branches and outcomes (Added / AlreadyExisted /
        // Expanded / Superseded) but they all reach this point only when
        // a state-changing write is at least *attempted*. Bumping here
        // keeps the counter monotonic for clients refreshing the
        // snapshot, accepting the slight over-count on the rare paths
        // that no-op after validation.
        self.bump_state_version();

        self.auto_correct_user_descriptive_name(schema)?;

        // De-collide a User proposal whose (possibly auto-corrected)
        // descriptive_name exactly collides with an incompatible-schema_type
        // STARTER SEED. The ingestion LLM routinely proposes a Hash/Single
        // shape for a name a persona seed pre-claimed as Range (e.g.
        // "Contacts"). Cross-schema_type expansion would corrupt molecule
        // reads, so the proposal needs its own canonical — but a hard 409 here
        // silently drops the user's document. Rename it to a free variant so it
        // ingests cleanly; the seed anchor is left intact. User-vs-User
        // collisions are intentionally NOT de-collided (they keep the 409
        // contract — see `decollide_seed_descriptive_name`).
        // (Card `schema-eval-persona-seed-type-collision`.)
        if let Some(existing) = self.find_decollided_idempotent_repost(schema, mutation_mappers) {
            return Ok(Err(SchemaAddOutcome::AlreadyExists(
                existing,
                mutation_mappers.clone(),
            )));
        }

        if let Some(decollided) = self.decollide_seed_descriptive_name(schema) {
            schema.descriptive_name = Some(decollided);
            // The name changed, so any precomputed identity hash is stale.
            schema.identity_hash = None;
        }

        self.prepare_proposal_fields(schema, mutation_mappers)?;

        // Compositional canonicalization (advisory / shadow). When the
        // `SCHEMA_COMPOSITIONAL_DECOMPOSITION` flag is on, decompose the
        // canonicalized proposal into its nested (`ref_fields`) components and
        // log, per component, whether an existing same-purpose canonical could
        // be reused via a typed `SchemaRef`. This MIRRORS the dual-signal
        // Phase C shadow pass: it computes and records the would-be reuse but
        // does NOT alter the live add-schema decision. With the flag off
        // (default) this is a cheap no-op, so the persisted result is
        // byte-for-byte today's whole-schema behavior.
        let _compositional_advice = self.log_compositional_advice(schema);

        // Compositional APPLY step (only when `SCHEMA_COMPOSITIONAL_DECOMPOSITION=apply`).
        // Rewrite the proposal's `ref_fields` so each component that clears the
        // per-component reuse gate points at the matched existing canonical, so
        // a parent that registers as new *references* the reused sub-schemas
        // instead of re-inlining them. `ref_fields` is not part of
        // `compute_identity_hash`, so the dedup/expansion seams below are
        // unaffected; this only changes the reference topology of a brand-new
        // canonical. Empty unless apply mode rewrote ≥1 component — in which
        // case the terminal new-registration return emits `Composed`.
        let composition_advice = self.apply_compositional_reuse(schema);

        // Compute (or recompute after canonicalization) the identity hash.
        Ok(Ok(composition_advice))
    }

    pub(super) async fn add_schema_inner(
        &self,
        mut schema: Schema,
        mut mutation_mappers: HashMap<String, String>,
    ) -> FoldDbResult<SchemaAddOutcome> {
        let composition_advice =
            match self.normalize_proposal(&mut schema, &mut mutation_mappers)? {
                Ok(advice) => advice,
                Err(outcome) => return Ok(outcome),
            };

        // Compute (or recompute after canonicalization) the identity hash.
        schema.compute_identity_hash();

        // Get the original schema name before we modify it
        let original_schema_name = schema.name.clone();

        // Use identity_hash as the schema identifier
        let identity_hash = schema
            .get_identity_hash()
            .ok_or_else(|| {
                FoldDbError::Config("Schema must have identity_hash computed".to_string())
            })?
            .clone();

        tracing::info!(
            target: "schema_service::schema",
            "Schema '{}' identity_hash: {}",
            original_schema_name,
            identity_hash
        );

        // Schema name is ALWAYS the identity_hash (hash of semantic name + fields).
        // This guarantees:
        // - Same semantic name + same fields = same hash = dedup
        // - Same semantic name + different fields = different hash = separate schemas
        // - Different semantic name + same fields = different hash = separate schemas
        // The human-readable name lives in descriptive_name (for display/search).
        // `mut` so the final duplicate guard can rebind it after a User-vs-User
        // de-collide renames the descriptive_name (which changes the identity
        // hash) — card `schema-canon-exact-name-veto-409`.
        let mut schema_name = identity_hash.clone();

        if let Some(outcome) = self
            .canonicalization_gate_step(&mut schema, &schema_name, &mut mutation_mappers)
            .await?
        {
            return Ok(outcome);
        }

        schema.name = schema_name.clone();

        if let Some(outcome) = self
            .race_condition_guard(&mut schema, &schema_name, &mutation_mappers)
            .await?
        {
            return Ok(outcome);
        }

        if let Some(outcome) =
            self.final_duplicate_guard(&mut schema, &mut schema_name, &mutation_mappers)?
        {
            return Ok(outcome);
        }

        self.persist_new_canonical(&mut schema, &schema_name)
            .await?;

        tracing::info!(
            target: "schema_service::schema",
            "Schema '{}' successfully added to registry",
            schema_name
        );

        // A newly-registered parent whose `ref_fields` were rewritten to reuse
        // existing canonicals (compositional apply step) is `Composed`, not a
        // plain `Added` — it carries the per-component reuse advice that drove
        // the composition. `composition_advice` is empty unless apply mode is
        // on AND at least one component reused, so with the flag off this is
        // exactly `Added` as before.
        if composition_advice.is_empty() {
            Ok(SchemaAddOutcome::Added(schema, mutation_mappers))
        } else {
            Ok(SchemaAddOutcome::Composed(
                schema,
                mutation_mappers,
                composition_advice,
            ))
        }
    }
}
