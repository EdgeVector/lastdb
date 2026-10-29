use super::*;

impl SchemaServiceState {
    /// Canonicalization-candidate seam: merge into an existing canonical
    /// (`AlreadyExists` / expansion), report a name conflict, or fall through
    /// to new registration (`None`).
    pub(super) async fn canonicalization_gate_step(
        &self,
        schema: &mut Schema,
        schema_name: &str,
        mutation_mappers: &mut HashMap<String, String>,
    ) -> FoldDbResult<Option<SchemaAddOutcome>> {
        let candidate_set = self.generate_canonicalization_candidates(schema, schema_name)?;

        if let Some(candidate) = rank_candidates(&candidate_set.candidates) {
            match self
                .canonicalization_gate_outcome(schema, schema_name, &candidate)
                .await
            {
                CanonicalizationGateOutcome::Merge => {
                    if let Some(outcome) = self
                        .merge_into_candidate(schema, schema_name, mutation_mappers, candidate)
                        .await?
                    {
                        return Ok(Some(outcome));
                    }
                }
                CanonicalizationGateOutcome::RescueRetry => {
                    // A strict dual-signal veto at a structural merge seam is
                    // authoritative. Fall through to the final descriptive-name
                    // guard, which de-collides exact-name user proposals and
                    // otherwise registers a distinct canonical when there is no
                    // collision.
                }
                CanonicalizationGateOutcome::DeCollideAndRegister => {
                    if candidate.seam == MatchSeam::IdentityHash {
                        let desc_name = candidate.target_descriptive_name.clone();
                        tracing::warn!(
                            target: "schema_service::schema",
                            descriptive_name = %desc_name,
                            existing_canonical = %candidate.existing_hash,
                            "Dual-signal veto on identity-hash dedup — returning DescriptiveNameConflict (409)",
                        );
                        return Ok(Some(SchemaAddOutcome::DescriptiveNameConflict(
                            crate::types::DescriptiveNameConflict {
                                existing_canonical: candidate.existing_hash,
                                descriptive_name: desc_name,
                                reason: "identity hash matches the existing canonical (same name and \
                                         fields) but the purpose-statement gate rejected the merge; \
                                         rename the descriptive_name to reflect its distinct purpose"
                                    .to_string(),
                            },
                        )));
                    }
                    // Fall through to new registration plus final duplicate
                    // guard/de-collision below.
                }
            }
        }

        Ok(None)
    }

    /// The gate approved a merge: return the existing canonical, map shared
    /// fields for a multi-key sibling (and fall through to registration), or
    /// expand the existing schema.
    async fn merge_into_candidate(
        &self,
        schema: &mut Schema,
        schema_name: &str,
        mutation_mappers: &mut HashMap<String, String>,
        candidate: MatchCandidate,
    ) -> FoldDbResult<Option<SchemaAddOutcome>> {
        let cross_key =
            crate::state_expansion::is_cross_key_layout_expansion(schema, &candidate.existing);

        if candidate.seam == MatchSeam::IdentityHash && !cross_key {
            tracing::info!(
                target: "schema_service::schema",
                "Schema '{}' already exists with same fields (active='{}') - returning existing",
                schema_name,
                candidate.existing_hash,
            );
            return Ok(Some(SchemaAddOutcome::AlreadyExists(
                candidate.existing,
                mutation_mappers.clone(),
            )));
        }

        // Same product, different keys: field-map + keep both identities.
        // Never classic expand (supersedes / can rewrite hash_field).
        if cross_key {
            let shared = crate::state_expansion::shared_field_names(schema, &candidate.existing);
            crate::state_expansion::apply_shared_field_mappers(
                schema,
                &candidate.existing_hash,
                &shared,
            );
            tracing::info!(
                target: "schema_service::schema",
                existing = %candidate.existing_hash,
                incoming_key = ?crate::state_expansion::key_layout_fingerprint(schema),
                existing_key = ?crate::state_expansion::key_layout_fingerprint(
                    &candidate.existing
                ),
                shared_fields = shared.len(),
                "Multi-key sibling: mapping shared fields; registering new identity \
                 (tip reindex required for the new key layout)",
            );
            // Fall through to new registration with mappers installed.
            // Do not adopt the existing descriptive_name (would fight the
            // sibling pin); keep the proposal's name when distinct.
        } else {
            if schema.descriptive_name.as_deref()
                != Some(candidate.target_descriptive_name.as_str())
            {
                tracing::info!(
                    target: "schema_service::schema",
                    incoming_desc = %schema.descriptive_name.as_deref().unwrap_or(""),
                    target_desc = %candidate.target_descriptive_name,
                    "Canonicalization candidate adopted target descriptive_name",
                );
            }
            schema.descriptive_name = Some(candidate.target_descriptive_name.clone());
            let incoming_fields = schema.fields.clone().unwrap_or_default();
            let existing_fields = candidate.existing.fields.clone().unwrap_or_default();
            let rename_map = self.semantic_field_rename_map(
                &incoming_fields,
                &existing_fields,
                &candidate.target_descriptive_name,
                &schema.field_descriptions,
                &candidate.existing.field_descriptions,
            );
            Self::apply_field_renames(schema, &rename_map, mutation_mappers);
            schema.dedup_fields();

            return self
                .expand_schema(
                    schema,
                    &candidate.existing,
                    &candidate.existing_hash,
                    &candidate.target_descriptive_name,
                    mutation_mappers,
                )
                .await
                .map(Some);
        }

        Ok(None)
    }

    /// Final defense-in-depth: a descriptive_name still bound to a different
    /// active canonical is either de-collided (rebinding `schema_name`) or
    /// reported as a 409 conflict.
    pub(super) fn final_duplicate_guard(
        &self,
        schema: &mut Schema,
        schema_name: &mut String,
        mutation_mappers: &HashMap<String, String>,
    ) -> FoldDbResult<Option<SchemaAddOutcome>> {
        let dup_conflict_hash: Option<String> = if let Some(ref desc_name) = schema.descriptive_name
        {
            // Namespace-aware lookup — see [`descriptive_name_key`] for the
            // (None, "a/b") vs (Some("a"), "b") aliasing that makes the
            // raw `index.get` answer namespace-impure.
            let preexisting = self
                .lookup_descriptive_name_in_namespace(schema.owner_app_id.as_deref(), desc_name)?;
            match preexisting {
                Some(existing_hash) if existing_hash != *schema_name => {
                    let existing_active = {
                        let schemas = read_lock(&self.schemas, "schemas")?;
                        schemas
                            .get(&existing_hash)
                            .filter(|s| s.superseded_by.is_none())
                            .cloned()
                    };
                    existing_active.map(|_| existing_hash)
                }
                _ => None,
            }
        } else {
            None
        };

        if let Some(existing_hash) = dup_conflict_hash {
            // This is the exact-name + purpose-VETOED dead-end (card
            // `schema-canon-exact-name-veto-409`). The proposal was already
            // offered same-name reuse under the looser purpose τ 0.72 +
            // field-fidelity gate (`find_purpose_reuse_target_allow_same_name`,
            // above) and that gate declined — the two same-name schemas are
            // genuinely distinct concepts. Rather than dead-end the user's
            // well-formed ingestion with a 409, de-collide the descriptive_name
            // so it registers as its OWN canonical — the design-blessed
            // non-merge outcome (the `distinct_purpose_blocks_merge_when_flag_on`
            // test accepts a fresh `Added` canonical as an alternative to 409).
            // The dual-signal split is preserved: distinct purposes still do NOT
            // merge; they just get separate canonicals instead of a rejection.
            if let Some(existing) = self.find_decollided_idempotent_repost(schema, mutation_mappers)
            {
                return Ok(Some(SchemaAddOutcome::AlreadyExists(
                    existing,
                    mutation_mappers.clone(),
                )));
            }

            if let Some(decollided) = self.decollide_user_descriptive_name(schema) {
                schema.descriptive_name = Some(decollided);
                // The descriptive_name changed → the identity hash is stale.
                // Recompute it and rebind `schema_name`/`schema.name` so the
                // persist path below stores the de-collided canonical.
                schema.identity_hash = None;
                schema.compute_identity_hash();
                *schema_name = schema
                    .get_identity_hash()
                    .ok_or_else(|| {
                        FoldDbError::Config(
                            "Schema must have identity_hash computed after de-collision"
                                .to_string(),
                        )
                    })?
                    .clone();
                schema.name = schema_name.clone();
                // Fall through to persist as a fresh canonical (no return).
            } else {
                let desc_name = schema.descriptive_name.clone().unwrap_or_default();
                tracing::warn!(
                    target: "schema_service::schema",
                    descriptive_name = %desc_name,
                    existing_hash = %existing_hash,
                    incoming_hash = %schema_name,
                    "Refusing duplicate descriptive_name registration — returning DescriptiveNameConflict (409)",
                );
                return Ok(Some(SchemaAddOutcome::DescriptiveNameConflict(
                    crate::types::DescriptiveNameConflict {
                        existing_canonical: existing_hash,
                        descriptive_name: desc_name,
                        reason: "an active schema with the same descriptive_name is already \
                                 registered with a different identity hash"
                            .to_string(),
                    },
                )));
            }
        }

        Ok(None)
    }
}
