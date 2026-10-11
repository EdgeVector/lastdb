use super::*;

impl SchemaServiceState {
    /// The descriptive_name index already points at the incoming identity hash.
    /// Returns a 409 for a cross-schema_type collision, `AlreadyExists` for a
    /// true duplicate, or `None` (after mapping fields) for a multi-key
    /// sibling so the caller falls through to registration.
    fn race_same_hash_outcome(
        schema: &mut Schema,
        existing_hash: &str,
        existing: &Schema,
        desc_name: &str,
        mutation_mappers: &HashMap<String, String>,
    ) -> Option<SchemaAddOutcome> {
        // The "existing" entry IS the schema we are about to persist
        // (identical identity_hash). Treat as AlreadyExists rather
        // than conflict — the index just hadn't caught up earlier in
        // this call, but they're the same canonical.
        //
        // Sibling guard: a concurrent add could have just inserted
        // an entry with the same identity_hash but a different
        // `schema_type` (the hash doesn't include schema_type).
        // Refuse with 409 instead of handing the caller back a
        // schema with the wrong on-disk shape — same gate the
        // early dedup short-circuit applies.
        if crate::state_expansion::is_cross_schema_type_expansion(schema, existing) {
            tracing::warn!(
                target: "schema_service::schema",
                descriptive_name = %desc_name,
                existing_canonical = %existing_hash,
                old_schema_type = ?existing.schema_type,
                new_schema_type = ?schema.schema_type,
                "Refusing cross-schema_type identity-hash collision in race-condition guard — returning DescriptiveNameConflict (409)",
            );
            return Some(SchemaAddOutcome::DescriptiveNameConflict(
                crate::types::DescriptiveNameConflict {
                    existing_canonical: existing_hash.to_string(),
                    descriptive_name: desc_name.to_string(),
                    reason: format!(
                        "incoming schema_type {:?} differs from existing {:?}; \
                         identity_hash collides because schema_type is not part \
                         of the hash, but merging would corrupt molecule reads",
                        schema.schema_type, existing.schema_type,
                    ),
                },
            ));
        }

        // Defense: if key layout ever fails to participate in
        // identity_hash, same hash + different keys must NOT
        // AlreadyExists-discard the new layout (that was the
        // multi-key silent-discard bug). Map fields and fall
        // through to de-collide / register a second identity.
        if crate::state_expansion::is_cross_key_layout_expansion(schema, existing) {
            let shared = crate::state_expansion::shared_field_names(schema, existing);
            crate::state_expansion::apply_shared_field_mappers(schema, existing_hash, &shared);
            tracing::warn!(
                target: "schema_service::schema",
                "Race-condition guard: identity_hash '{}' collides across key layouts \
                 — multi-key sibling (map fields; will de-collide / re-hash)",
                existing_hash,
            );
            // Fall through; do not return AlreadyExists.
        } else {
            tracing::info!(
                target: "schema_service::schema",
                "Race-condition guard: descriptive_name '{}' already points at incoming hash '{}' — short-circuit AlreadyExists",
                desc_name,
                existing_hash,
            );
            return Some(SchemaAddOutcome::AlreadyExists(
                existing.clone(),
                mutation_mappers.clone(),
            ));
        }

        None
    }

    /// Re-check the descriptive_name index to catch a concurrent add_schema
    /// that registered the same descriptive_name between the read-only checks
    /// and the persist.
    pub(super) async fn race_condition_guard(
        &self,
        schema: &mut Schema,
        schema_name: &str,
        mutation_mappers: &HashMap<String, String>,
    ) -> FoldDbResult<Option<SchemaAddOutcome>> {
        let race_expansion_target = if let Some(ref desc_name_owned) = schema.descriptive_name {
            // Namespace-aware lookup — a raw `index.get` could surface a
            // legacy un-owned schema whose `descriptive_name` happens to
            // alias the incoming `descriptive_name_key`.
            self.lookup_descriptive_name_in_namespace(
                schema.owner_app_id.as_deref(),
                desc_name_owned,
            )?
        } else {
            None
        };

        if let Some(existing_hash) = race_expansion_target {
            let existing = {
                let schemas = read_lock(&self.schemas, "schemas")?;
                schemas.get(&existing_hash).cloned()
            };
            if let Some(existing) = existing {
                if existing.superseded_by.is_none() {
                    if let Some(outcome) = self
                        .race_against_existing(
                            schema,
                            schema_name,
                            existing_hash,
                            existing,
                            mutation_mappers,
                        )
                        .await?
                    {
                        return Ok(Some(outcome));
                    }
                }
            }
        }

        Ok(None)
    }

    /// A concurrent add registered the same descriptive_name: answer with
    /// `AlreadyExists`, a 409, or an expansion; `None` falls through to the
    /// final duplicate guard, which de-collides the name.
    async fn race_against_existing(
        &self,
        schema: &mut Schema,
        schema_name: &str,
        existing_hash: String,
        existing: Schema,
        mutation_mappers: &HashMap<String, String>,
    ) -> FoldDbResult<Option<SchemaAddOutcome>> {
        let desc_name_owned = schema.descriptive_name.clone().unwrap_or_default();
        if existing_hash == schema_name {
            if let Some(outcome) = Self::race_same_hash_outcome(
                schema,
                &existing_hash,
                &existing,
                &desc_name_owned,
                mutation_mappers,
            ) {
                return Ok(Some(outcome));
            }
        }
        if crate::state_expansion::is_cross_schema_type_expansion(schema, &existing) {
            tracing::warn!(
                target: "schema_service::schema",
                descriptive_name = %desc_name_owned,
                existing_hash = %existing_hash,
                old_schema_type = ?existing.schema_type,
                new_schema_type = ?schema.schema_type,
                "Refusing cross-schema_type expansion in race-condition path — returning DescriptiveNameConflict (409)",
            );
            return Ok(Some(SchemaAddOutcome::DescriptiveNameConflict(
                crate::types::DescriptiveNameConflict {
                    existing_canonical: existing_hash,
                    descriptive_name: desc_name_owned,
                    reason: format!(
                        "incoming schema_type {:?} differs from existing {:?}; \
                         cross-schema_type expansion would corrupt molecule reads",
                        schema.schema_type, existing.schema_type,
                    ),
                },
            )));
        }
        let race_candidate = MatchCandidate::new(
            MatchSeam::NameExact,
            existing_hash.clone(),
            existing.clone(),
            desc_name_owned.clone(),
            1.0,
        );
        let race_gate = self
            .canonicalization_gate_outcome(schema, schema_name, &race_candidate)
            .await;
        if race_gate == CanonicalizationGateOutcome::Merge {
            if crate::state_expansion::is_cross_key_layout_expansion(schema, &existing) {
                let shared = crate::state_expansion::shared_field_names(schema, &existing);
                crate::state_expansion::apply_shared_field_mappers(schema, &existing_hash, &shared);
                tracing::warn!(
                    target: "schema_service::schema",
                    "Race condition: descriptive_name '{}' already registered as '{}' \
                     with different key layout — multi-key sibling (map fields, new id)",
                    desc_name_owned,
                    existing_hash
                );
                // Fall through to de-collide + register; do not expand.
            } else {
                tracing::warn!(
                target: "schema_service::schema",
                            "Race condition: descriptive_name '{}' already registered as '{}' — redirecting to expansion",
                            desc_name_owned,
                            existing_hash
                        );
                return self
                    .expand_schema(
                        schema,
                        &existing,
                        &existing_hash,
                        &desc_name_owned,
                        mutation_mappers,
                    )
                    .await
                    .map(Some);
            }
        }
        // Phase B dual-signal veto on the race-condition path:
        // fall through to the final defense, which de-collides
        // the descriptive_name so the genuinely-distinct
        // same-name proposal registers as its OWN canonical
        // instead of dead-ending at a 409. In shadow mode this
        // branch is never taken — the helper returns true and
        // the near-miss is recorded via the side effect.

        Ok(None)
    }
}
