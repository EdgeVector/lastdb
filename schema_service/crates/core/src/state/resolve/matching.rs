use super::*;

impl SchemaServiceState {
    pub(crate) fn resolve_schema_proposal_match(
        &self,
        proposal: &SchemaResolveProposal,
    ) -> FoldDbResult<Option<SchemaReuseMatch>> {
        if let Some(identity_hash) = proposal
            .identity_hash
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            let schemas = read_lock(&self.schemas, "schemas_cache")?;
            if let Some(existing) = schemas.get(identity_hash) {
                let (active_schema, active_name) =
                    match self.resolve_active_schema(existing, identity_hash, &schemas) {
                        Some(pair) => pair,
                        None => (existing.clone(), identity_hash.to_string()),
                    };
                let matched_desc = active_schema
                    .descriptive_name
                    .clone()
                    .unwrap_or_else(|| active_name.clone());
                return Ok(Some(self.schema_reuse_match_from_proposal(
                    proposal,
                    active_schema,
                    matched_desc,
                    true,
                )));
            }
        }

        let owner_app_id = proposal
            .owner_app_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let (matched_desc, matched_hash, is_exact) = match self
            .find_matching_descriptive_name(&proposal.descriptive_name, owner_app_id)
        {
            Ok((Some(desc), Some(hash), exact)) => (desc, hash, exact),
            Ok(_) => {
                return self.resolve_schema_proposal_field_superset_match(proposal, owner_app_id);
            }
            Err(e) => {
                tracing::warn!(
                    target: "schema_service::schema",
                    descriptive_name = %proposal.descriptive_name,
                    "resolve_schema_proposal: error matching descriptive name: {}",
                    e
                );
                return self.resolve_schema_proposal_field_superset_match(proposal, owner_app_id);
            }
        };

        let (active_schema, _active_name) = {
            let schemas = read_lock(&self.schemas, "schemas_cache")?;
            let Some(existing) = schemas.get(&matched_hash) else {
                return Ok(None);
            };
            match self.resolve_active_schema(existing, &matched_hash, &schemas) {
                Some(pair) => pair,
                None => (existing.clone(), matched_hash),
            }
        };

        // A semantic (non-exact) name hit is only the first of the register
        // path's gates. `add_schema` also requires `schema_names_are_similar`
        // and, by default, the dual-signal purpose gate before it merges; this
        // read-only resolve skipped both, so a node was told to reuse a schema
        // that registration would have refused. Seen 2026-09-22 on DEV:
        // `LastgitRef_hashrange_v2` resolved to `LastgitPack_hashrange_v2`
        // (git refs onto pack files) on the name embedding alone, with 3 of 7
        // fields unmapped. A vetoed name hit falls through to the same
        // field-superset lookup a name miss gets.
        if !is_exact
            && !self.resolve_semantic_name_hit_passes_merge_gates(
                proposal,
                &matched_desc,
                &active_schema,
            )
        {
            return self.resolve_schema_proposal_field_superset_match(proposal, owner_app_id);
        }

        Ok(Some(self.schema_reuse_match_from_proposal(
            proposal,
            active_schema,
            matched_desc,
            is_exact,
        )))
    }

    pub(crate) fn resolve_schema_proposal_field_superset_match(
        &self,
        proposal: &SchemaResolveProposal,
        owner_app_id: Option<&str>,
    ) -> FoldDbResult<Option<SchemaReuseMatch>> {
        let Some(owner_app_id) = owner_app_id else {
            return Ok(None);
        };
        if proposal.fields.is_empty() {
            return Ok(None);
        }

        let proposal_fields: HashSet<&String> = proposal.fields.iter().collect();
        let schemas = read_lock(&self.schemas, "schemas_cache")?;
        let mut candidates: Vec<Schema> = schemas
            .values()
            .filter(|schema| {
                schema.superseded_by.is_none()
                    && schema.owner_app_id.as_deref().filter(|s| !s.is_empty())
                        == Some(owner_app_id)
                    && schema.fields.as_ref().is_some_and(|fields| {
                        let existing: HashSet<&String> = fields.iter().collect();
                        proposal_fields.is_subset(&existing)
                    })
            })
            .cloned()
            .collect();

        candidates.sort_by(|a, b| {
            let a_len = a.fields.as_ref().map_or(usize::MAX, Vec::len);
            let b_len = b.fields.as_ref().map_or(usize::MAX, Vec::len);
            a_len
                .cmp(&b_len)
                .then_with(|| {
                    a.descriptive_name
                        .as_deref()
                        .unwrap_or(a.name.as_str())
                        .cmp(b.descriptive_name.as_deref().unwrap_or(b.name.as_str()))
                })
                .then_with(|| a.name.cmp(&b.name))
        });

        drop(schemas);

        // Field NAMES alone are not evidence of the same data; the field
        // DESCRIPTIONS are. This fallback lets an app's schema declared under
        // a node-local name (`Pantry Item Local`, same fields, same
        // descriptions) reuse the app's canonical, so the name and purpose
        // may differ — but every shared field must describe the same thing.
        // Without this it answered LastgitPackBlobIndex with LastgitRepoIndex
        // on DEV (2026-09-22): both are `key` / `payload_json` / `updated_at`
        // rollups, of pack blobs and of repos.
        let Some(active_schema) = candidates
            .into_iter()
            .find(|candidate| self.resolve_field_descriptions_agree(proposal, candidate))
        else {
            return Ok(None);
        };
        let matched_desc = active_schema
            .descriptive_name
            .clone()
            .unwrap_or_else(|| active_schema.name.clone());

        Ok(Some(self.schema_reuse_match_from_proposal(
            proposal,
            active_schema,
            matched_desc,
            false,
        )))
    }

    pub(crate) fn schema_reuse_match_from_proposal(
        &self,
        proposal: &SchemaResolveProposal,
        active_schema: Schema,
        matched_descriptive_name: String,
        is_exact_match: bool,
    ) -> SchemaReuseMatch {
        let existing_fields: Vec<String> =
            active_schema.fields.as_ref().cloned().unwrap_or_default();
        let field_rename_map = self.semantic_field_rename_map(
            &proposal.fields,
            &existing_fields,
            &proposal.descriptive_name,
            &proposal.field_descriptions,
            &active_schema.field_descriptions,
        );

        let existing_set: HashSet<&String> = existing_fields.iter().collect();
        let unmapped_fields: Vec<String> = proposal
            .fields
            .iter()
            .filter(|field| {
                !existing_set.contains(*field) && !field_rename_map.contains_key(*field)
            })
            .cloned()
            .collect();
        let is_superset = unmapped_fields.is_empty();
        let system = self.is_system_schema(&active_schema.name);

        SchemaReuseMatch {
            schema: SchemaEnvelope {
                schema: active_schema,
                system,
            },
            matched_descriptive_name,
            is_exact_match,
            field_rename_map,
            is_superset,
            unmapped_fields,
        }
    }
}
