use super::*;

impl SchemaServiceState {
    /// Seam: the incoming identity hash already exists with no new fields.
    /// Returns `true` when a conflict was recorded.
    pub(super) fn collect_identity_hash_candidate(
        &self,
        schema: &Schema,
        schema_name: &str,
        set: &mut CandidateSet,
    ) -> FoldDbResult<bool> {
        let exact_hash_match: Option<(Schema, String, bool)> = {
            let schemas = read_lock(&self.schemas, "schemas")?;
            if let Some(existing_schema) = schemas.get(schema_name) {
                let (check_schema, check_name) = self
                    .resolve_active_schema(existing_schema, schema_name, &schemas)
                    .unwrap_or_else(|| (existing_schema.clone(), schema_name.to_string()));
                let existing_fields: HashSet<String> = check_schema
                    .fields
                    .as_ref()
                    .map(|f| f.iter().cloned().collect())
                    .unwrap_or_default();
                let incoming_fields: HashSet<String> = schema
                    .fields
                    .as_ref()
                    .map(|f| f.iter().cloned().collect())
                    .unwrap_or_default();
                let has_new_fields = !incoming_fields.is_subset(&existing_fields);
                Some((check_schema, check_name, has_new_fields))
            } else {
                None
            }
        };

        if let Some((existing, existing_hash, has_new_fields)) = exact_hash_match {
            if crate::state_expansion::is_cross_schema_type_expansion(schema, &existing) {
                let desc_name = existing
                    .descriptive_name
                    .clone()
                    .or_else(|| schema.descriptive_name.clone())
                    .unwrap_or_default();
                set.conflict = Some(Self::cross_schema_type_conflict(
                    schema,
                    &existing,
                    existing_hash,
                    desc_name,
                    format!(
                        "incoming schema_type {:?} differs from existing {:?}; \
                         identity_hash collides because schema_type is not part of \
                         the hash, but merging would corrupt molecule reads",
                        schema.schema_type, existing.schema_type,
                    ),
                ));
                return Ok(true);
            }
            if !has_new_fields {
                let desc_name = existing
                    .descriptive_name
                    .clone()
                    .or_else(|| schema.descriptive_name.clone())
                    .unwrap_or_default();
                set.candidates.push(MatchCandidate::new(
                    MatchSeam::IdentityHash,
                    existing_hash,
                    existing,
                    desc_name,
                    1.0,
                ));
            }
        }
        Ok(false)
    }

    /// Seam: exact or semantically similar descriptive_name.
    /// Returns `true` when a conflict was recorded.
    pub(super) fn collect_descriptive_name_candidate(
        &self,
        schema: &Schema,
        set: &mut CandidateSet,
    ) -> FoldDbResult<bool> {
        if let Some(incoming_desc_name) = schema.descriptive_name.as_deref() {
            let (matched_desc, existing_schema_name, is_exact_match) = self
                .find_matching_descriptive_name(
                    incoming_desc_name,
                    schema.owner_app_id.as_deref(),
                )?;
            let should_merge = if let Some(ref _old_name) = existing_schema_name {
                if is_exact_match {
                    true
                } else if let Some(ref canonical_desc) = matched_desc {
                    self.schema_names_are_similar(incoming_desc_name, canonical_desc)
                } else {
                    false
                }
            } else {
                false
            };

            if should_merge {
                if let Some(existing_hash) = existing_schema_name {
                    let existing = {
                        let schemas = read_lock(&self.schemas, "schemas")?;
                        schemas.get(&existing_hash).cloned()
                    };
                    if let Some(existing) = existing {
                        let desc_name =
                            matched_desc.unwrap_or_else(|| incoming_desc_name.to_string());
                        if crate::state_expansion::is_cross_schema_type_expansion(schema, &existing)
                        {
                            set.conflict = Some(Self::cross_schema_type_conflict(
                                schema,
                                &existing,
                                existing_hash,
                                desc_name,
                                format!(
                                    "incoming schema_type {:?} differs from existing {:?}; \
                                     cross-schema_type expansion would corrupt molecule reads",
                                    schema.schema_type, existing.schema_type,
                                ),
                            ));
                            return Ok(true);
                        }
                        set.candidates.push(MatchCandidate::new(
                            if is_exact_match {
                                MatchSeam::NameExact
                            } else {
                                MatchSeam::NameSemantic
                            },
                            existing_hash,
                            existing,
                            desc_name,
                            if is_exact_match { 1.0 } else { 0.9 },
                        ));
                    }
                }
            }
        }
        Ok(false)
    }

    /// Seam: same-key field overlap (Jaccard >= 0.6) with a similar name.
    /// Returns `true` when a conflict was recorded.
    pub(super) fn collect_field_overlap_candidate(
        &self,
        schema: &Schema,
        set: &mut CandidateSet,
    ) -> FoldDbResult<bool> {
        let overlap_target = {
            let schemas = read_lock(&self.schemas, "schemas")?;
            let incoming_fields: HashSet<String> = schema
                .fields
                .as_ref()
                .map(|f| f.iter().cloned().collect())
                .unwrap_or_default();

            let mut best: Option<(String, Schema, f64)> = None;
            for (existing_name, existing_schema) in schemas.iter() {
                if existing_schema.superseded_by.is_some() {
                    continue;
                }
                if existing_schema.owner_app_id != schema.owner_app_id {
                    continue;
                }
                let existing_fields: HashSet<String> = existing_schema
                    .fields
                    .as_ref()
                    .map(|f| f.iter().cloned().collect())
                    .unwrap_or_default();
                let jaccard =
                    crate::state_matching::jaccard_index(&incoming_fields, &existing_fields);
                if jaccard >= 0.6 {
                    if let (Some(inc_desc), Some(ext_desc)) = (
                        schema.descriptive_name.as_deref(),
                        existing_schema.descriptive_name.as_deref(),
                    ) {
                        if let (Ok(inc_emb), Ok(ext_emb)) = (
                            self.embedder.embed_text(inc_desc),
                            self.embedder.embed_text(ext_desc),
                        ) {
                            let name_sim = cosine_similarity(&inc_emb, &ext_emb);
                            if name_sim >= 0.8 && best.as_ref().is_none_or(|(_, _, j)| jaccard > *j)
                            {
                                best =
                                    Some((existing_name.clone(), existing_schema.clone(), jaccard));
                            }
                        }
                    }
                }
            }
            best
        };

        if let Some((existing_hash, existing, jaccard)) = overlap_target {
            let desc_name = existing
                .descriptive_name
                .clone()
                .or_else(|| schema.descriptive_name.clone())
                .unwrap_or_default();
            if crate::state_expansion::is_cross_schema_type_expansion(schema, &existing) {
                set.conflict = Some(Self::cross_schema_type_conflict(
                    schema,
                    &existing,
                    existing_hash,
                    desc_name,
                    format!(
                        "incoming schema_type {:?} differs from existing {:?}; \
                         field-overlap (Jaccard {:.2}) cannot bridge cross-schema_type \
                         without corrupting molecule reads",
                        schema.schema_type, existing.schema_type, jaccard,
                    ),
                ));
                return Ok(true);
            }
            // Same-key field overlap → classic expand candidate.
            // Cross-key field overlap is handled by the multi-key sibling pass
            // below (keeps both identities + field mappers).
            if !crate::state_expansion::is_cross_key_layout(schema, &existing) {
                set.candidates.push(MatchCandidate::new(
                    MatchSeam::FieldOverlap,
                    existing_hash,
                    existing,
                    desc_name,
                    jaccard as f32,
                ));
            }
        }
        Ok(false)
    }

    /// Seam: same product fields under a different key layout.
    pub(super) fn collect_multi_key_sibling_candidate(
        &self,
        schema: &Schema,
        set: &mut CandidateSet,
    ) -> FoldDbResult<()> {
        // Multi-key sibling discovery (preference-schema-expand-same-product-
        // different-keys): same owner, high field Jaccard, *different* key
        // layout. Descriptive names often differ (BoardCards vs MilestoneCards),
        // so do NOT require name-embedding similarity — field product shape +
        // cross-key is the signal. Purpose/lexical reuse deliberately skips
        // cross-key (they must not classic-expand); this seam is the path that
        // auto-applies field mappers and registers a second addressable identity.
        let multi_key_target = {
            let schemas = read_lock(&self.schemas, "schemas")?;
            let incoming_fields: HashSet<String> = schema
                .fields
                .as_ref()
                .map(|f| f.iter().cloned().collect())
                .unwrap_or_default();
            let mut best: Option<(String, Schema, f64)> = None;
            for (existing_name, existing_schema) in schemas.iter() {
                if existing_schema.superseded_by.is_some() {
                    continue;
                }
                if existing_schema.owner_app_id != schema.owner_app_id {
                    continue;
                }
                if !crate::state_expansion::is_cross_key_layout(schema, existing_schema) {
                    continue;
                }
                let existing_fields: HashSet<String> = existing_schema
                    .fields
                    .as_ref()
                    .map(|f| f.iter().cloned().collect())
                    .unwrap_or_default();
                let jaccard =
                    crate::state_matching::jaccard_index(&incoming_fields, &existing_fields);
                if jaccard >= 0.6 && best.as_ref().is_none_or(|(_, _, j)| jaccard > *j) {
                    best = Some((existing_name.clone(), existing_schema.clone(), jaccard));
                }
            }
            best
        };
        if let Some((existing_hash, existing, jaccard)) = multi_key_target {
            // Keep the *incoming* descriptive_name as the target so we do not
            // collapse MilestoneCards onto BoardCards' pin name.
            let desc_name = schema
                .descriptive_name
                .clone()
                .or_else(|| existing.descriptive_name.clone())
                .unwrap_or_default();
            set.candidates.push(MatchCandidate::new(
                MatchSeam::FieldOverlap,
                existing_hash,
                existing,
                desc_name,
                jaccard as f32,
            ));
        }
        Ok(())
    }

    /// Seam: purpose or lexical field reuse.
    pub(super) fn collect_purpose_reuse_candidate(
        &self,
        schema: &Schema,
        set: &mut CandidateSet,
    ) -> FoldDbResult<()> {
        if let Some((reuse_hash, reuse_desc)) = self
            .find_purpose_reuse_target_allow_same_name(schema)
            .or_else(|| self.find_lexical_field_reuse_target(schema))
        {
            let existing = {
                let schemas = read_lock(&self.schemas, "schemas")?;
                schemas.get(&reuse_hash).cloned()
            };
            if let Some(existing) = existing.filter(|s| s.superseded_by.is_none()) {
                set.candidates.push(MatchCandidate::new(
                    MatchSeam::PurposeReuse,
                    reuse_hash,
                    existing,
                    reuse_desc,
                    0.0,
                ));
            }
        }
        Ok(())
    }
}
