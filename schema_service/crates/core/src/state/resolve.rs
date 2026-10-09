use super::*;
// lint:file-size-ok moved verbatim from the parent module; one method family per file

/// Bound `POST /v1/schemas/resolve` so the public read path cannot turn one
/// request into unbounded semantic matching work.
pub const MAX_SCHEMA_RESOLVE_PROPOSALS: usize = 50;

/// Resolve confidence: the match class, scaled by field coverage when the
/// match leaves proposal fields unmapped.
///
/// The class alone reported a fixed 0.6 for every partial, non-exact match,
/// so a hit that mapped 4 of 7 fields read as "60% confident" (2026-09-22,
/// LastgitRef onto LastgitPack). Consumers only report this number.
pub(super) fn resolve_match_confidence(
    reuse_match: &SchemaReuseMatch,
    proposal_field_count: usize,
) -> f32 {
    let class = match (reuse_match.is_exact_match, reuse_match.is_superset) {
        (true, true) => 1.0,
        (false, true) => 0.9,
        (true, false) => 0.75,
        (false, false) => 0.6,
    };
    if reuse_match.is_superset || proposal_field_count == 0 {
        return class;
    }
    let covered = proposal_field_count.saturating_sub(reuse_match.unmapped_fields.len());
    class * covered as f32 / proposal_field_count as f32
}

impl SchemaServiceState {
    /// Find schemas similar to the given schema using Jaccard index on field name sets
    pub fn find_similar_schemas(
        &self,
        name: &str,
        threshold: f64,
    ) -> FoldDbResult<SimilarSchemasResponse> {
        let schemas = read_lock(&self.schemas, "schemas")?;

        let target = schemas
            .get(name)
            .ok_or_else(|| FoldDbError::Config(format!("Schema '{name}' not found")))?;

        let target_fields = collect_field_names(target);

        let system_set: HashSet<String> = self
            .system_schema_hashes
            .read()
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default();

        let mut similar: Vec<SimilarSchemaEntry> = schemas
            .iter()
            .filter(|(k, _)| k.as_str() != name)
            .filter_map(|(schema_name, schema)| {
                let other_fields = collect_field_names(schema);
                let similarity = jaccard_index(&target_fields, &other_fields);
                if similarity >= threshold {
                    Some(SimilarSchemaEntry {
                        schema: SchemaEnvelope {
                            schema: schema.clone(),
                            system: system_set.contains(schema_name),
                        },
                        similarity,
                    })
                } else {
                    None
                }
            })
            .collect();

        similar.sort_by(|a, b| {
            b.similarity
                .partial_cmp(&a.similarity)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(SimilarSchemasResponse {
            query_schema: name.to_string(),
            threshold,
            similar_schemas: similar,
        })
    }

    /// Batch check whether proposed schemas can reuse existing ones.
    ///
    /// For each entry, finds a matching descriptive name (exact or semantic),
    /// resolves to the active (non-deprecated) schema, computes field rename
    /// maps, and determines if the existing schema is a superset.
    ///
    /// Read-only operation — only acquires read locks.
    pub fn batch_check_schema_reuse(
        &self,
        entries: &[SchemaLookupEntry],
    ) -> FoldDbResult<HashMap<String, SchemaReuseMatch>> {
        let mut results = HashMap::new();

        let schemas = read_lock(&self.schemas, "schemas_cache")?;

        for entry in entries {
            // 1. Find matching descriptive name (exact or semantic).
            // `SchemaLookupEntry` doesn't carry an `owner_app_id` (batch reuse
            // operates on the un-owned/legacy namespace), so the search is
            // scoped to `owner_app_id == None`.
            let (matched_desc, matched_hash, is_exact) =
                match self.find_matching_descriptive_name(&entry.descriptive_name, None) {
                    Ok((Some(desc), Some(hash), exact)) => (desc, hash, exact),
                    Ok(_) => continue,
                    Err(e) => {
                        tracing::warn!(
                        target: "schema_service::schema",
                                        "batch_check_schema_reuse: error matching '{}': {}",
                                        entry.descriptive_name,
                                        e
                                    );
                        continue;
                    }
                };

            // 2. Resolve to active (non-deprecated) schema
            let Some(existing) = schemas.get(&matched_hash) else {
                continue;
            };
            let (active_schema, _active_name) =
                match self.resolve_active_schema(existing, &matched_hash, &schemas) {
                    Some(pair) => pair,
                    None => (existing.clone(), matched_hash.clone()),
                };

            // 3. Get the active schema's fields
            let existing_fields: Vec<String> =
                active_schema.fields.as_ref().cloned().unwrap_or_default();

            // 4. Compute semantic field rename map
            let field_rename_map = self.semantic_field_rename_map(
                &entry.fields,
                &existing_fields,
                &entry.descriptive_name,
                &HashMap::new(),
                &active_schema.field_descriptions,
            );

            // 5. Determine superset status and unmapped fields
            let existing_set: HashSet<&String> = existing_fields.iter().collect();
            let mut unmapped = Vec::new();
            for f in &entry.fields {
                if !existing_set.contains(f) && !field_rename_map.contains_key(f) {
                    unmapped.push(f.clone());
                }
            }
            let is_superset = unmapped.is_empty();

            let system = self.is_system_schema(&active_schema.name);
            results.insert(
                entry.descriptive_name.clone(),
                SchemaReuseMatch {
                    schema: SchemaEnvelope {
                        schema: active_schema,
                        system,
                    },
                    matched_descriptive_name: matched_desc,
                    is_exact_match: is_exact,
                    field_rename_map,
                    is_superset,
                    unmapped_fields: unmapped,
                },
            );
        }

        Ok(results)
    }

    /// Resolve proposed schemas against the current shared registry without
    /// mutating registry state.
    ///
    /// This is the core for `POST /v1/schemas/resolve`. It deliberately avoids
    /// the `add_schema` pipeline, canonical-field registration, app registry
    /// writes, state-version bumps, and telemetry counters. It only reads the
    /// active schema registry and returns the same style of reuse advice that
    /// older batch-check callers consume.
    // lint:fn-size-ok moved verbatim from the parent module; splitting is separate work.
    pub fn resolve_schema_proposals(
        &self,
        request: &SchemaResolveRequest,
    ) -> FoldDbResult<SchemaResolveResponse> {
        if request.proposals.len() > MAX_SCHEMA_RESOLVE_PROPOSALS {
            return Err(FoldDbError::Config(format!(
                "too many schema resolve proposals: {} > {MAX_SCHEMA_RESOLVE_PROPOSALS}",
                request.proposals.len()
            )));
        }

        let registry_version = self.current_state_version();
        let cache_stale = request
            .client_registry_version
            .is_some_and(|client_version| client_version < registry_version);
        let mut results = HashMap::new();

        for proposal in &request.proposals {
            let result = if cache_stale {
                SchemaResolveResult {
                    outcome: SchemaResolveOutcome::Refresh,
                    matched_shared_schema_hash: None,
                    r#match: None,
                    candidates: Vec::new(),
                    candidate_shared_schema_hashes: Vec::new(),
                    confidence: None,
                }
            } else if let Some(exact) = self.resolve_schema_proposal_exact_match(proposal)? {
                // O(1) catalog hit: the proposal's identity_hash, or its
                // (owner_app_id, descriptive_name), is already an exact row.
                // This runs before the embedding-beam path because that
                // path embeds the whole registry per request; a fresh node
                // declaring schemas the catalog already holds must not pay
                // for it (2026-09-13: every fresh install timed out here and
                // then burned its add-schema quota on registers).
                let outcome = match exact.outcome {
                    SchemaResolveOutcome::Reuse => "matched_existing",
                    SchemaResolveOutcome::CandidateEquivalent => "component_cover",
                    SchemaResolveOutcome::Novel => "no_match",
                    SchemaResolveOutcome::Refresh => "refresh",
                };
                self.record_schema_match_outcome(outcome, "exact_catalog_row", None);
                exact
            } else if let Some(native) = self.try_native_component_cover_resolve(proposal)? {
                // embedding-beam / native_component_cover@1 (preferred live path)
                self.record_schema_match_outcome(
                    match native.outcome {
                        SchemaResolveOutcome::Reuse => "matched_existing",
                        SchemaResolveOutcome::CandidateEquivalent => "component_cover",
                        SchemaResolveOutcome::Novel => "no_match",
                        SchemaResolveOutcome::Refresh => "refresh",
                    },
                    "native_component_cover",
                    None,
                );
                native
            } else if let Some(reuse_match) = self.resolve_schema_proposal_match(proposal)? {
                let shared_hash = reuse_match
                    .schema
                    .schema
                    .identity_hash
                    .clone()
                    .unwrap_or_else(|| reuse_match.schema.schema.name.clone());
                let confidence = resolve_match_confidence(&reuse_match, proposal.fields.len());
                if reuse_match.is_superset {
                    SchemaResolveResult {
                        outcome: SchemaResolveOutcome::Reuse,
                        matched_shared_schema_hash: Some(shared_hash),
                        r#match: Some(reuse_match),
                        candidates: Vec::new(),
                        candidate_shared_schema_hashes: Vec::new(),
                        confidence: Some(confidence),
                    }
                } else {
                    SchemaResolveResult {
                        outcome: SchemaResolveOutcome::CandidateEquivalent,
                        matched_shared_schema_hash: None,
                        r#match: None,
                        candidates: vec![reuse_match],
                        candidate_shared_schema_hashes: vec![shared_hash],
                        confidence: Some(confidence),
                    }
                }
            } else {
                SchemaResolveResult {
                    outcome: SchemaResolveOutcome::Novel,
                    matched_shared_schema_hash: None,
                    r#match: None,
                    candidates: Vec::new(),
                    candidate_shared_schema_hashes: Vec::new(),
                    confidence: None,
                }
            };

            results.insert(proposal.descriptive_name.clone(), result);
        }

        Ok(SchemaResolveResponse {
            registry_version,
            cache_stale,
            results,
        })
    }

    /// Exact catalog lookups only — no embedding work. Returns a reuse-style
    /// result when the proposal's `identity_hash` is an active registry row,
    /// or when `(owner_app_id, descriptive_name)` resolves through the exact
    /// `descriptive_name_index`. `Ok(None)` means "not an exact row"; the
    /// caller then decides whether to spend on semantic matching.
    pub(super) fn resolve_schema_proposal_exact_match(
        &self,
        proposal: &SchemaResolveProposal,
    ) -> FoldDbResult<Option<SchemaResolveResult>> {
        let reuse_match = match self.exact_identity_reuse_match(proposal)? {
            Some(m) => m,
            None => match self.exact_owner_name_reuse_match(proposal)? {
                Some(m) => m,
                None => return Ok(None),
            },
        };
        let shared_hash = reuse_match
            .schema
            .schema
            .identity_hash
            .clone()
            .unwrap_or_else(|| reuse_match.schema.schema.name.clone());
        let confidence = resolve_match_confidence(&reuse_match, proposal.fields.len());
        Ok(Some(if reuse_match.is_superset {
            SchemaResolveResult {
                outcome: SchemaResolveOutcome::Reuse,
                matched_shared_schema_hash: Some(shared_hash),
                r#match: Some(reuse_match),
                candidates: Vec::new(),
                candidate_shared_schema_hashes: Vec::new(),
                confidence: Some(confidence),
            }
        } else {
            SchemaResolveResult {
                outcome: SchemaResolveOutcome::CandidateEquivalent,
                matched_shared_schema_hash: None,
                r#match: None,
                candidates: vec![reuse_match],
                candidate_shared_schema_hashes: vec![shared_hash],
                confidence: Some(confidence),
            }
        }))
    }

    /// Exact `identity_hash` row in the active registry, if any.
    pub(super) fn exact_identity_reuse_match(
        &self,
        proposal: &SchemaResolveProposal,
    ) -> FoldDbResult<Option<SchemaReuseMatch>> {
        let Some(identity_hash) = proposal
            .identity_hash
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        else {
            return Ok(None);
        };
        let schemas = read_lock(&self.schemas, "schemas_cache")?;
        let Some(existing) = schemas.get(identity_hash) else {
            return Ok(None);
        };
        let (active_schema, active_name) =
            match self.resolve_active_schema(existing, identity_hash, &schemas) {
                Some(pair) => pair,
                None => (existing.clone(), identity_hash.to_string()),
            };
        let matched_desc = active_schema
            .descriptive_name
            .clone()
            .unwrap_or_else(|| active_name.clone());
        Ok(Some(self.schema_reuse_match_from_proposal(
            proposal,
            active_schema,
            matched_desc,
            true,
        )))
    }

    /// Exact `(owner_app_id, descriptive_name)` row through the namespaced
    /// `descriptive_name_index`, if any. No semantic name matching here.
    pub(super) fn exact_owner_name_reuse_match(
        &self,
        proposal: &SchemaResolveProposal,
    ) -> FoldDbResult<Option<SchemaReuseMatch>> {
        let owner_app_id = proposal
            .owner_app_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let desc_name = proposal.descriptive_name.trim();
        if desc_name.is_empty() {
            return Ok(None);
        }
        let Some(matched_hash) =
            self.lookup_descriptive_name_in_namespace(owner_app_id, desc_name)?
        else {
            return Ok(None);
        };
        let schemas = read_lock(&self.schemas, "schemas_cache")?;
        let Some(existing) = schemas.get(&matched_hash) else {
            return Ok(None);
        };
        let (active_schema, _active_name) =
            match self.resolve_active_schema(existing, &matched_hash, &schemas) {
                Some(pair) => pair,
                None => (existing.clone(), matched_hash.clone()),
            };
        Ok(Some(self.schema_reuse_match_from_proposal(
            proposal,
            active_schema,
            desc_name.to_string(),
            true,
        )))
    }

    pub(super) fn resolve_schema_proposal_match(
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

    /// The register path's merge gates, applied to a resolve proposal.
    ///
    /// Mirrors `add_schema`: the second name gate
    /// ([`Self::schema_names_are_similar`]) and the purpose gate
    /// ([`Self::resolve_purpose_gate_passes`]).
    pub(super) fn resolve_semantic_name_hit_passes_merge_gates(
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
    pub(super) fn resolve_field_descriptions_agree(
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
        if !crate::state_matching::dual_signal_canonicalization_enabled() {
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

    pub(super) fn resolve_schema_proposal_field_superset_match(
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
