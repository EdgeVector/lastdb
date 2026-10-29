use super::*;

impl SchemaServiceState {
    /// Highest-similarity same-namespace schema whose embedded
    /// `descriptive_name` clears [`DESCRIPTIVE_NAME_SIMILARITY_THRESHOLD`].
    fn best_semantic_descriptive_match(
        &self,
        query_embedding: &[f32],
        owner_app_id: Option<&str>,
    ) -> FoldDbResult<Option<(String, f32)>> {
        // Source of truth for the namespace is the schema's own
        // `owner_app_id` — `parse_canonical_name(existing_key)` is NOT
        // safe to use here because `descriptive_name_key` is non-injective
        // on `(Option<&str>, &str)`: a legacy un-owned schema whose
        // `descriptive_name` contains `/` (e.g. "Vehicles/Sedans") encodes
        // to the same key as `(Some("Vehicles"), "Sedans")`, so a parse
        // would misclassify it as belonging to `Some("Vehicles")` and
        // silently skip it from same-namespace lookups. Iterating schemas
        // first lets us read the actual `owner_app_id`. Same shape as the
        // bug fixed in `resolve_active_schema` and
        // `descriptive_name_conflicts_cross_type` at sibling sites.
        let schemas = read_lock(&self.schemas, "schemas")?;
        let embeddings = read_lock(
            &self.descriptive_name_embeddings,
            "descriptive_name_embeddings",
        )?;

        fn normalize(s: Option<&str>) -> Option<&str> {
            s.filter(|x| !x.is_empty())
        }
        let target_owner = normalize(owner_app_id);

        let mut best_match: Option<(String, f32)> = None;
        for schema in schemas.values() {
            // Superseded schemas are not merge targets — their
            // `descriptive_name_index` slot has already been replaced by
            // the active expansion successor (see `expand_schema`).
            if schema.superseded_by.is_some() {
                continue;
            }
            if crate::builtin_schemas::is_schema_org_leftover(schema) {
                continue;
            }
            if normalize(schema.owner_app_id.as_deref()) != target_owner {
                continue;
            }
            let Some(existing_desc) = schema.descriptive_name.as_deref() else {
                continue;
            };
            let key =
                crate::state::descriptive_name_key(schema.owner_app_id.as_deref(), existing_desc);
            let Some(existing_vec) = embeddings.get(&key) else {
                continue;
            };
            let sim = cosine_similarity(query_embedding, existing_vec);
            if sim >= DESCRIPTIVE_NAME_SIMILARITY_THRESHOLD
                && best_match
                    .as_ref()
                    .is_none_or(|(_, best_sim)| sim > *best_sim)
            {
                best_match = Some((existing_desc.to_string(), sim));
            }
        }
        Ok(best_match)
    }

    /// Find an existing descriptive_name that matches the given name within
    /// the `owner_app_id` namespace.
    ///
    /// First tries exact match (via the namespaced
    /// `descriptive_name_index`), then falls back to semantic (embedding)
    /// similarity restricted to other entries in the same namespace.
    /// Returns `(matched_descriptive_name, schema_identity_hash, is_exact_match)`.
    ///
    /// **Namespacing (app_identity v3.1, Lane B2b):** the dedup index and
    /// embedding cache are keyed by `descriptive_name_key(owner_app_id,
    /// desc)` (see [`super::state::descriptive_name_key`]). Passing
    /// `owner_app_id = Some("fbrain")` restricts the search to other
    /// `fbrain/*` schemas; `None` matches the un-owned / legacy / seed
    /// bucket. This is what stops an `fbrain/Project` registration from
    /// matching (and getting 409'd against) a same-named seed `Project`.
    pub(crate) fn find_matching_descriptive_name(
        &self,
        desc_name: &str,
        owner_app_id: Option<&str>,
    ) -> FoldDbResult<(Option<String>, Option<String>, bool)> {
        // 1. Exact match (within the same namespace). The helper rejects
        //    same-key hits whose resolved schema lives in a different
        //    namespace — `descriptive_name_key` is ambiguous on
        //    `(None, "a/b")` vs `(Some("a"), "b")`, and a raw index lookup
        //    would otherwise leak across namespaces.
        if let Some(hash) = self.lookup_descriptive_name_in_namespace(owner_app_id, desc_name)? {
            return Ok((Some(desc_name.to_string()), Some(hash), true));
        }

        // 2. Semantic similarity via embeddings — restricted to the same
        //    `owner_app_id` namespace so an `fbrain/Hiking Trip Reports` can't
        //    fuzzy-match into a seed `Hiking Trip Reports`.
        let query_embedding = match self.embedder.embed_text(desc_name) {
            Ok(vec) => vec,
            Err(e) => {
                tracing::warn!(
                target: "schema_service::schema",
                        "Failed to embed descriptive_name '{}' for similarity search: {}",
                        desc_name,
                        e
                    );
                return Ok((None, None, false));
            }
        };

        let best_match = self.best_semantic_descriptive_match(&query_embedding, owner_app_id)?;

        if let Some((matched_desc, similarity)) = best_match {
            tracing::info!(
            target: "schema_service::schema",
                "Semantic descriptive_name match: '{}' ≈ '{}' (similarity: {:.3})",
                desc_name,
                matched_desc,
                similarity
            );
            // Re-resolve through the namespace-aware helper so a
            // matched_desc whose `descriptive_name_key` happens to alias
            // another namespace's entry doesn't return that entry's hash.
            let hash = self.lookup_descriptive_name_in_namespace(owner_app_id, &matched_desc)?;
            return Ok((Some(matched_desc), hash, false));
        }

        Ok((None, None, false))
    }

    /// Check whether two schema names are semantically similar enough to be
    /// considered the same collection. Uses embedding similarity on the
    /// human-readable form of the names (underscores → spaces).
    ///
    /// This acts as a second gate for descriptive_name matching: even if
    /// "Holiday Illustration" ≈ "Famous Paintings" in embedding space, the
    /// schema names `artwork_collection` vs `famous_paintings` should NOT merge.
    pub(crate) fn schema_names_are_similar(&self, incoming: &str, existing: &str) -> bool {
        // Exact match (case-insensitive)
        if incoming.eq_ignore_ascii_case(existing) {
            return true;
        }

        // Convert snake_case to readable form for embedding comparison
        let readable_incoming = incoming.replace('_', " ");
        let readable_existing = existing.replace('_', " ");

        let Ok(incoming_emb) = self.embedder.embed_text(&readable_incoming) else {
            return false;
        };
        let Ok(existing_emb) = self.embedder.embed_text(&readable_existing) else {
            return false;
        };

        let sim = cosine_similarity(&incoming_emb, &existing_emb);
        tracing::info!(
            target: "schema_service::schema",
            "Schema name similarity: '{}' vs '{}' = {:.3}",
            incoming,
            existing,
            sim
        );
        // Use a high threshold — schema names are short and precise, so only
        // near-synonyms should match (e.g., "blog_posts" ≈ "blog_articles").
        sim >= 0.85
    }

    /// Compute the Phase B/C diagnostic for one (incoming, existing) pair:
    /// both the structural descriptive_name similarity and the dual-signal
    /// purpose similarity, plus the gate result. Returns `None` when any
    /// required embedding fails — callers treat that as a veto (Phase B
    /// hot path) or as "no near-miss data" (Phase C shadow). The
    /// "embedder failure = veto" rule matches the original
    /// `purpose_signal_passes`.
    pub(crate) fn dual_signal_diagnostic(
        &self,
        incoming: &Schema,
        existing: &Schema,
    ) -> Option<DualSignalDiagnostic> {
        let inc_desc = incoming.descriptive_name.as_deref().unwrap_or("");
        let ex_desc = existing.descriptive_name.as_deref().unwrap_or("");
        let inc_purpose = incoming.purpose_statement.as_deref().unwrap_or("");
        let ex_purpose = existing.purpose_statement.as_deref().unwrap_or("");

        // Structural signal — descriptive_name cosine. Exact-match
        // short-circuit avoids the embedder trip and matches the
        // exact-match arm in `find_matching_descriptive_name`.
        let struct_similarity = if inc_desc.eq_ignore_ascii_case(ex_desc) {
            1.0
        } else {
            let inc_vec = self.embedder.embed_text(inc_desc).ok()?;
            let ex_vec = self.embedder.embed_text(ex_desc).ok()?;
            cosine_similarity(&inc_vec, &ex_vec)
        };

        // Purpose signal — identical formula to `purpose_signal_passes`.
        // Identical blobs short-circuit to 1.0 without an embedder trip
        // (cosine of a vector with itself), mirroring the struct-signal
        // exact-match short-circuit above. This keeps the identity-hash
        // dedup seam free of embedding calls on the idempotent re-publish
        // hot path (same name, same fields, same purpose).
        let inc_blob = format!("{inc_desc} — {inc_purpose}");
        let ex_blob = format!("{ex_desc} — {ex_purpose}");
        let purpose_similarity = if inc_blob == ex_blob {
            1.0
        } else {
            let inc_vec = self.embedder.embed_text(&inc_blob).ok()?;
            let ex_vec = self.embedder.embed_text(&ex_blob).ok()?;
            cosine_similarity(&inc_vec, &ex_vec)
        };

        Some(DualSignalDiagnostic {
            struct_similarity,
            purpose_similarity,
            allows_merge: purpose_similarity >= PURPOSE_SIMILARITY_THRESHOLD,
        })
    }
}
