use super::*;
impl SchemaServiceState {
    /// Reject proposals whose shape would break identity hashing or the
    /// descriptive_name / field registries. Pure: reads the proposal only.
    pub(super) fn validate_proposal_shape(schema: &Schema) -> FoldDbResult<()> {
        schema.validate_definition().map_err(FoldDbError::Config)?;
        Self::validate_descriptive_name(schema)?;
        Self::validate_field_names(schema)
    }

    /// The descriptive_name must be present and must not use the reserved
    /// `app:` prefix on an un-owned schema.
    fn validate_descriptive_name(schema: &Schema) -> FoldDbResult<()> {
        // descriptive_name is required — it's how schemas are identified, displayed,
        // and matched for expansion. A schema without one is a bug in the caller.
        if schema
            .descriptive_name
            .as_ref()
            .is_none_or(|dn| dn.trim().is_empty())
        {
            return Err(FoldDbError::Config(
                "Schema must have a non-empty descriptive_name".to_string(),
            ));
        }

        // The "app:" prefix is reserved for the app-namespaced
        // `compute_identity_hash` format (`app:{owner_app_id}:{descriptive_name}:
        // {sorted fields}`, see `Schema::compute_identity_hash`). A legacy
        // (un-owned) schema whose descriptive_name starts with `app:` can be
        // hand-crafted so its hash input equals an app-owned schema's input —
        // e.g. (owner_app_id=None, descriptive_name="app:kanban:Tasks", fields=["title"])
        // and (owner_app_id=Some("kanban"), descriptive_name="Tasks", fields=["title"])
        // both hash to sha256("app:kanban:Tasks:title"). Schemas are keyed by
        // identity_hash, so the collision silently merges two cross-namespace
        // schemas into one slot — the very namespace invariant
        // `owner_app_id` is meant to enforce (app_identity v3.1, Lane B2a).
        // Reject up-front rather than relying on `compute_identity_hash`
        // being injective on adversarial inputs.
        if schema
            .owner_app_id
            .as_deref()
            .filter(|s| !s.is_empty())
            .is_none()
            && schema
                .descriptive_name
                .as_deref()
                .is_some_and(|dn| dn.starts_with("app:"))
        {
            return Err(FoldDbError::Config(
                "Schema descriptive_name must not start with reserved prefix 'app:' \
                 — that prefix is reserved for app-namespaced identity hashes; \
                 set owner_app_id and a plain descriptive_name instead"
                    .to_string(),
            ));
        }

        Ok(())
    }

    /// Every field name must be non-blank and free of the `,` and `:`
    /// separators that `compute_identity_hash` uses.
    fn validate_field_names(schema: &Schema) -> FoldDbResult<()> {
        // Mirror the descriptive_name guard for `fields`: every entry must be
        // a non-empty, non-whitespace identifier. Without this check a
        // payload like `fields = [""]` slipped through (the caller could
        // satisfy the `field_descriptions` contains-key check by adding a
        // `""` description) and the blank entry then propagated into
        // `compute_identity_hash` (joined with `,` so it produced a leading-
        // comma input string), into `populate_runtime_fields` (which keys
        // `runtime_fields` by the blank name), and into the global
        // canonical-field registry. Every downstream lookup assumes field
        // identifiers are non-empty; reject up front rather than letting
        // the malformed input persist.
        if let Some(ref fields) = schema.fields {
            let blank: Vec<&String> = fields.iter().filter(|f| f.trim().is_empty()).collect();
            if !blank.is_empty() {
                return Err(FoldDbError::Config(format!(
                    "Schema fields contain empty/whitespace-only field name(s): {blank:?}"
                )));
            }
        }

        // Field names must not contain `,` — `compute_identity_hash` joins
        // sorted field names with that exact separator to build the hash
        // input, so a field name containing `,` makes the join ambiguous and
        // collapses semantically distinct schemas onto the same identity
        // hash. For example, with `descriptive_name = "Foo"`:
        //   - `fields = ["a", "b,c"]`  → sorted+joined "a,b,c" → hash "Foo:a,b,c"
        //   - `fields = ["a", "b", "c"]` → sorted+joined "a,b,c" → hash "Foo:a,b,c"
        // Both inputs route to the same identity_hash slot, so the dedup
        // gate at `schemas.get(&schema_name)` short-circuits to
        // `AlreadyExists` / expansion and the caller receives a schema with
        // the wrong field shape. Reject up-front rather than relying on
        // `compute_identity_hash` being injective on adversarial inputs.
        // Same shape as the `app:`-prefix guard above and the
        // empty-field-name guard just above it.
        if let Some(ref fields) = schema.fields {
            let with_comma: Vec<&String> = fields.iter().filter(|f| f.contains(',')).collect();
            if !with_comma.is_empty() {
                return Err(FoldDbError::Config(format!(
                    "Schema field names must not contain ',' (would collide on \
                     identity_hash, which joins sorted field names with ','): {with_comma:?}"
                )));
            }
        }

        // Field names must not contain `:` — `compute_identity_hash` uses
        // `:` as the *part* separator between `app:{owner_app_id}:` (when
        // present), `descriptive_name`, and the comma-joined field list.
        // A field name containing `:` shifts the boundary between the
        // descriptive_name and field segments, collapsing semantically
        // distinct schemas onto the same identity hash. For example, with
        // `owner_app_id = None`:
        //   - `descriptive_name="Foo:bar", fields=["x"]`  → hash input "Foo:bar:x"
        //   - `descriptive_name="Foo",     fields=["bar:x"]` → hash input "Foo:bar:x"
        // And with `owner_app_id = Some("kanban")`:
        //   - `descriptive_name="Tasks", fields=["title:done"]`   → "app:kanban:Tasks:title:done"
        //   - `descriptive_name="Tasks:title", fields=["done"]`   → "app:kanban:Tasks:title:done"
        // Both inputs route to the same identity_hash slot, so the dedup
        // gate at `schemas.get(&schema_name)` short-circuits to
        // `AlreadyExists` and the caller receives a schema with the wrong
        // (descriptive_name, fields) shape. Reject up-front rather than
        // relying on `compute_identity_hash` being injective on
        // adversarial inputs. Same shape as the `,`-field-name guard
        // above (PR #511), the empty-field-name guard (PR #507), and the
        // `app:`-prefix descriptive_name guard.
        if let Some(ref fields) = schema.fields {
            let with_colon: Vec<&String> = fields.iter().filter(|f| f.contains(':')).collect();
            if !with_colon.is_empty() {
                return Err(FoldDbError::Config(format!(
                    "Schema field names must not contain ':' (would collide on \
                     identity_hash, which uses ':' to separate owner_app_id, \
                     descriptive_name, and the joined field list): {with_colon:?}"
                )));
            }
        }

        Ok(())
    }

    /// Replace caption / generic / over-specific descriptive names on
    /// User-proposed schemas, or reject the proposal when no acceptable
    /// correction exists.
    pub(super) fn auto_correct_user_descriptive_name(
        &self,
        schema: &mut Schema,
    ) -> FoldDbResult<()> {
        // Auto-correct bad descriptive names:
        // 1. AI captions ("A photo of a sunset") → use schema.name title-cased
        // 2. Generic structural names ("Document Collection") → use schema.name title-cased
        // 3. Over-specific instance-level names (e.g. "Roasted Tomato Soup
        //    Recipe" for one recipe file) → use schema.name title-cased,
        //    which the ingestion prompt steers to a category like "recipes".
        // 4. If title-cased name is STILL generic (e.g. "content_articles" → "Content Articles"),
        //    fall back to generate_collection_name() which infers from field patterns.
        //
        // Only applied to User-proposed schemas. SystemSeed and StarterSeed
        // schemas are pre-validated at definition time — overriding
        // "Article" or "Note" on a Schema.org StarterSeed would break the
        // well-known vocabulary we're trying to preserve.
        let is_user_schema = schema.source == schema_types::SchemaSource::User;
        if is_user_schema {
            if let Some(ref dn) = schema.descriptive_name.clone() {
                let is_caption = Self::is_caption_name(dn);
                let is_generic = crate::name_validator::is_generic_name(dn);
                let is_over_specific = crate::name_validator::is_over_specific_name(dn);
                if is_caption || is_generic || is_over_specific {
                    let reason = if is_caption {
                        "caption"
                    } else if is_generic {
                        "generic"
                    } else {
                        "over-specific"
                    };
                    if let Some(corrected) = self.improve_descriptive_name(schema, dn) {
                        tracing::warn!(
                            target: "schema_service::schema",
                            "Auto-corrected descriptive_name from '{}' to '{}' ({})",
                            truncate_on_char_boundary(dn, 60),
                            corrected,
                            reason,
                        );
                        schema.descriptive_name = Some(corrected);
                    } else {
                        let message = crate::name_validator::reject_generic_name(dn)
                            .err()
                            .unwrap_or_else(|| {
                                let problem = match reason {
                                    "caption" => "looks like a caption",
                                    "over-specific" => "is too specific to a single item",
                                    _ => "is not an acceptable content-specific name",
                                };
                                format!(
                                    "Schema descriptive_name '{dn}' {problem}, and no \
                                     acceptable content-specific correction could be \
                                     derived. Re-run classification with a \
                                     descriptive_name that names the content topic."
                                )
                            });
                        tracing::warn!(
                            target: "schema_service::schema",
                            "Detected {} descriptive_name '{}'; no acceptable improvement available — rejecting user schema",
                            reason,
                            truncate_on_char_boundary(dn, 60)
                        );
                        return Err(FoldDbError::Config(message));
                    }
                }
            }
        }
        Ok(())
    }

    /// Default the purpose statement, require field descriptions, canonicalize
    /// field names against the global registry, and dedup fields.
    pub(super) fn prepare_proposal_fields(
        &self,
        schema: &mut Schema,
        mutation_mappers: &mut HashMap<String, String>,
    ) -> FoldDbResult<()> {
        // Phase A of dual-signal canonicalization: if the proposal didn't
        // supply a `purpose_statement`, default it to the (possibly
        // auto-corrected) `descriptive_name` so downstream consumers always
        // see a value. Phase B will tighten this — once the purpose-embedding
        // pipeline is in place, a missing or empty purpose will likely be
        // rejected outright. See fbrain `dual-signal-schema-canonicalization`.
        if schema
            .purpose_statement
            .as_deref()
            .is_none_or(|p| p.trim().is_empty())
        {
            schema.purpose_statement = schema.descriptive_name.clone();
        }

        // field_descriptions is required — the schema service uses them for
        // semantic field matching (embedding "field_name: description").
        // Without descriptions, field matching degrades to bare name comparison.
        if let Some(ref fields) = schema.fields {
            let missing: Vec<&String> = fields
                .iter()
                .filter(|f| !schema.field_descriptions.contains_key(*f))
                .collect();
            if !missing.is_empty() {
                return Err(FoldDbError::Config(format!(
                    "Schema fields missing descriptions (required for semantic matching): {missing:?}"
                )));
            }
        }

        // Canonicalize field names against the global canonical field registry
        // before any dedup or identity hash computation.
        if let Some(ref fields) = schema.fields {
            let rename_map = self.canonicalize_fields(fields, schema, mutation_mappers)?;
            if !rename_map.is_empty() {
                Self::apply_field_renames(schema, &rename_map, mutation_mappers);
                // Canonicalization changed field names, so any precomputed identity
                // hash is stale — force recomputation below.
                schema.identity_hash = None;
            }
        }

        // Catalog field identity hashes (name+description+type+version) so
        // local Mini can map (same key) or protein-bind (different keys).
        // Runs *after* renames so hashes key under the final field names.
        schema.ensure_field_hashes();

        // Deduplicate fields before computing identity hash
        schema.dedup_fields();
        Ok(())
    }
}
