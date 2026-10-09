use super::*;
// lint:file-size-ok moved verbatim from add_schema.rs; one method family per file

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
        schema: Schema,
        mutation_mappers: HashMap<String, String>,
    ) -> FoldDbResult<SchemaAddOutcome> {
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

    pub(super) async fn add_schema_inner(
        &self,
        mut schema: Schema,
        mut mutation_mappers: HashMap<String, String>,
    ) -> FoldDbResult<SchemaAddOutcome> {
        // lint:fn-size-ok moved verbatim from the original impl; splitting is a separate change
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

        // Snapshot version bump (app_identity v3.1, Lane B2a). add_schema
        // has many branches and outcomes (Added / AlreadyExisted /
        // Expanded / Superseded) but they all reach this point only when
        // a state-changing write is at least *attempted*. Bumping here
        // keeps the counter monotonic for clients refreshing the
        // snapshot, accepting the slight over-count on the rare paths
        // that no-op after validation.
        self.bump_state_version();

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
                    if let Some(corrected) = self.improve_descriptive_name(&schema, dn) {
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
        if let Some(existing) = self.find_decollided_idempotent_repost(&schema, &mutation_mappers) {
            return Ok(SchemaAddOutcome::AlreadyExists(
                existing,
                mutation_mappers.clone(),
            ));
        }

        if let Some(decollided) = self.decollide_seed_descriptive_name(&schema) {
            schema.descriptive_name = Some(decollided);
            // The name changed, so any precomputed identity hash is stale.
            schema.identity_hash = None;
        }

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
            let rename_map = self.canonicalize_fields(fields, &schema, &mut mutation_mappers);
            if !rename_map.is_empty() {
                Self::apply_field_renames(&mut schema, &rename_map, &mut mutation_mappers);
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

        // Compositional canonicalization (advisory / shadow). When the
        // `SCHEMA_COMPOSITIONAL_DECOMPOSITION` flag is on, decompose the
        // canonicalized proposal into its nested (`ref_fields`) components and
        // log, per component, whether an existing same-purpose canonical could
        // be reused via a typed `SchemaRef`. This MIRRORS the dual-signal
        // Phase C shadow pass: it computes and records the would-be reuse but
        // does NOT alter the live add-schema decision. With the flag off
        // (default) this is a cheap no-op, so the persisted result is
        // byte-for-byte today's whole-schema behavior.
        let _compositional_advice = self.log_compositional_advice(&schema);

        // Compositional APPLY step (only when `SCHEMA_COMPOSITIONAL_DECOMPOSITION=apply`).
        // Rewrite the proposal's `ref_fields` so each component that clears the
        // per-component reuse gate points at the matched existing canonical, so
        // a parent that registers as new *references* the reused sub-schemas
        // instead of re-inlining them. `ref_fields` is not part of
        // `compute_identity_hash`, so the dedup/expansion seams below are
        // unaffected; this only changes the reference topology of a brand-new
        // canonical. Empty unless apply mode rewrote ≥1 component — in which
        // case the terminal new-registration return emits `Composed`.
        let composition_advice = self.apply_compositional_reuse(&mut schema);

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

        let candidate_set = self.generate_canonicalization_candidates(&schema, &schema_name)?;
        if let Some(conflict) = candidate_set.conflict {
            return Ok(SchemaAddOutcome::DescriptiveNameConflict(conflict.conflict));
        }

        if let Some(candidate) = rank_candidates(&candidate_set.candidates) {
            match self
                .canonicalization_gate_outcome(&schema, &schema_name, &candidate)
                .await
            {
                CanonicalizationGateOutcome::Merge => {
                    let cross_key = crate::state_expansion::is_cross_key_layout_expansion(
                        &schema,
                        &candidate.existing,
                    );

                    if candidate.seam == MatchSeam::IdentityHash && !cross_key {
                        tracing::info!(
                            target: "schema_service::schema",
                            "Schema '{}' already exists with same fields (active='{}') - returning existing",
                            schema_name,
                            candidate.existing_hash,
                        );
                        return Ok(SchemaAddOutcome::AlreadyExists(
                            candidate.existing,
                            mutation_mappers.clone(),
                        ));
                    }

                    // Same product, different keys: field-map + keep both identities.
                    // Never classic expand (supersedes / can rewrite hash_field).
                    if cross_key {
                        let shared = crate::state_expansion::shared_field_names(
                            &schema,
                            &candidate.existing,
                        );
                        crate::state_expansion::apply_shared_field_mappers(
                            &mut schema,
                            &candidate.existing_hash,
                            &shared,
                        );
                        tracing::info!(
                            target: "schema_service::schema",
                            existing = %candidate.existing_hash,
                            incoming_key = ?crate::state_expansion::key_layout_fingerprint(&schema),
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
                        Self::apply_field_renames(&mut schema, &rename_map, &mut mutation_mappers);
                        schema.dedup_fields();

                        return self
                            .expand_schema(
                                &mut schema,
                                &candidate.existing,
                                &candidate.existing_hash,
                                &candidate.target_descriptive_name,
                                &mutation_mappers,
                            )
                            .await;
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
                        return Ok(SchemaAddOutcome::DescriptiveNameConflict(
                            crate::types::DescriptiveNameConflict {
                                existing_canonical: candidate.existing_hash,
                                descriptive_name: desc_name,
                                reason: "identity hash matches the existing canonical (same name and \
                                         fields) but the purpose-statement gate rejected the merge; \
                                         rename the descriptive_name to reflect its distinct purpose"
                                    .to_string(),
                            },
                        ));
                    }
                    // Fall through to new registration plus final duplicate
                    // guard/de-collision below.
                }
            }
        }

        schema.name = schema_name.clone();

        // Final guard: re-check descriptive_name_index under write lock to prevent
        // race conditions where two concurrent add_schema calls with the same
        // descriptive_name both pass the read-only check and create duplicates.
        // Final guard: snapshot the descriptive_name_index to detect if a concurrent
        // add_schema call already registered this descriptive_name.
        // Race lookups are namespaced by `owner_app_id` so a concurrent
        // `fbrain/Project` registration races against another `fbrain/Project`,
        // never against a seed `Project`.
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
                    let desc_name_owned = schema.descriptive_name.clone().unwrap_or_default();
                    if existing_hash == schema_name {
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
                        if crate::state_expansion::is_cross_schema_type_expansion(
                            &schema, &existing,
                        ) {
                            tracing::warn!(
                                target: "schema_service::schema",
                                descriptive_name = %desc_name_owned,
                                existing_canonical = %existing_hash,
                                old_schema_type = ?existing.schema_type,
                                new_schema_type = ?schema.schema_type,
                                "Refusing cross-schema_type identity-hash collision in race-condition guard — returning DescriptiveNameConflict (409)",
                            );
                            return Ok(SchemaAddOutcome::DescriptiveNameConflict(
                                crate::types::DescriptiveNameConflict {
                                    existing_canonical: existing_hash,
                                    descriptive_name: desc_name_owned,
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
                        if crate::state_expansion::is_cross_key_layout_expansion(&schema, &existing)
                        {
                            let shared =
                                crate::state_expansion::shared_field_names(&schema, &existing);
                            crate::state_expansion::apply_shared_field_mappers(
                                &mut schema,
                                &existing_hash,
                                &shared,
                            );
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
                                desc_name_owned,
                                existing_hash,
                            );
                            return Ok(SchemaAddOutcome::AlreadyExists(existing, mutation_mappers));
                        }
                    }
                    if crate::state_expansion::is_cross_schema_type_expansion(&schema, &existing) {
                        tracing::warn!(
                            target: "schema_service::schema",
                            descriptive_name = %desc_name_owned,
                            existing_hash = %existing_hash,
                            old_schema_type = ?existing.schema_type,
                            new_schema_type = ?schema.schema_type,
                            "Refusing cross-schema_type expansion in race-condition path — returning DescriptiveNameConflict (409)",
                        );
                        return Ok(SchemaAddOutcome::DescriptiveNameConflict(
                            crate::types::DescriptiveNameConflict {
                                existing_canonical: existing_hash,
                                descriptive_name: desc_name_owned,
                                reason: format!(
                                    "incoming schema_type {:?} differs from existing {:?}; \
                                     cross-schema_type expansion would corrupt molecule reads",
                                    schema.schema_type, existing.schema_type,
                                ),
                            },
                        ));
                    }
                    let race_candidate = MatchCandidate::new(
                        MatchSeam::NameExact,
                        existing_hash.clone(),
                        existing.clone(),
                        desc_name_owned.clone(),
                        1.0,
                    );
                    let race_gate = self
                        .canonicalization_gate_outcome(&schema, &schema_name, &race_candidate)
                        .await;
                    if race_gate == CanonicalizationGateOutcome::Merge {
                        if crate::state_expansion::is_cross_key_layout_expansion(&schema, &existing)
                        {
                            let shared =
                                crate::state_expansion::shared_field_names(&schema, &existing);
                            crate::state_expansion::apply_shared_field_mappers(
                                &mut schema,
                                &existing_hash,
                                &shared,
                            );
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
                                    &mut schema,
                                    &existing,
                                    &existing_hash,
                                    &desc_name_owned,
                                    &mutation_mappers,
                                )
                                .await;
                        }
                    }
                    // Phase B dual-signal veto on the race-condition path:
                    // fall through to the final defense, which de-collides
                    // the descriptive_name so the genuinely-distinct
                    // same-name proposal registers as its OWN canonical
                    // instead of dead-ending at a 409. In shadow mode this
                    // branch is never taken — the helper returns true and
                    // the near-miss is recorded via the side effect.
                }
            }
        }

        // Final defense-in-depth: any path that reached this point with a
        // descriptive_name already bound to a different active canonical hash
        // would silently produce a duplicate row. The expansion paths above
        // already handle the legitimate same-hash / mergeable cases — anything
        // still standing here is a conflict.
        // The lookup is scoped by `owner_app_id` (app_identity v3.1, Lane B2b)
        // so `fbrain/Project` only conflicts with another active
        // `fbrain/Project`, not with a seed/legacy `Project`.
        let dup_conflict_hash: Option<String> = if let Some(ref desc_name) = schema.descriptive_name
        {
            // Namespace-aware lookup — see [`descriptive_name_key`] for the
            // (None, "a/b") vs (Some("a"), "b") aliasing that makes the
            // raw `index.get` answer namespace-impure.
            let preexisting = self
                .lookup_descriptive_name_in_namespace(schema.owner_app_id.as_deref(), desc_name)?;
            match preexisting {
                Some(existing_hash) if existing_hash != schema_name => {
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
            if let Some(existing) =
                self.find_decollided_idempotent_repost(&schema, &mutation_mappers)
            {
                return Ok(SchemaAddOutcome::AlreadyExists(
                    existing,
                    mutation_mappers.clone(),
                ));
            }

            if let Some(decollided) = self.decollide_user_descriptive_name(&schema) {
                schema.descriptive_name = Some(decollided);
                // The descriptive_name changed → the identity hash is stale.
                // Recompute it and rebind `schema_name`/`schema.name` so the
                // persist path below stores the de-collided canonical.
                schema.identity_hash = None;
                schema.compute_identity_hash();
                schema_name = schema
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
                return Ok(SchemaAddOutcome::DescriptiveNameConflict(
                    crate::types::DescriptiveNameConflict {
                        existing_canonical: existing_hash,
                        descriptive_name: desc_name,
                        reason: "an active schema with the same descriptive_name is already \
                                 registered with a different identity hash"
                            .to_string(),
                    },
                ));
            }
        }

        // Persist to storage backend
        self.persist_schema(&schema).await?;

        // Insert into in-memory cache and update descriptive_name index atomically
        // to prevent a window where the schema exists but isn't indexed.
        {
            let mut schemas = write_lock(&self.schemas, "schemas")?;
            schemas.insert(schema_name.clone(), schema.clone());
        }

        if let Some(ref desc_name) = schema.descriptive_name {
            // Index/embedding writes are keyed by the namespaced form
            // (app_identity v3.1, Lane B2b) so `fbrain/Project` and a seed
            // `Project` get distinct entries instead of overwriting each other.
            let index_key = descriptive_name_key(schema.owner_app_id.as_deref(), desc_name);
            // Update the index under a scope-bounded lock so the
            // write guard is guaranteed-dropped before the async
            // persist below (Rust's future-state-machine analysis
            // doesn't always accept explicit `drop()` for liveness,
            // and holding a `RwLockWriteGuard<HashMap<String, String>>`
            // across an await violates `Send`).
            {
                let mut index = write_lock(&self.descriptive_name_index, "descriptive_name_index")?;
                index.insert(index_key.clone(), schema_name.clone());
            }

            // Cache embedding for new descriptive_name AND persist it
            // to the S3 blob so the next cold start finds it without
            // recomputing. Best-effort persistence — a blob write
            // failure leaves the in-memory cache ahead of S3 (fine,
            // the blob is a cache) and `warm-embeddings` can fix it.
            if let Ok(vec) = self.embedder.embed_text(desc_name) {
                {
                    if let Ok(mut embeddings) = self.descriptive_name_embeddings.write() {
                        embeddings.insert(index_key.clone(), vec.clone());
                    }
                }
                let backend = self.storage.backend();
                if let Err(e) = backend
                    .save_descriptive_name_embedding(&schema_name, &vec)
                    .await
                {
                    tracing::warn!(
                    target: "schema_service::schema",
                                    "Failed to persist descriptive_name embedding for schema '{}' ('{}'): {} — \
                                     next cold start will miss this entry until warm-embeddings runs",
                                    schema_name,
                                    desc_name,
                                    e
                                );
                }
            }
        }

        // Register new fields as canonical for future schema proposals.
        // Fails if classification cannot be determined (no ANTHROPIC_API_KEY for new fields).
        self.register_canonical_fields(&schema).await?;

        // Propagate canonical field types, classifications, and interest categories to the schema
        self.apply_canonical_types(&mut schema);
        self.apply_canonical_classifications(&mut schema);
        self.apply_canonical_interest_categories(&mut schema);

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
