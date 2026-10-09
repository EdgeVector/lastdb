use std::collections::{HashMap, HashSet};

use crate::embedder::cosine_similarity;
use crate::lock_helpers::{read_lock, write_lock};
use schema_types::Schema;
use schema_types::{FoldDbError, FoldDbResult};

use super::state::SchemaServiceState;
use super::state_matching::SEMANTIC_RENAME_THRESHOLD;
use super::types::SchemaAddOutcome;

/// Whether expansion across `schema_type` would be unsafe.
///
/// Historically each `DeclarativeSchemaType` stored molecules in a different
/// on-disk shape, so cross-type expansion corrupted reads. After
/// `north-star-one-molecule-kind` (storage unify + module delete), keyed
/// molecules share one layout (`MoleculeHashRange` with optional empty key
/// components). Cross-type expansion is therefore safe and this guard is a
/// permanent no-op — kept so call sites stay explicit and searchable.
#[must_use]
pub(super) fn is_cross_schema_type_expansion(_incoming: &Schema, _existing: &Schema) -> bool {
    false
}

/// Normalized key layout for expand / multi-key decisions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyLayoutFingerprint {
    pub hash_field: Option<String>,
    pub range_field: Option<String>,
}

/// Extract partition key layout from a schema (empty strings → None).
#[must_use]
pub fn key_layout_fingerprint(schema: &Schema) -> KeyLayoutFingerprint {
    let norm = |s: Option<&str>| {
        s.map(str::trim)
            .filter(|x| !x.is_empty())
            .map(str::to_string)
    };
    match schema.key.as_ref() {
        Some(k) => KeyLayoutFingerprint {
            hash_field: norm(k.hash_field.as_deref()),
            range_field: norm(k.range_field.as_deref()),
        },
        None => KeyLayoutFingerprint {
            hash_field: None,
            range_field: None,
        },
    }
}

/// True when incoming and existing use **different lookup keys**.
///
/// Same-product multi-key indexes (e.g. BoardCards `board` vs MilestoneCards
/// `milestone`) must **not** go through classic `expand_schema` (which
/// supersedes the old identity and can rewrite the live key). They register
/// as a multi-key sibling with field mappers + background tip reindex instead.
///
/// Preference: `preference-schema-expand-same-product-different-keys`.
#[must_use]
pub fn is_cross_key_layout(incoming: &Schema, existing: &Schema) -> bool {
    key_layout_fingerprint(incoming) != key_layout_fingerprint(existing)
}

/// Alias used at expand call sites (symmetric with `is_cross_schema_type_expansion`).
#[must_use]
pub(super) fn is_cross_key_layout_expansion(incoming: &Schema, existing: &Schema) -> bool {
    is_cross_key_layout(incoming, existing)
}

/// Shared field names present on both schemas (for field_mapper linkage).
#[must_use]
pub fn shared_field_names(incoming: &Schema, existing: &Schema) -> Vec<String> {
    let a: HashSet<String> = incoming
        .fields
        .as_ref()
        .map(|f| f.iter().cloned().collect())
        .unwrap_or_default();
    let b: HashSet<String> = existing
        .fields
        .as_ref()
        .map(|f| f.iter().cloned().collect())
        .unwrap_or_default();
    let mut shared: Vec<String> = a.intersection(&b).cloned().collect();
    shared.sort();
    shared
}

/// Attach field mappers for every shared field so payload molecules stay shared
/// while the incoming schema keeps its own key layout (multi-key sibling).
pub fn apply_shared_field_mappers(
    incoming: &mut Schema,
    existing_hash: &str,
    shared_fields: &[String],
) {
    use schema_types::FieldMapper;
    let mut mappers = incoming.field_mappers().cloned().unwrap_or_default();
    for field in shared_fields {
        mappers
            .entry(field.clone())
            .or_insert_with(|| FieldMapper::new(existing_hash.to_string(), field.clone()));
    }
    if !mappers.is_empty() {
        incoming.field_mappers = Some(mappers);
    }
}

impl SchemaServiceState {
    /// If a schema has been superseded by an expanded version, resolve to the
    /// active schema. Returns `None` if no redirection is needed.
    pub(super) fn resolve_active_schema(
        &self,
        existing_schema: &Schema,
        schema_name: &str,
        schemas: &HashMap<String, Schema>,
    ) -> Option<(Schema, String)> {
        let desc_name = existing_schema.descriptive_name.as_ref()?;
        // Index keys are namespaced by `owner_app_id` (app_identity v3.1,
        // Lane B2b), so an active-schema resolution for a same-named seed
        // and `fbrain/<X>` looks up the right entry.
        let key =
            super::state::descriptive_name_key(existing_schema.owner_app_id.as_deref(), desc_name);
        let index = match read_lock(&self.descriptive_name_index, "descriptive_name_index") {
            Ok(idx) => idx,
            Err(e) => {
                tracing::warn!(
                    target: "schema_service::schema",
                    "{} — falling back to original schema",
                    e
                );
                return None;
            }
        };
        let current_hash = index.get(&key)?;
        if *current_hash == schema_name {
            return None;
        }
        let active_schema = schemas.get(current_hash)?;
        // Namespace re-verification: `descriptive_name_key` is not injective
        // on `(Option<&str>, &str)` — `(None, "kanban/Tasks")` and
        // `(Some("kanban"), "Tasks")` both encode to `"kanban/Tasks"`. When a
        // same-keyed sibling-namespace schema overwrote the index slot, the
        // raw `index.get` answer points at an entry from a disjoint
        // namespace. Supersession is per-namespace by definition (it's the
        // tail of an `expand_schema` chain within one namespace), so a
        // cross-namespace "active" entry is never a real redirection. Drop
        // it and report no redirection. Same shape as PR #466 / #487 at a
        // sibling site.
        fn normalize(s: Option<&str>) -> Option<&str> {
            s.filter(|x| !x.is_empty())
        }
        if normalize(active_schema.owner_app_id.as_deref())
            != normalize(existing_schema.owner_app_id.as_deref())
        {
            return None;
        }
        tracing::info!(
            target: "schema_service::schema",
            "Schema '{}' was superseded by '{}' — checking active schema",
            schema_name,
            current_hash
        );
        Some((active_schema.clone(), current_hash.clone()))
    }

    /// Find semantic field name matches between incoming and existing schemas.
    ///
    /// For fields in the incoming schema that don't have a literal match in the
    /// existing schema, uses context-enriched embeddings to detect synonyms
    /// (e.g., "creator" ≈ "artist" in an artwork context).
    ///
    /// Returns a map: incoming_field_name → existing_field_name (canonical).
    pub(super) fn semantic_field_rename_map(
        &self,
        incoming_fields: &[String],
        existing_fields: &[String],
        descriptive_name: &str,
        incoming_descriptions: &HashMap<String, String>,
        existing_descriptions: &HashMap<String, String>,
    ) -> HashMap<String, String> {
        let existing_set: HashSet<&String> = existing_fields.iter().collect();
        let mut rename_map: HashMap<String, String> = HashMap::new();
        // Track which existing fields have been claimed to avoid many-to-one mapping
        let mut claimed: HashSet<String> = HashSet::new();

        for incoming_field in incoming_fields {
            // Skip fields that already have a literal match
            if existing_set.contains(incoming_field) {
                continue;
            }

            let Some(incoming_emb) = self.get_field_embedding(
                incoming_field,
                descriptive_name,
                incoming_descriptions
                    .get(incoming_field.as_str())
                    .map(std::string::String::as_str),
            ) else {
                continue;
            };

            let mut best: Option<(&str, f32)> = None;
            for existing_field in existing_fields {
                if claimed.contains(existing_field) {
                    continue;
                }
                let Some(existing_emb) = self.get_field_embedding(
                    existing_field,
                    descriptive_name,
                    existing_descriptions
                        .get(existing_field.as_str())
                        .map(std::string::String::as_str),
                ) else {
                    continue;
                };
                let sim = cosine_similarity(&incoming_emb, &existing_emb);
                if sim >= SEMANTIC_RENAME_THRESHOLD && best.as_ref().is_none_or(|(_, s)| sim > *s) {
                    best = Some((existing_field.as_str(), sim));
                }
            }

            if let Some((matched_field, similarity)) = best {
                // Bidirectional check: verify the existing field's best match among
                // all incoming fields is also this incoming field. This prevents false
                // positives like "medium"→"artist" when "creator"→"artist" is stronger.
                let existing_emb = self
                    .get_field_embedding(
                        matched_field,
                        descriptive_name,
                        existing_descriptions
                            .get(matched_field)
                            .map(std::string::String::as_str),
                    )
                    .unwrap();
                let mut reverse_best: Option<(&str, f32)> = None;
                for candidate in incoming_fields {
                    if existing_set.contains(candidate) {
                        continue; // skip literal matches
                    }
                    let Some(candidate_emb) = self.get_field_embedding(
                        candidate,
                        descriptive_name,
                        incoming_descriptions
                            .get(candidate.as_str())
                            .map(std::string::String::as_str),
                    ) else {
                        continue;
                    };
                    let sim = cosine_similarity(&existing_emb, &candidate_emb);
                    if reverse_best.as_ref().is_none_or(|(_, s)| sim > *s) {
                        reverse_best = Some((candidate.as_str(), sim));
                    }
                }

                let is_mutual =
                    reverse_best.is_some_and(|(best_incoming, _)| best_incoming == incoming_field);

                if is_mutual {
                    tracing::info!(
                    target: "schema_service::schema",
                                "Semantic field rename: '{}' → '{}' (similarity: {:.3}, context: '{}')",
                                incoming_field,
                                matched_field,
                                similarity,
                                descriptive_name
                            );
                    rename_map.insert(incoming_field.clone(), matched_field.to_string());
                    claimed.insert(matched_field.to_string());
                } else {
                    tracing::info!(
                    target: "schema_service::schema",
                                "Rejected non-mutual match: '{}' → '{}' (similarity: {:.3}, but existing field's best match is '{}')",
                                incoming_field,
                                matched_field,
                                similarity,
                                reverse_best.map_or("none", |(f, _)| f),
                            );
                }
            } else {
                tracing::info!(
                target: "schema_service::schema",
                        "No semantic field match for '{}' in context '{}' — treating as new field",
                        incoming_field,
                        descriptive_name
                    );
            }
        }

        rename_map
    }

    /// Apply field renames to a schema: rename fields, update classifications,
    /// mutation_mappers, ref_fields, field_types, and the Range/Hash
    /// `KeyConfig` to use canonical names.
    pub(super) fn apply_field_renames(
        schema: &mut Schema,
        rename_map: &HashMap<String, String>,
        mutation_mappers: &mut HashMap<String, String>,
    ) {
        if rename_map.is_empty() {
            return;
        }

        // Rename in fields list
        if let Some(ref mut fields) = schema.fields {
            for field in fields.iter_mut() {
                if let Some(canonical) = rename_map.get(field) {
                    *field = canonical.clone();
                }
            }
        }

        // Rename in field_classifications and field_data_classifications
        for (old_name, canonical) in rename_map {
            if let Some(classifications) = schema.field_classifications.remove(old_name) {
                schema
                    .field_classifications
                    .entry(canonical.clone())
                    .or_insert(classifications);
            }
            if let Some(data_classification) = schema.field_data_classifications.remove(old_name) {
                schema
                    .field_data_classifications
                    .entry(canonical.clone())
                    .or_insert(data_classification);
            }
            if let Some(interest_category) = schema.field_interest_categories.remove(old_name) {
                schema
                    .field_interest_categories
                    .entry(canonical.clone())
                    .or_insert(interest_category);
            }
            // field_descriptions must migrate too — the pre-rename validation at
            // state.rs guarantees a description for every entry in `fields`, and
            // downstream embeddings (`semantic_field_rename_map`,
            // `canonicalize_fields`) look the description up by the renamed key.
            // Leaving the old key behind silently drops the description from
            // future matches and persists a stale entry for a field that no
            // longer exists in `fields`.
            if let Some(description) = schema.field_descriptions.remove(old_name) {
                schema
                    .field_descriptions
                    .entry(canonical.clone())
                    .or_insert(description);
            }
            // ref_fields migrates with the rename. The doc-comment on this
            // function already promises this, but the original implementation
            // only moved the classification-style maps. `state_fields::
            // infer_field_type` reads `ref_fields` by field name, so a stale
            // key under `old_name` makes the renamed field look like a plain
            // value field (`FieldValueType::Any`) and the schema persists with
            // a ref entry pointing at a field that no longer exists in
            // `fields`.
            if let Some(target) = schema.ref_fields.remove(old_name) {
                schema.ref_fields.entry(canonical.clone()).or_insert(target);
            }
            // field_types must migrate too. `state_fields::infer_field_type`
            // reads `schema.field_types` by field name to decide the type of
            // every new canonical-registry entry (and `Schema::get_field_type`
            // reads it on every later type check). Leaving the declared type
            // under `old_name` means the renamed field falls back to
            // `FieldValueType::Any` — so `register_canonical_fields` then seeds
            // the canonical registry with `Any` for what the caller explicitly
            // declared as e.g. `String` or `Integer`. The stale entry under
            // `old_name` also persists a type for a field absent from `fields`
            // and (because `apply_canonical_types` only fills in *missing*
            // entries) lets a later canonical lookup silently overwrite the
            // caller's type with whatever the registry happens to hold.
            if let Some(ty) = schema.field_types.remove(old_name) {
                schema.field_types.entry(canonical.clone()).or_insert(ty);
            }
            // field_hashes / field_versions are catalog identity for local
            // map-vs-protein coherence. Migrate with renames so a post-hash
            // canonicalize does not drop the hash under the old name.
            if let Some(h) = schema.field_hashes.remove(old_name) {
                schema.field_hashes.entry(canonical.clone()).or_insert(h);
            }
            if let Some(ver) = schema.field_versions.remove(old_name) {
                schema
                    .field_versions
                    .entry(canonical.clone())
                    .or_insert(ver);
            }
            // Add mutation_mapper: old_name → canonical so AI mutations still work
            mutation_mappers
                .entry(old_name.clone())
                .or_insert_with(|| canonical.clone());
        }

        // schema.key (Range/Hash/HashRange `KeyConfig`) names data fields by
        // string. `KeyValue::from_mutation` looks each mutation up by these
        // names, so a stale entry pointing at a no-longer-declared field
        // resolves to `None` on every mutation — range writes silently land in
        // an empty-key slot, range queries see nothing. Rewriting in place
        // keeps the KeyConfig consistent with the renamed `fields`.
        if let Some(ref mut key) = schema.key {
            if let Some(ref mut hash_field) = key.hash_field {
                if let Some(canonical) = rename_map.get(hash_field) {
                    *hash_field = canonical.clone();
                }
            }
            if let Some(ref mut range_field) = key.range_field {
                if let Some(canonical) = rename_map.get(range_field) {
                    *range_field = canonical.clone();
                }
            }
        }
    }

    /// Expand an incoming schema to be a superset of an existing schema.
    ///
    /// Merges fields, dual-stamps FieldMappers (per old field) and one
    /// RecordMapper (predecessor identity), merges classifications and
    /// ref_fields, recomputes identity_hash, persists, and updates caches.
    /// Does not clear FieldMappers. Copies `molecule_uuid` only when the
    /// predecessor already has R.
    ///
    /// Returns `SchemaAddOutcome::Expanded` on success, or `AlreadyExists` if
    /// the incoming fields are a subset of the existing.
    pub(super) async fn expand_schema(
        &self,
        schema: &mut Schema,
        existing: &Schema,
        old_name: &str,
        desc_name: &str,
        mutation_mappers: &HashMap<String, String>,
    ) -> FoldDbResult<SchemaAddOutcome> {
        // Never classic-expand across different key layouts (would supersede
        // and can rewrite hash_field on the live pin — BoardCards incident).
        if is_cross_key_layout(schema, existing) {
            return Err(FoldDbError::Config(format!(
                "refusing expand_schema across different key layouts \
                 (incoming {:?}/{:?} vs existing {:?}/{:?}); use multi-key sibling \
                 registration with field mappers + tip reindex instead",
                key_layout_fingerprint(schema).hash_field,
                key_layout_fingerprint(schema).range_field,
                key_layout_fingerprint(existing).hash_field,
                key_layout_fingerprint(existing).range_field,
            )));
        }

        let existing_fields = existing.fields.clone().unwrap_or_default();
        let existing_set: HashSet<String> = existing_fields.iter().cloned().collect();
        let new_field_set: HashSet<String> = schema
            .fields
            .as_ref()
            .map(|nf| nf.iter().cloned().collect())
            .unwrap_or_default();

        // If the new schema's fields are a subset of the existing, reuse existing
        if new_field_set.is_subset(&existing_set) {
            tracing::info!(
            target: "schema_service::schema",
                "New schema is a subset of existing '{}' (descriptive_name='{}') — reusing existing",
                old_name,
                desc_name
            );
            return Ok(SchemaAddOutcome::AlreadyExists(
                existing.clone(),
                mutation_mappers.clone(),
            ));
        }

        tracing::info!(
            target: "schema_service::schema",
            "Expanding schema (descriptive_name='{}') — merging fields from old hash '{}'",
            desc_name,
            old_name
        );

        // Merge to superset: existing fields + new-only fields
        let new_fields_to_add: Vec<String> =
            new_field_set.difference(&existing_set).cloned().collect();
        let mut merged_fields = existing_fields.clone();
        merged_fields.extend(new_fields_to_add);
        if merged_fields
            .iter()
            .any(|f| f == schema_types::RECORD_SENTINEL)
        {
            return Err(FoldDbError::Config(format!(
                "refusing expand_schema: field name {:?} is reserved as RECORD_SENTINEL",
                schema_types::RECORD_SENTINEL
            )));
        }
        schema.fields = Some(merged_fields);

        // Dual-stamp: keep N FieldMappers (old Mini copies field UUIDs) AND
        // one RecordMapper (new Mini copies R once the predecessor has one).
        // Never set field_mappers = None here — that hides historical rows
        // for a Mini that ignores record_mapper.
        use schema_types::{FieldMapper, RecordMapper};
        let mut mappers: HashMap<String, FieldMapper> =
            schema.field_mappers().cloned().unwrap_or_default();
        for field in &existing_fields {
            mappers
                .entry(field.clone())
                .or_insert_with(|| FieldMapper::new(old_name.to_string(), field.clone()));
        }
        schema.field_mappers = Some(mappers);
        schema.record_mapper = Some(RecordMapper::new(old_name.to_string()));
        // Copy R only when the predecessor already holds a record molecule.
        // Today's catalogs have field_molecule_uuids, not molecule_uuid.
        if schema.molecule_uuid.is_none() {
            schema.molecule_uuid = existing.molecule_uuid.clone();
        }
        schema.field_molecule_uuids = None;

        // Merge field_classifications (keep existing, add new)
        for (field, classifications) in &existing.field_classifications {
            schema
                .field_classifications
                .entry(field.clone())
                .or_insert_with(|| classifications.clone());
        }

        // Merge field_data_classifications (keep existing, add new)
        for (field, classification) in &existing.field_data_classifications {
            schema
                .field_data_classifications
                .entry(field.clone())
                .or_insert_with(|| classification.clone());
        }

        // Merge field_interest_categories (keep existing, add new)
        for (field, category) in &existing.field_interest_categories {
            schema
                .field_interest_categories
                .entry(field.clone())
                .or_insert_with(|| category.clone());
        }

        // Merge ref_fields (keep existing references)
        for (field, target) in &existing.ref_fields {
            schema
                .ref_fields
                .entry(field.clone())
                .or_insert_with(|| target.clone());
        }

        // Merge field_descriptions and field_types from existing into the
        // expanded schema for any field the incoming schema didn't supply
        // itself.
        //
        // Without this merge the existing-only fields (which get added to
        // `schema.fields` above) carry no entry in these maps after expansion
        // and the same class of silent breakage that #439 / #457 fixed for
        // `apply_field_renames` shows up here too:
        //
        //  - `field_descriptions` missing breaks the
        //    `add_schema`-time invariant that every entry in `fields` has a
        //    description, and downstream `build_field_description` falls
        //    back to a low-signal stub that lowers semantic-match quality.
        //  - `field_types` missing makes `Schema::get_field_type` fall back
        //    to `Any` whenever the canonical registry doesn't already
        //    carry the field (e.g. legacy entries with `field_type == Any`),
        //    losing the type the existing schema's caller had declared.
        for (field, description) in &existing.field_descriptions {
            schema
                .field_descriptions
                .entry(field.clone())
                .or_insert_with(|| description.clone());
        }
        for (field, ty) in &existing.field_types {
            schema
                .field_types
                .entry(field.clone())
                .or_insert_with(|| ty.clone());
        }

        // Recompute identity hash with merged fields.
        // The expanded schema is a NEW schema — its name is the identity hash
        // (derived from schema name + fields). The old schema keeps its name and
        // gets blocked/superseded. Field mappers point back to the old schema.
        schema.compute_identity_hash();
        let new_hash = schema
            .get_identity_hash()
            .ok_or_else(|| {
                FoldDbError::Config("Failed to compute merged identity_hash".to_string())
            })?
            .clone();
        schema.name = new_hash.clone();
        let expanded_name = schema.name.clone();

        // Persist expanded schema
        self.persist_schema(schema).await?;

        // Mark the old schema as superseded in memory and grab a clone for persistence
        let old_clone = {
            let mut schemas = write_lock(&self.schemas, "schemas")?;
            let clone = if let Some(old_schema) = schemas.get_mut(old_name) {
                old_schema.superseded_by = Some(expanded_name.clone());
                Some(old_schema.clone())
            } else {
                None
            };
            schemas.insert(expanded_name.clone(), schema.clone());
            clone
        };

        // Persist the superseded marker (outside the lock)
        if let Some(old_schema) = old_clone {
            self.persist_schema(&old_schema).await?;
        }

        // Update descriptive_name index to point to expanded schema. The
        // key is namespaced by `owner_app_id` (app_identity v3.1, Lane B2b)
        // so the expansion target replaces only the matching same-owner
        // entry, leaving any same-named seed/legacy entry intact.
        {
            let key = super::state::descriptive_name_key(schema.owner_app_id.as_deref(), desc_name);
            let mut index = write_lock(&self.descriptive_name_index, "descriptive_name_index")?;
            index.insert(key, expanded_name);
        }

        // Register new fields as canonical for future schema proposals.
        // Fails if classification cannot be determined (no ANTHROPIC_API_KEY for new fields).
        self.register_canonical_fields(schema).await?;

        // Propagate canonical field types, classifications, and interest categories to the expanded schema
        self.apply_canonical_types(schema);
        self.apply_canonical_classifications(schema);
        self.apply_canonical_interest_categories(schema);

        tracing::info!(
            target: "schema_service::schema",
            "Schema expanded: old='{}' (blocked) -> new='{}' (descriptive_name='{}')",
            old_name,
            schema.name,
            desc_name
        );

        Ok(SchemaAddOutcome::Expanded(
            old_name.to_string(),
            schema.clone(),
            mutation_mappers.clone(),
        ))
    }
}
