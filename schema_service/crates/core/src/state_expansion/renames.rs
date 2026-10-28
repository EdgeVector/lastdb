//! Semantic field renames: detect synonym fields and rewrite a schema to the
//! canonical names.

use std::collections::{HashMap, HashSet};

use crate::embedder::cosine_similarity;
use schema_types::Schema;

use super::super::state::SchemaServiceState;
use super::super::state_matching::SEMANTIC_RENAME_THRESHOLD;

/// Names and descriptions that give a field name its embedding context.
struct EmbeddingContext<'a> {
    descriptive_name: &'a str,
    incoming_descriptions: &'a HashMap<String, String>,
    existing_descriptions: &'a HashMap<String, String>,
}

/// Move `old` to `canonical` in a per-field map; a canonical entry already
/// present wins.
fn migrate_entry<V>(map: &mut HashMap<String, V>, old: &str, canonical: &str) {
    if let Some(value) = map.remove(old) {
        map.entry(canonical.to_string()).or_insert(value);
    }
}

impl SchemaServiceState {
    fn field_embedding(
        &self,
        field: &str,
        descriptive_name: &str,
        descriptions: &HashMap<String, String>,
    ) -> Option<Vec<f32>> {
        self.get_field_embedding(
            field,
            descriptive_name,
            descriptions.get(field).map(std::string::String::as_str),
        )
    }

    /// Unclaimed existing field most similar to `incoming_emb` at or above the
    /// rename threshold, with its similarity and embedding.
    fn best_existing_match<'a>(
        &self,
        incoming_emb: &[f32],
        existing_fields: &'a [String],
        claimed: &HashSet<String>,
        ctx: &EmbeddingContext<'_>,
    ) -> Option<(&'a str, f32, Vec<f32>)> {
        let mut best: Option<(&str, f32, Vec<f32>)> = None;
        for existing_field in existing_fields {
            if claimed.contains(existing_field)
                || crate::field_name_policy::uses_app_abbreviation(existing_field)
            {
                continue;
            }
            let Some(existing_emb) = self.field_embedding(
                existing_field,
                ctx.descriptive_name,
                ctx.existing_descriptions,
            ) else {
                continue;
            };
            let sim = cosine_similarity(incoming_emb, &existing_emb);
            if sim >= SEMANTIC_RENAME_THRESHOLD && best.as_ref().is_none_or(|(_, s, _)| sim > *s) {
                best = Some((existing_field.as_str(), sim, existing_emb));
            }
        }
        best
    }

    /// The non-literal incoming field most similar to `existing_emb`.
    fn best_incoming_for<'a>(
        &self,
        existing_emb: &[f32],
        incoming_fields: &'a [String],
        existing_set: &HashSet<&String>,
        ctx: &EmbeddingContext<'_>,
    ) -> Option<&'a str> {
        let mut reverse_best: Option<(&str, f32)> = None;
        for candidate in incoming_fields {
            if existing_set.contains(candidate) {
                continue; // skip literal matches
            }
            let Some(candidate_emb) =
                self.field_embedding(candidate, ctx.descriptive_name, ctx.incoming_descriptions)
            else {
                continue;
            };
            let sim = cosine_similarity(existing_emb, &candidate_emb);
            if reverse_best.as_ref().is_none_or(|(_, s)| sim > *s) {
                reverse_best = Some((candidate.as_str(), sim));
            }
        }
        reverse_best.map(|(name, _)| name)
    }

    /// Find semantic field name matches between incoming and existing schemas.
    ///
    /// For fields in the incoming schema that don't have a literal match in the
    /// existing schema, uses context-enriched embeddings to detect synonyms
    /// (e.g., "creator" ≈ "artist" in an artwork context).
    ///
    /// Returns a map: incoming_field_name → existing_field_name (canonical).
    pub(crate) fn semantic_field_rename_map(
        &self,
        incoming_fields: &[String],
        existing_fields: &[String],
        descriptive_name: &str,
        incoming_descriptions: &HashMap<String, String>,
        existing_descriptions: &HashMap<String, String>,
    ) -> HashMap<String, String> {
        let ctx = EmbeddingContext {
            descriptive_name,
            incoming_descriptions,
            existing_descriptions,
        };
        let existing_set: HashSet<&String> = existing_fields.iter().collect();
        let mut rename_map: HashMap<String, String> = HashMap::new();
        // Track which existing fields have been claimed to avoid many-to-one mapping
        let mut claimed: HashSet<String> = HashSet::new();

        for incoming_field in incoming_fields {
            // Skip fields that already have a literal match
            if existing_set.contains(incoming_field) {
                continue;
            }
            let Some(incoming_emb) =
                self.field_embedding(incoming_field, descriptive_name, incoming_descriptions)
            else {
                continue;
            };
            let Some((matched_field, similarity, existing_emb)) =
                self.best_existing_match(&incoming_emb, existing_fields, &claimed, &ctx)
            else {
                tracing::info!(
                    target: "schema_service::schema",
                    "No semantic field match for '{}' in context '{}' — treating as new field",
                    incoming_field,
                    descriptive_name
                );
                continue;
            };

            // Bidirectional check: verify the existing field's best match among
            // all incoming fields is also this incoming field. This prevents false
            // positives like "medium"→"artist" when "creator"→"artist" is stronger.
            let reverse_best =
                self.best_incoming_for(&existing_emb, incoming_fields, &existing_set, &ctx);
            if reverse_best == Some(incoming_field.as_str()) {
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
                    reverse_best.unwrap_or("none"),
                );
            }
        }

        rename_map
    }

    /// Apply field renames to a schema: rename fields, update classifications,
    /// mutation_mappers, ref_fields, field_types, and the Range/Hash
    /// `KeyConfig` to use canonical names.
    pub(crate) fn apply_field_renames(
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

        for (old_name, canonical) in rename_map {
            migrate_field_metadata(schema, old_name, canonical);
            // Add mutation_mapper: old_name → canonical so AI mutations still work
            mutation_mappers
                .entry(old_name.clone())
                .or_insert_with(|| canonical.clone());
        }

        rename_key_fields(schema, rename_map);
    }
}

/// Migrate every per-field map from `old_name` to `canonical`.
fn migrate_field_metadata(schema: &mut Schema, old_name: &str, canonical: &str) {
    migrate_entry(&mut schema.field_classifications, old_name, canonical);
    migrate_entry(&mut schema.field_data_classifications, old_name, canonical);
    migrate_entry(&mut schema.field_interest_categories, old_name, canonical);
    // field_descriptions must migrate too — the pre-rename validation at
    // state.rs guarantees a description for every entry in `fields`, and
    // downstream embeddings (`semantic_field_rename_map`,
    // `canonicalize_fields`) look the description up by the renamed key.
    // Leaving the old key behind silently drops the description from
    // future matches and persists a stale entry for a field that no
    // longer exists in `fields`.
    migrate_entry(&mut schema.field_descriptions, old_name, canonical);
    // ref_fields migrates with the rename. `state_fields::infer_field_type`
    // reads `ref_fields` by field name, so a stale key under `old_name` makes
    // the renamed field look like a plain value field (`FieldValueType::Any`)
    // and the schema persists with a ref entry pointing at a field that no
    // longer exists in `fields`.
    migrate_entry(&mut schema.ref_fields, old_name, canonical);
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
    migrate_entry(&mut schema.field_types, old_name, canonical);
    // field_hashes / field_versions are catalog identity for local
    // map-vs-protein coherence. Migrate with renames so a post-hash
    // canonicalize does not drop the hash under the old name.
    migrate_entry(&mut schema.field_hashes, old_name, canonical);
    migrate_entry(&mut schema.field_versions, old_name, canonical);
}

/// schema.key (Range/Hash/HashRange `KeyConfig`) names data fields by
/// string. `KeyValue::from_mutation` looks each mutation up by these
/// names, so a stale entry pointing at a no-longer-declared field
/// resolves to `None` on every mutation — range writes silently land in
/// an empty-key slot, range queries see nothing. Rewriting in place
/// keeps the KeyConfig consistent with the renamed `fields`.
fn rename_key_fields(schema: &mut Schema, rename_map: &HashMap<String, String>) {
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
