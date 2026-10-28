use std::collections::{HashMap, HashSet};

use crate::embedder::cosine_similarity;
use crate::lock_helpers::read_lock;
use schema_types::FieldValueType;
use schema_types::FoldDbError;
use schema_types::FoldDbResult;
use schema_types::Schema;

use super::state::SchemaServiceState;
use super::state_matching::FIELD_SIMILARITY_THRESHOLD;
use super::types::CanonicalField;

impl SchemaServiceState {
    /// Build embedding text from a field's description.
    /// Embeds the description only. The field name is intentionally absent from
    /// the embedding text because different sources use different names for the
    /// same concept (e.g. "summary" vs "subject"), and including the name adds
    /// noise that pushes cosine similarity below threshold. Field names are
    /// compared separately by name-similarity matching elsewhere in this module.
    pub(super) fn build_embedding_text(description: &str) -> String {
        description.to_string()
    }

    /// Build a description for a field from its schema context.
    /// Prefers AI-generated field_descriptions, falls back to field_classifications + descriptive_name.
    ///
    /// For AI-generated descriptions, returns the description as-is without appending
    /// the schema's descriptive_name. The "in {schema}" suffix is shared by ALL fields
    /// in a schema and inflates cross-field similarity, causing false positive matches
    /// (e.g. "subject" matching "calendar" because both end with "in Calendar Events").
    /// Only the fallback paths use the suffix since their descriptions are generic.
    pub(super) fn build_field_description(field_name: &str, schema: &Schema) -> String {
        // Prefer the AI-generated natural language description (already specific)
        if let Some(desc) = schema.field_descriptions.get(field_name) {
            return desc.clone();
        }

        // Fall back to classifications + descriptive_name for context
        let desc_name = schema.descriptive_name.as_deref().unwrap_or("unknown");
        let classifications = schema
            .field_classifications
            .get(field_name)
            .map(|c| c.join(", "))
            .unwrap_or_default();

        if classifications.is_empty() {
            format!("field in {desc_name}")
        } else {
            format!("{classifications} field in {desc_name}")
        }
    }

    /// Canonicalize incoming field names against the global canonical field registry.
    /// Returns a rename map: incoming_field -> canonical_field.
    /// A description that matches an anti-field sentence is rejected.
    pub(super) fn canonicalize_fields(
        &self,
        incoming_fields: &[String],
        schema: &Schema,
        mutation_mappers: &mut HashMap<String, String>,
    ) -> FoldDbResult<HashMap<String, String>> {
        // Reject before the rename. An existing canonical name is not a
        // pass: the match is the description, not the name.
        self.reject_non_semantic_fields(incoming_fields, schema)?;

        let Ok(canonical) = self.canonical_fields.read() else {
            return Ok(HashMap::new());
        };
        let Ok(embeddings) = self.canonical_field_embeddings.read() else {
            return Ok(HashMap::new());
        };

        if canonical.is_empty() {
            return Ok(HashMap::new());
        }

        let mut rename_map: HashMap<String, String> = HashMap::new();
        let mut claimed: HashSet<String> = HashSet::new();

        for incoming_field in incoming_fields {
            // Don't rename if it already IS a canonical field
            if canonical.contains_key(incoming_field) {
                continue;
            }

            let incoming_desc = Self::build_field_description(incoming_field, schema);
            let incoming_embed_text = Self::build_embedding_text(&incoming_desc);
            let Ok(incoming_embedding) = self.embedder.embed_text(&incoming_embed_text) else {
                continue;
            };

            // Find best canonical match
            let mut best: Option<(&str, f32)> = None;
            for (canon_name, canon_vec) in embeddings.iter() {
                if crate::field_name_policy::invalid_canonical_name(canon_name) {
                    continue;
                }
                let sim = cosine_similarity(&incoming_embedding, canon_vec);
                if sim >= FIELD_SIMILARITY_THRESHOLD
                    && best.is_none_or(|(_, best_sim)| sim > best_sim)
                {
                    best = Some((canon_name.as_str(), sim));
                }
            }

            let Some((matched_canonical, _)) = best else {
                continue;
            };

            // Bidirectional check: is this incoming field the best match
            // for the canonical field too?
            let Some(canon_vec) = embeddings.get(matched_canonical) else {
                continue;
            };
            let mut reverse_best: Option<(&str, f32)> = None;
            for candidate in incoming_fields {
                let cand_desc = Self::build_field_description(candidate, schema);
                let cand_embed_text = Self::build_embedding_text(&cand_desc);
                if let Ok(cand_vec) = self.embedder.embed_text(&cand_embed_text) {
                    let sim = cosine_similarity(canon_vec, &cand_vec);
                    if reverse_best.is_none_or(|(_, best_sim)| sim > best_sim) {
                        reverse_best = Some((candidate.as_str(), sim));
                    }
                }
            }

            let is_mutual =
                reverse_best.is_some_and(|(best_incoming, _)| best_incoming == incoming_field);
            if is_mutual && !claimed.contains(matched_canonical) {
                tracing::info!(
                target: "schema_service::schema",
                        "Canonical field rename: '{}' -> '{}'",
                        incoming_field,
                        matched_canonical
                    );
                rename_map.insert(incoming_field.clone(), matched_canonical.to_string());
                claimed.insert(matched_canonical.to_string());

                // Update mutation_mappers: incoming data key -> canonical field name
                if let Some(data_key) = mutation_mappers.remove(incoming_field) {
                    mutation_mappers.insert(data_key, matched_canonical.to_string());
                } else {
                    mutation_mappers.insert(incoming_field.clone(), matched_canonical.to_string());
                }
            }
        }

        Ok(rename_map)
    }

    /// Propagate one piece of canonical-registry metadata into a schema's
    /// per-field map.
    ///
    /// Shared core of [`apply_canonical_types`](Self::apply_canonical_types),
    /// [`apply_canonical_classifications`](Self::apply_canonical_classifications),
    /// and [`apply_canonical_interest_categories`](Self::apply_canonical_interest_categories):
    /// for every schema field that does not already have an entry in
    /// `target`, look the field up in `canonical` and insert whatever
    /// `extract` pulls off the [`CanonicalField`] (skipping fields where
    /// `extract` yields `None`).
    fn apply_canonical_field_map<T>(
        canonical: &HashMap<String, CanonicalField>,
        field_names: &[String],
        target: &mut HashMap<String, T>,
        extract: impl Fn(&CanonicalField) -> Option<T>,
    ) {
        for field_name in field_names {
            // Skip if the schema already has a value declared for this field.
            if target.contains_key(field_name) {
                continue;
            }
            if let Some(canonical) = canonical.get(field_name) {
                if let Some(value) = extract(canonical) {
                    target.insert(field_name.clone(), value);
                }
            }
        }
    }

    /// Populate a schema's `field_types` map from the canonical field registry.
    /// Validate the resulting definition before persistence or publication.
    pub(super) fn apply_canonical_types(&self, schema: &mut Schema) -> FoldDbResult<()> {
        let fields = read_lock(&self.canonical_fields, "canonical_fields")?;
        let field_names: Vec<String> = schema.fields.clone().unwrap_or_default();
        Self::apply_canonical_field_map(&fields, &field_names, &mut schema.field_types, |c| {
            (c.field_type != FieldValueType::Any).then(|| c.field_type.clone())
        });
        // Newly introduced reference fields do not exist in the registry yet.
        for field_name in field_names {
            if let Some(target) = schema.ref_fields.get(&field_name) {
                schema
                    .field_types
                    .entry(field_name)
                    .or_insert_with(|| FieldValueType::SchemaRef(target.clone()));
            }
        }
        schema.validate_definition().map_err(FoldDbError::Config)
    }

    /// Populate a schema's `field_data_classifications` map from the canonical field registry.
    /// Called after canonicalization to propagate classifications from the registry to the schema.
    /// Only fills in fields that don't already have a classification declared.
    pub(super) fn apply_canonical_classifications(&self, schema: &mut Schema) {
        let Ok(fields) = self.canonical_fields.read() else {
            return;
        };
        let field_names: Vec<String> = schema.fields.clone().unwrap_or_default();
        Self::apply_canonical_field_map(
            &fields,
            &field_names,
            &mut schema.field_data_classifications,
            |c| c.classification.clone(),
        );
    }

    /// Populate a schema's `field_interest_categories` map from the canonical field registry.
    /// Called after canonicalization to propagate interest categories from the registry to the schema.
    /// Only fills in fields that don't already have an interest category declared.
    pub(super) fn apply_canonical_interest_categories(&self, schema: &mut Schema) {
        let Ok(fields) = self.canonical_fields.read() else {
            return;
        };
        let field_names: Vec<String> = schema.fields.clone().unwrap_or_default();
        Self::apply_canonical_field_map(
            &fields,
            &field_names,
            &mut schema.field_interest_categories,
            |c| c.interest_category.clone(),
        );
    }
}
