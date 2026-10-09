use std::collections::{HashMap, HashSet};

use crate::embedder::cosine_similarity;
use crate::lock_helpers::{read_lock, write_lock};
use schema_types::FieldValueType;
use schema_types::FoldDbResult;
use schema_types::Schema;
use schema_types::{DataClassification, INTERNAL};

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

    /// Infer the FieldValueType for a field from schema metadata.
    /// Uses ref_fields for schema references, field_types if declared,
    /// and falls back to Any.
    fn infer_field_type(field_name: &str, schema: &Schema) -> FieldValueType {
        // If the schema already has a declared type, use it
        if let Some(ft) = schema.field_types.get(field_name) {
            return ft.clone();
        }

        // If it's a ref_field, type is SchemaRef
        if let Some(ref_schema) = schema.ref_fields.get(field_name) {
            return FieldValueType::SchemaRef(ref_schema.clone());
        }

        // No type info available
        FieldValueType::Any
    }

    /// Register new fields from a schema as canonical.
    /// Only adds fields that don't already exist in the registry.
    ///
    /// Classification is **not** LLM-inferred. For each new field:
    /// 1. Use caller-provided `field_data_classifications` if present
    /// 2. Otherwise default to internal/general (`sensitivity=1`, `general`)
    ///
    /// Interest categories are only carried when already present on the
    /// schema declaration / overlay; they are never auto-inferred.
    pub(super) async fn register_canonical_fields(&self, schema: &Schema) -> FoldDbResult<()> {
        let field_names = schema.fields.as_deref().unwrap_or(&[]);

        // Phase 1: Identify new fields (read lock only)
        let new_fields: Vec<String> = {
            let fields = read_lock(&self.canonical_fields, "canonical_fields")?;
            field_names
                .iter()
                .filter(|f| !fields.contains_key(*f))
                .cloned()
                .collect()
        };

        if new_fields.is_empty() {
            return Ok(());
        }

        // Phase 2: resolve classification without LLM; build entries.
        let field_meta: Vec<(String, String, FieldValueType)> = new_fields
            .iter()
            .map(|f| {
                let desc = Self::build_field_description(f, schema);
                let ft = Self::infer_field_type(f, schema);
                (f.clone(), desc, ft)
            })
            .collect();

        let mut entries: Vec<(String, CanonicalField, Option<Vec<f32>>)> = Vec::new();

        for (field_name, desc, field_type) in &field_meta {
            let classification = schema
                .field_data_classifications
                .get(field_name)
                .cloned()
                .unwrap_or_else(|| {
                    DataClassification::new(INTERNAL, "general")
                        .expect("default internal/general classification is valid")
                });
            // Interest category: only if the schema declaration already carries one
            // (e.g. Schema.org overlay / explicit publish metadata). Never LLM-filled.
            let interest_category = schema.field_interest_categories.get(field_name).cloned();

            let embed_text = Self::build_embedding_text(desc);
            let embedding = self.embedder.embed_text(&embed_text).ok();

            entries.push((
                field_name.clone(),
                CanonicalField {
                    description: desc.clone(),
                    field_type: field_type.clone(),
                    version: 1,
                    classification: Some(classification),
                    interest_category,
                },
                embedding,
            ));
        }

        // Phase 3: Store under write locks. We collect the set that
        // needs to be persisted in the backend, drop the sync locks,
        // then await persistence — holding std::sync::RwLockWriteGuard
        // across an .await is unsound and also deadlocks the external
        // backend path.
        let to_persist: Vec<(String, CanonicalField)> = {
            let mut fields = write_lock(&self.canonical_fields, "canonical_fields")?;
            let mut embeddings = write_lock(
                &self.canonical_field_embeddings,
                "canonical_field_embeddings",
            )?;

            let mut collected = Vec::new();
            for (field_name, canonical, embedding) in entries {
                // Re-check in case another thread registered it between phase 1 and 3
                if fields.contains_key(&field_name) {
                    continue;
                }
                if let Some(vec) = embedding {
                    embeddings.insert(field_name.clone(), vec);
                }
                collected.push((field_name.clone(), canonical.clone()));
                fields.insert(field_name, canonical);
            }
            collected
        };

        self.persist_canonical_fields(&to_persist).await?;

        Ok(())
    }

    /// Canonicalize incoming field names against the global canonical field registry.
    /// Returns a rename map: incoming_field -> canonical_field.
    /// Uses the same bidirectional best-match + threshold approach as semantic_field_rename_map.
    /// Embeds "field_name: description" for richer semantic matching.
    pub(super) fn canonicalize_fields(
        &self,
        incoming_fields: &[String],
        schema: &Schema,
        mutation_mappers: &mut HashMap<String, String>,
    ) -> HashMap<String, String> {
        let Ok(canonical) = self.canonical_fields.read() else {
            return HashMap::new();
        };
        let Ok(embeddings) = self.canonical_field_embeddings.read() else {
            return HashMap::new();
        };

        if canonical.is_empty() {
            return HashMap::new();
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

        rename_map
    }

    /// Persist a canonical field to the active storage backend.
    pub(super) async fn persist_canonical_field(
        &self,
        name: &str,
        canonical: &CanonicalField,
    ) -> FoldDbResult<()> {
        self.storage
            .backend()
            .save_canonical_field(name, canonical)
            .await
    }

    /// Persist many canonical fields to the active storage backend in one
    /// batch: a single sled-tree pass locally, or one batched call (one
    /// blob RMW on the S3 backend) externally — instead of one full
    /// round-trip per field on every schema add.
    pub(super) async fn persist_canonical_fields(
        &self,
        fields: &[(String, CanonicalField)],
    ) -> FoldDbResult<()> {
        if fields.is_empty() {
            return Ok(());
        }
        self.storage.backend().save_canonical_fields(fields).await
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
    /// Called after canonicalization to propagate types from the registry to the schema.
    pub(super) fn apply_canonical_types(&self, schema: &mut Schema) {
        let Ok(fields) = self.canonical_fields.read() else {
            return;
        };
        let field_names: Vec<String> = schema.fields.clone().unwrap_or_default();
        Self::apply_canonical_field_map(&fields, &field_names, &mut schema.field_types, |c| {
            (c.field_type != FieldValueType::Any).then(|| c.field_type.clone())
        });
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
