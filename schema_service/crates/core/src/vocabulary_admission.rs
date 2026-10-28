//! One admission policy for ordinary schemas, declared fields, seeds and imports.

use crate::embedder::cosine_similarity;
use crate::field_name_policy::{self, NAME_POLICY_VERSION};
use crate::lock_helpers::read_lock;
use crate::state::SchemaServiceState;
use crate::types::CanonicalField;
use crate::vocabulary_matching::{name_has_definition_support, reuse_error, semantic_candidate};
use schema_types::{FoldDbError, FoldDbResult, Schema};
use std::collections::HashMap;

pub(crate) const EMBEDDING_CACHE_LIMIT: usize = 4096;

impl SchemaServiceState {
    /// Validate a complete batch without a durable write or authority override.
    pub(crate) fn checked_vocabulary_batch(
        &self,
        fields: &[(String, CanonicalField)],
    ) -> FoldDbResult<Vec<(String, CanonicalField)>> {
        let mut candidates = read_lock(&self.canonical_fields, "canonical_fields")?.clone();
        let ordered = self.provisional_vocabulary_batch(fields)?;
        let mut vectors = HashMap::new();
        for (name, field) in &ordered {
            self.check_field_meaning(name, field)?;
            if !candidates.contains_key(name) {
                let mut possible: Vec<_> = candidates.iter().collect();
                possible.sort_by(|left, right| left.0.cmp(right.0));
                for (other_name, other) in possible {
                    if semantic_candidate(name, field, other_name, other)
                        && self.same_field_embedding(field, other, &mut vectors)
                    {
                        return Err(reuse_error(name, other_name));
                    }
                }
                candidates.insert(name.clone(), field.clone());
            }
        }
        Ok(ordered)
    }

    /// Cheap request checks; safe before ordinary authentication and quotas.
    /// This creates no definitions and performs no model inference or memo write.
    pub(crate) fn provisional_vocabulary_batch(
        &self,
        fields: &[(String, CanonicalField)],
    ) -> FoldDbResult<Vec<(String, CanonicalField)>> {
        let mut candidates = read_lock(&self.canonical_fields, "canonical_fields")?.clone();
        let mut ordered = fields.to_vec();
        ordered.sort_by(|left, right| left.0.cmp(&right.0));
        for (name, field) in &mut ordered {
            field.name_policy_version = NAME_POLICY_VERSION;
            field_name_policy::validate_canonical_insert(&candidates, name, field)?;
            candidates.insert(name.clone(), field.clone());
        }
        Ok(ordered)
    }

    fn check_field_meaning(&self, name: &str, field: &CanonicalField) -> FoldDbResult<()> {
        let vector = self.vocabulary_embedding(&field.description);
        if !name_has_definition_support(name, &field.description) {
            let name_vector = self.vocabulary_embedding(&name.replace('_', " "));
            if !matches!((&name_vector, &vector), (Some(name), Some(description))
                if cosine_similarity(name, description) >= 0.3)
            {
                return Err(FoldDbError::Config(format!(
                    "invalid_canonical_meaning: '{name}' needs a name that corresponds to its description"
                )));
            }
        }
        if vector.is_some_and(|vector| self.embedding_matches_anti_field(&vector)) {
            return Err(FoldDbError::Config(format!(
                "invalid_canonical_description: '{name}' describes a generic text container"
            )));
        }
        Ok(())
    }

    pub(crate) fn vocabulary_embedding(&self, text: &str) -> Option<Vec<f32>> {
        if let Some(vector) = self.native_resolve_embeddings.read().ok()?.get(text) {
            return Some(vector.clone());
        }
        let vector = self.embedder.embed_text(text).ok()?;
        let mut cache = self.native_resolve_embeddings.write().ok()?;
        if cache.len() < EMBEDDING_CACHE_LIMIT {
            cache.insert(text.to_owned(), vector.clone());
        }
        Some(vector)
    }

    fn same_field_embedding(
        &self,
        left: &CanonicalField,
        right: &CanonicalField,
        vectors: &mut HashMap<String, Option<Vec<f32>>>,
    ) -> bool {
        for field in [left, right] {
            vectors
                .entry(field.description.clone())
                .or_insert_with(|| self.vocabulary_embedding(&field.description));
        }
        match (&vectors[&left.description], &vectors[&right.description]) {
            (Some(left), Some(right)) => cosine_similarity(left, right) >= 0.96,
            // Exact normalized meanings still fail through the durable string rule.
            // No semantic equivalence is inferred without vector evidence.
            _ => false,
        }
    }

    pub(crate) fn schema_vocabulary_candidates(
        &self,
        schema: &Schema,
    ) -> FoldDbResult<Vec<(String, CanonicalField)>> {
        let vocabulary = read_lock(&self.canonical_fields, "canonical_fields")?;
        field_name_policy::schema_field_names(schema)
            .into_iter()
            .map(|name| {
                if let Some(existing) = vocabulary.get(name) {
                    return Ok((name.clone(), existing.clone()));
                }
                let description =
                    schema
                        .field_descriptions
                        .get(name)
                        .cloned()
                        .ok_or_else(|| {
                            FoldDbError::Config(format!(
                                "invalid_canonical_description: '{name}' requires a description"
                            ))
                        })?;
                let field_type = schema.field_types.get(name).cloned().ok_or_else(|| {
                    FoldDbError::Config(format!(
                        "invalid_canonical_type: '{name}' requires an explicit type"
                    ))
                })?;
                Ok((
                    name.clone(),
                    CanonicalField {
                        description,
                        field_type,
                        version: schema.field_versions.get(name).copied().unwrap_or(1),
                        name_policy_version: NAME_POLICY_VERSION,
                        classification: schema.field_data_classifications.get(name).cloned(),
                        interest_category: schema.field_interest_categories.get(name).cloned(),
                    },
                ))
            })
            .collect()
    }

    pub(crate) async fn admit_declared_vocabulary(
        &self,
        inputs: &[crate::declared_fields::DeclareFieldInput],
    ) -> FoldDbResult<()> {
        let fields = {
            let vocabulary = read_lock(&self.canonical_fields, "canonical_fields")?;
            inputs
                .iter()
                .map(|input| {
                    let field = vocabulary
                        .get(&input.name)
                        .cloned()
                        .unwrap_or(CanonicalField {
                            description: input.description.clone(),
                            field_type: input.field_type.clone(),
                            version: input.version,
                            name_policy_version: NAME_POLICY_VERSION,
                            classification: None,
                            interest_category: None,
                        });
                    (input.name.clone(), field)
                })
                .collect::<Vec<_>>()
        };
        let provisional = self
            .checked_vocabulary_batch(&fields)?
            .into_iter()
            .collect();
        for input in inputs {
            field_name_policy::validate_declared_concept(
                &provisional,
                &input.name,
                &input.field_type,
                &input.description,
                input.version,
            )?;
        }
        self.add_canonical_fields(&fields).await
    }

    /// Called at schema persistence, after logical rejection and reuse guards.
    pub(crate) async fn admit_schema_vocabularies(&self, schemas: &[Schema]) -> FoldDbResult<()> {
        let mut fields = HashMap::new();
        for schema in schemas {
            schema.validate_definition().map_err(FoldDbError::Config)?;
            let normalized = self.normalized_schema_field_vocabulary(schema)?;
            for (name, field) in self.schema_vocabulary_candidates(&normalized)? {
                if fields.get(&name).is_some_and(|stored| stored != &field) {
                    return Err(FoldDbError::Config(format!(
                        "canonical_field_conflict: '{name}' has conflicting definitions in the schema batch"
                    )));
                }
                fields.insert(name, field);
            }
        }
        self.add_canonical_fields(&fields.into_iter().collect::<Vec<_>>())
            .await
    }
}
