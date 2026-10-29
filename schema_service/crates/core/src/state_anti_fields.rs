use crate::embedder::cosine_similarity;
use schema_types::FoldDbError;
use schema_types::FoldDbResult;
use schema_types::Schema;

use super::state::SchemaServiceState;
use super::state_matching::FIELD_SIMILARITY_THRESHOLD;

/// Descriptions that name a container, not a concept. A field whose
/// description embedding scores at or above [`FIELD_SIMILARITY_THRESHOLD`]
/// against either sentence is rejected.
const ANTI_FIELD_DESCRIPTIONS: &[&str] = &[
    "the main textual body of a document or message",
    "the textual body of a message",
];

impl SchemaServiceState {
    fn anti_field_embeddings(&self) -> Vec<Vec<f32>> {
        if let Some(cached) = self.cached_anti_field_embeddings() {
            return cached;
        }
        let built: Vec<Vec<f32>> = ANTI_FIELD_DESCRIPTIONS
            .iter()
            .filter_map(|text| self.embedder.embed_text(text).ok())
            .collect();
        if built.len() == ANTI_FIELD_DESCRIPTIONS.len() {
            self.store_anti_field_embeddings(&built);
        }
        built
    }

    fn cached_anti_field_embeddings(&self) -> Option<Vec<Vec<f32>>> {
        self.anti_field_embedding_cache
            .read()
            .ok()
            .and_then(|guard| guard.clone())
    }

    fn store_anti_field_embeddings(&self, built: &[Vec<f32>]) {
        let Ok(mut slot) = self.anti_field_embedding_cache.write() else {
            return;
        };
        if slot.is_none() {
            *slot = Some(built.to_vec());
        }
    }

    pub(super) fn embedding_matches_anti_field(&self, embedding: &[f32]) -> bool {
        self.anti_field_embeddings()
            .iter()
            .any(|anti| cosine_similarity(embedding, anti) >= FIELD_SIMILARITY_THRESHOLD)
    }

    pub(super) fn reject_non_semantic_fields(
        &self,
        incoming_fields: &[String],
        schema: &Schema,
    ) -> FoldDbResult<()> {
        for field_name in incoming_fields {
            let description = Self::build_field_description(field_name, schema);
            let Some(embedding) = self.vocabulary_embedding(&description) else {
                continue;
            };
            if self.embedding_matches_anti_field(&embedding) {
                return Err(FoldDbError::Schema(format!(
                    "field '{field_name}' was rejected: its description matches a non-semantic field pattern"
                )));
            }
        }
        Ok(())
    }
}
