//! The real S3 definition adapter with an empty embedding cache. The checks
//! disable model inference and exercise no DynamoDB or app/release behavior.

use async_trait::async_trait;
use schema_service_core::{
    types::CanonicalField, CanonicalVocabularyWrite, DeclaredFieldRecord, ExternalSchemaPersistence,
};
use schema_service_s3::S3BlobPersistence;
use schema_types::{FoldDbResult, Schema};
use std::{collections::HashMap, sync::Arc};

pub struct DefinitionBackend(pub Arc<S3BlobPersistence>);

#[async_trait]
impl ExternalSchemaPersistence for DefinitionBackend {
    async fn save_schema(&self, schema: &Schema) -> FoldDbResult<()> {
        self.0.save_schema(schema).await
    }

    async fn save_schema_definitions(
        &self,
        schemas: &[Schema],
        fields: &[(String, CanonicalField)],
    ) -> FoldDbResult<()> {
        self.0.save_schema_definitions(schemas, fields).await
    }

    async fn save_schema_definitions_if_vocabulary(
        &self,
        schemas: &[Schema],
        vocabulary: &CanonicalVocabularyWrite,
    ) -> FoldDbResult<()> {
        self.0
            .save_schema_definitions_if_vocabulary(schemas, vocabulary)
            .await
    }

    async fn load_all_schemas(&self) -> FoldDbResult<HashMap<String, Schema>> {
        self.0.load_all_schemas().await
    }

    async fn save_canonical_field(&self, name: &str, field: &CanonicalField) -> FoldDbResult<()> {
        self.0.save_canonical_field(name, field).await
    }

    async fn save_canonical_fields(&self, fields: &[(String, CanonicalField)]) -> FoldDbResult<()> {
        self.0.save_canonical_fields(fields).await
    }

    async fn load_all_canonical_fields(&self) -> FoldDbResult<HashMap<String, CanonicalField>> {
        self.0.load_all_canonical_fields().await
    }

    async fn save_descriptive_name_embedding(
        &self,
        _key: &str,
        _value: &[f32],
    ) -> FoldDbResult<()> {
        panic!("disabled model must not write an embedding")
    }

    async fn load_descriptive_name_embeddings(&self) -> FoldDbResult<HashMap<String, Vec<f32>>> {
        Ok(HashMap::new())
    }

    async fn save_canonical_field_embedding(&self, _key: &str, _value: &[f32]) -> FoldDbResult<()> {
        panic!("disabled model must not write an embedding")
    }

    async fn load_canonical_field_embeddings(&self) -> FoldDbResult<HashMap<String, Vec<f32>>> {
        Ok(HashMap::new())
    }

    async fn save_declared_field_definitions(
        &self,
        record: &DeclaredFieldRecord,
        fields: &[(String, CanonicalField)],
        expected: Option<&DeclaredFieldRecord>,
    ) -> FoldDbResult<()> {
        self.0
            .save_declared_field_definitions(record, fields, expected)
            .await
    }

    async fn save_declared_field_definitions_if_vocabulary(
        &self,
        record: &DeclaredFieldRecord,
        vocabulary: &CanonicalVocabularyWrite,
        expected: Option<&DeclaredFieldRecord>,
    ) -> FoldDbResult<()> {
        self.0
            .save_declared_field_definitions_if_vocabulary(record, vocabulary, expected)
            .await
    }

    async fn load_all_declared_fields(&self) -> FoldDbResult<Vec<DeclaredFieldRecord>> {
        self.0.load_all_declared_fields().await
    }
}
