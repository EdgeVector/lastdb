//! Inject a failed commit at the persistence trait boundary. Read paths use the
//! real memory backend; no alternative registry algorithm is implemented here.
use super::support::*;
use async_trait::async_trait;
use schema_service_core::{
    types::CanonicalField, DeclaredFieldRecord, ExternalSchemaPersistence,
    InMemorySchemaPersistence,
};
use schema_types::{FieldValueType, FoldDbError, FoldDbResult, Schema};
use std::{collections::HashMap, sync::Arc};

#[derive(Default)]
struct FailedCommit(InMemorySchemaPersistence);

fn unavailable() -> FoldDbResult<()> {
    Err(FoldDbError::Config(
        "injected definition commit failure".into(),
    ))
}

fn assert_commit_failed(error: impl std::fmt::Debug) {
    assert!(
        format!("{error:?}").contains("injected definition commit failure"),
        "a failure before persistence does not exercise commit rollback: {error:?}"
    );
}

#[async_trait]
impl ExternalSchemaPersistence for FailedCommit {
    async fn save_schema(&self, _schema: &Schema) -> FoldDbResult<()> {
        unavailable()
    }

    async fn save_schema_definitions(
        &self,
        _schemas: &[Schema],
        _fields: &[(String, CanonicalField)],
    ) -> FoldDbResult<()> {
        unavailable()
    }

    async fn load_all_schemas(&self) -> FoldDbResult<HashMap<String, Schema>> {
        self.0.load_all_schemas().await
    }

    async fn save_canonical_field(&self, _name: &str, _field: &CanonicalField) -> FoldDbResult<()> {
        unavailable()
    }

    async fn save_canonical_fields(
        &self,
        _fields: &[(String, CanonicalField)],
    ) -> FoldDbResult<()> {
        unavailable()
    }

    async fn load_all_canonical_fields(&self) -> FoldDbResult<HashMap<String, CanonicalField>> {
        self.0.load_all_canonical_fields().await
    }

    async fn save_descriptive_name_embedding(&self, key: &str, value: &[f32]) -> FoldDbResult<()> {
        self.0.save_descriptive_name_embedding(key, value).await
    }

    async fn load_descriptive_name_embeddings(&self) -> FoldDbResult<HashMap<String, Vec<f32>>> {
        self.0.load_descriptive_name_embeddings().await
    }

    async fn save_canonical_field_embedding(&self, key: &str, value: &[f32]) -> FoldDbResult<()> {
        self.0.save_canonical_field_embedding(key, value).await
    }

    async fn load_canonical_field_embeddings(&self) -> FoldDbResult<HashMap<String, Vec<f32>>> {
        self.0.load_canonical_field_embeddings().await
    }

    async fn save_declared_field_definitions(
        &self,
        _record: &DeclaredFieldRecord,
        _fields: &[(String, CanonicalField)],
        _expected: Option<&DeclaredFieldRecord>,
    ) -> FoldDbResult<()> {
        unavailable()
    }

    async fn load_all_declared_fields(&self) -> FoldDbResult<Vec<DeclaredFieldRecord>> {
        self.0.load_all_declared_fields().await
    }
}

#[test]
fn failed_commit_cannot_publish_vocabulary_schema_or_declaration_caches() {
    run(async {
        let backend = Arc::new(FailedCommit::default());
        let state = state(backend.clone()).await;
        let definition = field(
            "Parcel identifier assigned by the carrier",
            FieldValueType::String,
        );
        assert_commit_failed(
            state
                .add_canonical_field("parcel_identifier", definition)
                .await
                .err()
                .expect("canonical commit must fail"),
        );
        assert_commit_failed(
            state
                .add_schema(schema("parcel-app", false), HashMap::new())
                .await
                .err()
                .expect("schema commit must fail"),
        );
        assert_commit_failed(
            state
                .declare_field(&declaration("parcel-app", "delivery"))
                .await
                .err()
                .expect("declaration commit must fail"),
        );
        assert!(state
            .canonical_fields
            .read()
            .expect("vocabulary")
            .is_empty());
        assert!(state.schemas.read().expect("schemas").is_empty());
        assert!(state
            .declared_fields
            .read()
            .expect("declarations")
            .is_empty());
        assert!(backend
            .load_all_canonical_fields()
            .await
            .expect("durable vocabulary")
            .is_empty());
        assert!(backend
            .load_all_schemas()
            .await
            .expect("durable schemas")
            .is_empty());
        assert!(backend
            .load_all_declared_fields()
            .await
            .expect("durable declarations")
            .is_empty());
    });
}
