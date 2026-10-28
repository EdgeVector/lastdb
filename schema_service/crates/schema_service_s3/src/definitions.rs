//! One conditional S3 image holds schemas, canonical fields and declarations.

use super::*;

const DEFINITIONS_KEY: &str = "definitions.json";
const REGISTRY_ITEM: &str = "registry";

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DefinitionSet {
    pub(super) schemas: HashMap<String, Schema>,
    pub(super) canonical_fields: HashMap<String, CanonicalField>,
    pub(super) declared_fields: HashMap<String, DeclaredFieldRecord>,
}

impl S3BlobPersistence {
    /// A new image requires an empty definition namespace. Never implicitly
    /// convert live two-blob stores while old-format writers can still run.
    pub(super) async fn load_definition_set(&self) -> FoldDbResult<DefinitionSet> {
        let (mut blob, etag) = self.get_blob::<DefinitionSet>(DEFINITIONS_KEY).await?;
        if blob.version != 1 || blob.items.len() > 1 {
            return Err(FoldDbError::Config(
                "unsupported definitions.json registry image".to_owned(),
            ));
        }
        if let Some(definitions) = blob.items.remove(REGISTRY_ITEM) {
            return Ok(definitions);
        }
        if etag.is_some() {
            return Err(FoldDbError::Config(
                "definitions.json is missing its registry image".to_owned(),
            ));
        }
        let ((schemas, _), (fields, _), (declarations, _)) = tokio::try_join!(
            self.get_blob::<Schema>(SCHEMAS_KEY),
            self.get_blob::<CanonicalField>(CANONICAL_FIELDS_KEY),
            self.get_blob::<DeclaredFieldRecord>(DECLARED_FIELDS_KEY),
        )?;
        if !schemas.items.is_empty() || !fields.items.is_empty() || !declarations.items.is_empty() {
            return Err(FoldDbError::Config(
                "atomic definitions require an empty namespace or explicit conversion; legacy blobs remain unchanged".to_owned(),
            ));
        }
        Ok(DefinitionSet::default())
    }

    async fn update_definition_set<F>(&self, modify: F) -> FoldDbResult<()>
    where
        F: Fn(&mut DefinitionSet) -> FoldDbResult<()>,
    {
        let initial = self.load_definition_set().await?;
        self.update_blob::<DefinitionSet, _>(DEFINITIONS_KEY, move |blob| {
            let definitions = blob
                .items
                .entry(REGISTRY_ITEM.to_owned())
                .or_insert_with(|| initial.clone());
            modify(definitions)
        })
        .await
    }

    pub(super) async fn commit_schema_definitions(
        &self,
        schemas: &[Schema],
        fields: &[(String, CanonicalField)],
    ) -> FoldDbResult<()> {
        if schemas.is_empty() && fields.is_empty() {
            return Ok(());
        }
        let schemas = schemas.to_vec();
        let fields = fields.to_vec();
        self.update_definition_set(move |definitions| {
            definitions.canonical_fields =
                schema_service_core::field_name_policy::canonical_fields_after_insert(
                    &definitions.canonical_fields,
                    &fields,
                )?;
            for schema in &schemas {
                definitions
                    .schemas
                    .insert(schema.name.clone(), schema.clone());
            }
            Ok(())
        })
        .await
    }

    pub(super) async fn commit_declaration_definitions(
        &self,
        record: &DeclaredFieldRecord,
        fields: &[(String, CanonicalField)],
        expected: Option<&DeclaredFieldRecord>,
    ) -> FoldDbResult<()> {
        let record = record.clone();
        let fields = fields.to_vec();
        let expected = expected.cloned();
        self.update_definition_set(move |definitions| {
            schema_service_core::declared_fields::validate_declaration_commit(
                &definitions.declared_fields,
                &record,
                expected.as_ref(),
            )?;
            definitions.canonical_fields =
                schema_service_core::field_name_policy::canonical_fields_after_insert(
                    &definitions.canonical_fields,
                    &fields,
                )?;
            definitions
                .declared_fields
                .insert(record.declaration_id.clone(), record.clone());
            Ok(())
        })
        .await
    }
}
