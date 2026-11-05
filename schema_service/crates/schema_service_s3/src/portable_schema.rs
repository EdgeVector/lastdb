//! Exact shared schema revisions are never overwritten by catalog refresh.

use schema_types::{FoldDbError, FoldDbResult, PortableSchemaDefinition, PortableSchemaId};

use super::{is_precondition_failed, S3BlobPersistence};

impl S3BlobPersistence {
    pub(crate) async fn put_portable_schema(
        &self,
        definition: &PortableSchemaDefinition,
    ) -> FoldDbResult<()> {
        let id = definition.id();
        definition.verify(id).map_err(FoldDbError::Config)?;
        let key = portable_schema_key(id);
        let result = self
            .start_put(&key, Self::encode_json(&key, definition)?)
            .if_none_match("*")
            .send()
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(error) if is_precondition_failed(&error) => self
                .get_portable_schema(id)
                .await?
                .map(|_| ())
                .ok_or_else(|| {
                    FoldDbError::S3("portable schema disappeared after a put conflict".into())
                }),
            Err(error) => Err(Self::put_s3_error(&key, &error)),
        }
    }

    pub(crate) async fn get_portable_schema(
        &self,
        id: &PortableSchemaId,
    ) -> FoldDbResult<Option<PortableSchemaDefinition>> {
        let result = self
            .get_single_object::<PortableSchemaDefinition>(&portable_schema_key(id))
            .await?;
        if let Some(definition) = &result {
            definition.verify(id).map_err(FoldDbError::Config)?;
        }
        Ok(result)
    }
}

fn portable_schema_key(id: &PortableSchemaId) -> String {
    format!("portable-schemas/{id}.json")
}
