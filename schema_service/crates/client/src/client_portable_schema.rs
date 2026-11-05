//! Read and pin portable schema versions by exact identity.

use schema_service_core::portable_schema::PinPortableSchemaRequest;
use schema_types::{FoldDbError, FoldDbResult, PortableSchemaDefinition, PortableSchemaId};

use super::{
    reqwest_error_is_retryable, status_is_retryable, with_retries, RetryError, SchemaServiceClient,
};

impl SchemaServiceClient {
    /// Read a catalog definition by its exact ID and prepare an immutable pin.
    /// The caller can bind its local schema before sending the pin request.
    pub async fn prepare_portable_schema(
        &self,
        catalog_id: &str,
    ) -> FoldDbResult<PortableSchemaDefinition> {
        PortableSchemaDefinition::validate_catalog_id(catalog_id).map_err(FoldDbError::Config)?;
        let schema = self.get_schema(catalog_id).await?.schema;
        PortableSchemaDefinition::from_catalog(catalog_id, schema).map_err(FoldDbError::Config)
    }

    /// Store only the exact prepared revision. Retries carry the same ID.
    pub async fn pin_portable_schema(
        &self,
        candidate: &PortableSchemaDefinition,
    ) -> FoldDbResult<PortableSchemaDefinition> {
        let id = candidate.id();
        candidate.verify(id).map_err(FoldDbError::Config)?;
        let request = PinPortableSchemaRequest {
            catalog_id: candidate.catalog_id().to_owned(),
        };
        let url = self.portable_schema_url(id);
        let pinned = with_retries(|| async {
            let response =
                observability::propagation::inject_w3c(self.client.put(&url).json(&request))
                    .send()
                    .await
                    .map_err(|error| {
                        RetryError::classify(
                            reqwest_error_is_retryable(&error),
                            FoldDbError::Config("portable schema pin request failed".into()),
                        )
                    })?;
            let status = response.status();
            if !status.is_success() {
                return Err(RetryError::classify(
                    status_is_retryable(status),
                    FoldDbError::Config(format!("portable schema pin returned {status}")),
                ));
            }
            response
                .json::<PortableSchemaDefinition>()
                .await
                .map_err(|_| {
                    RetryError::Permanent(FoldDbError::Config(
                        "invalid portable schema pin response".into(),
                    ))
                })
        })
        .await?;
        pinned.verify(id).map_err(FoldDbError::Config)?;
        Ok(pinned)
    }

    /// A replay must stop on an unavailable, missing, or mismatched revision.
    pub async fn get_portable_schema(
        &self,
        id: &PortableSchemaId,
    ) -> FoldDbResult<PortableSchemaDefinition> {
        let definition: PortableSchemaDefinition = self
            .get_json(&self.portable_schema_url(id), "portable schema")
            .await?;
        definition.verify(id).map_err(FoldDbError::Config)?;
        Ok(definition)
    }

    fn portable_schema_url(&self, id: &PortableSchemaId) -> String {
        format!("{}/v1/schema-versions/{id}", self.base_url)
    }
}
