use super::*;

impl SchemaServiceState {
    /// Persist a schema to the storage backend.
    pub(crate) async fn persist_schema(&self, schema: &Schema) -> FoldDbResult<()> {
        match &self.storage {
            SchemaStorage::External(backend) => {
                backend.save_schema(schema).await?;
                tracing::info!(
                target: "schema_service::schema",
                        "Schema '{}' persisted to external backend",
                        schema.name
                    );
            }
        }
        Ok(())
    }

    /// Persist many schemas to the storage backend in one batch: a single
    /// sled pass with one flush locally, or one batched call (one blob RMW
    /// on the S3 backend) externally — instead of one full round-trip per
    /// schema.
    pub(crate) async fn persist_schemas(&self, schemas_to_persist: &[Schema]) -> FoldDbResult<()> {
        if schemas_to_persist.is_empty() {
            return Ok(());
        }
        match &self.storage {
            SchemaStorage::External(backend) => {
                backend.save_schemas(schemas_to_persist).await?;
                tracing::info!(
                target: "schema_service::schema",
                        "{} schemas persisted to external backend",
                        schemas_to_persist.len()
                    );
            }
        }
        Ok(())
    }

    /// Persist an owner-authenticated metadata update to an existing app
    /// (app_identity v3.1, `PUT /v1/apps/{id}`). The in-memory registry
    /// already verified the signer is the owner and that `display_name`
    /// is unchanged; sled overwrites in place and the External backend
    /// is asked to preserve `owner_dev_pubkey` / `registered_at` while
    /// swapping `metadata`.
    pub(crate) async fn persist_app_update(&self, app: &AppRecord) -> FoldDbResult<()> {
        match &self.storage {
            SchemaStorage::External(backend) => {
                backend.update_app(app).await?;
                tracing::info!(
                    target: "schema_service::app_identity",
                    app_id = %app.app_id,
                    "App metadata update persisted to external backend"
                );
            }
        }
        Ok(())
    }

    /// Persist a single app registration (app_identity v3.1, Lane B2b).
    /// First-write-wins is enforced in the in-memory registry before this
    /// is called; on the External backend `save_app` is additionally
    /// insert-if-absent so a cross-instance race can't clobber a winner.
    pub(crate) async fn persist_app(&self, app: &AppRecord) -> FoldDbResult<()> {
        match &self.storage {
            SchemaStorage::External(backend) => {
                backend.save_app(app).await?;
                tracing::info!(
                    target: "schema_service::app_identity",
                    app_id = %app.app_id,
                    "App registration persisted to external backend"
                );
            }
        }
        Ok(())
    }
}
