use super::*;

impl SchemaServiceState {
    /// Persist a schema to the storage backend.
    pub(crate) async fn persist_schema(&self, schema: &Schema) -> FoldDbResult<()> {
        let fields = self.checked_schema_vocabulary(std::slice::from_ref(schema))?;
        match &self.storage {
            SchemaStorage::External(_) => {
                self.commit_vocabulary(
                    &fields,
                    crate::vocabulary_commit::VocabularyCommit::Schemas(std::slice::from_ref(
                        schema,
                    )),
                )
                .await?;
                tracing::info!(
                target: "schema_service::schema",
                        "Schema '{}' persisted to external backend",
                        schema.name
                    );
            }
        }
        Ok(())
    }

    /// Persist many schemas to the storage backend in one batch: one
    /// batched call (one blob RMW on the S3 backend) instead of one full
    /// round-trip per schema.
    pub(crate) async fn persist_schemas(&self, schemas_to_persist: &[Schema]) -> FoldDbResult<()> {
        if schemas_to_persist.is_empty() {
            return Ok(());
        }
        let fields = self.checked_schema_vocabulary(schemas_to_persist)?;
        match &self.storage {
            SchemaStorage::External(_) => {
                self.commit_vocabulary(
                    &fields,
                    crate::vocabulary_commit::VocabularyCommit::Schemas(schemas_to_persist),
                )
                .await?;
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
    /// is unchanged; the backend is asked to preserve `owner_dev_pubkey` /
    /// `registered_at` while swapping `metadata` (the in-memory backend
    /// overwrites the record in place).
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
