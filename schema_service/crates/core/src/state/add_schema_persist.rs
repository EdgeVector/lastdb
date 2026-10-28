use super::*;

impl SchemaServiceState {
    /// Persist a new canonical, update the in-memory cache and
    /// descriptive_name index/embedding, register its fields, and propagate
    /// canonical types, classifications, and interest categories.
    pub(super) async fn persist_new_canonical(
        &self,
        schema: &mut Schema,
        schema_name: &str,
    ) -> FoldDbResult<()> {
        self.validate_proposal_field_names(schema)?;
        // Resolve inherited types before any durable write or cache publication.
        self.apply_canonical_types(schema)?;
        let schema_name = schema_name.to_string();
        // Persist to storage backend
        self.persist_schema(schema).await?;

        // Insert into in-memory cache and update descriptive_name index atomically
        // to prevent a window where the schema exists but isn't indexed.
        {
            let mut schemas = write_lock(&self.schemas, "schemas")?;
            schemas.insert(schema_name.clone(), schema.clone());
        }

        if let Some(ref desc_name) = schema.descriptive_name {
            // Index/embedding writes are keyed by the namespaced form
            // (app_identity v3.1, Lane B2b) so `fbrain/Project` and a seed
            // `Project` get distinct entries instead of overwriting each other.
            let index_key = descriptive_name_key(schema.owner_app_id.as_deref(), desc_name);
            // Update the index under a scope-bounded lock so the
            // write guard is guaranteed-dropped before the async
            // persist below (Rust's future-state-machine analysis
            // doesn't always accept explicit `drop()` for liveness,
            // and holding a `RwLockWriteGuard<HashMap<String, String>>`
            // across an await violates `Send`).
            {
                let mut index = write_lock(&self.descriptive_name_index, "descriptive_name_index")?;
                index.insert(index_key.clone(), schema_name.clone());
            }

            // Cache embedding for new descriptive_name AND persist it
            // to the S3 blob so the next cold start finds it without
            // recomputing. Best-effort persistence — a blob write
            // failure leaves the in-memory cache ahead of S3 (fine,
            // the blob is a cache) and `warm-embeddings` can fix it.
            if let Ok(vec) = self.embedder.embed_text(desc_name) {
                {
                    if let Ok(mut embeddings) = self.descriptive_name_embeddings.write() {
                        embeddings.insert(index_key.clone(), vec.clone());
                    }
                }
                let backend = self.storage.backend();
                if let Err(e) = backend
                    .save_descriptive_name_embedding(&schema_name, &vec)
                    .await
                {
                    tracing::warn!(
                    target: "schema_service::schema",
                                    "Failed to persist descriptive_name embedding for schema '{}' ('{}'): {} — \
                                     next cold start will miss this entry until warm-embeddings runs",
                                    schema_name,
                                    desc_name,
                                    e
                                );
                }
            }
        }

        // Register new fields as canonical for future schema proposals.
        // Fails if classification cannot be determined (no ANTHROPIC_API_KEY for new fields).
        self.register_canonical_fields(schema).await?;

        // Propagate classifications and interest categories for the new fields.
        self.apply_canonical_classifications(schema);
        self.apply_canonical_interest_categories(schema);

        Ok(())
    }
}
