//! `ExternalSchemaPersistence` implementation for [`S3BlobPersistence`].

use super::*;

#[async_trait]
impl ExternalSchemaPersistence for S3BlobPersistence {
    async fn save_schema(&self, schema: &Schema) -> FoldDbResult<()> {
        let name = schema.name.clone();
        let schema = schema.clone();
        self.update_blob::<Schema, _>(SCHEMAS_KEY, move |blob| {
            blob.items.insert(name.clone(), schema.clone());
            Ok(())
        })
        .await
    }

    async fn save_schemas(&self, schemas: &[Schema]) -> FoldDbResult<()> {
        if schemas.is_empty() {
            return Ok(());
        }
        // One GET-modify-PUT for the whole batch — the default trait impl
        // would round-trip the entire schemas blob once per schema.
        let schemas = schemas.to_vec();
        self.update_blob::<Schema, _>(SCHEMAS_KEY, move |blob| {
            for schema in &schemas {
                blob.items.insert(schema.name.clone(), schema.clone());
            }
            Ok(())
        })
        .await
    }

    async fn load_all_schemas(&self) -> FoldDbResult<HashMap<String, Schema>> {
        let (blob, _) = self.get_blob::<Schema>(SCHEMAS_KEY).await?;
        Ok(blob.items)
    }

    async fn save_canonical_field(&self, name: &str, field: &CanonicalField) -> FoldDbResult<()> {
        let name = name.to_string();
        let field = field.clone();
        self.update_blob::<CanonicalField, _>(CANONICAL_FIELDS_KEY, move |blob| {
            blob.items.insert(name.clone(), field.clone());
            Ok(())
        })
        .await
    }

    async fn save_canonical_fields(&self, fields: &[(String, CanonicalField)]) -> FoldDbResult<()> {
        if fields.is_empty() {
            return Ok(());
        }
        // One GET-modify-PUT for the whole batch (see `save_schemas`).
        let fields = fields.to_vec();
        self.update_blob::<CanonicalField, _>(CANONICAL_FIELDS_KEY, move |blob| {
            for (name, field) in &fields {
                blob.items.insert(name.clone(), field.clone());
            }
            Ok(())
        })
        .await
    }

    async fn load_all_canonical_fields(&self) -> FoldDbResult<HashMap<String, CanonicalField>> {
        let (blob, _) = self
            .get_blob::<CanonicalField>(CANONICAL_FIELDS_KEY)
            .await?;
        Ok(blob.items)
    }

    async fn save_descriptive_name_embedding(
        &self,
        schema_hash: &str,
        embedding: &[f32],
    ) -> FoldDbResult<()> {
        self.put_embedding(EMBEDDING_KIND_DESCRIPTIVE, schema_hash, embedding)
            .await
    }

    async fn load_descriptive_name_embeddings(&self) -> FoldDbResult<HashMap<String, Vec<f32>>> {
        self.query_embeddings(EMBEDDING_KIND_DESCRIPTIVE).await
    }

    async fn save_canonical_field_embedding(
        &self,
        field_name: &str,
        embedding: &[f32],
    ) -> FoldDbResult<()> {
        self.put_embedding(EMBEDDING_KIND_CANONICAL, field_name, embedding)
            .await
    }

    async fn load_canonical_field_embeddings(&self) -> FoldDbResult<HashMap<String, Vec<f32>>> {
        self.query_embeddings(EMBEDDING_KIND_CANONICAL).await
    }

    async fn append_near_miss(&self, record: &NearMissRecord) -> FoldDbResult<()> {
        let id = record.registration_id.clone();
        let record = record.clone();
        self.update_blob::<NearMissRecord, _>(NEAR_MISSES_KEY, move |blob| {
            // HashMap-keyed by registration_id gives idempotency for free —
            // a second append with the same UUID is a no-op overwrite of
            // identical bytes.
            blob.items.insert(id.clone(), record.clone());
            Ok(())
        })
        .await
    }

    async fn load_all_near_misses(&self) -> FoldDbResult<Vec<NearMissRecord>> {
        let (blob, _) = self.get_blob::<NearMissRecord>(NEAR_MISSES_KEY).await?;
        Ok(blob.items.into_values().collect())
    }

    async fn save_app(&self, app: &AppRecord) -> FoldDbResult<()> {
        let app_id = app.app_id.clone();
        let app = app.clone();
        self.update_blob::<AppRecord, _>(APPS_KEY, move |blob| {
            // First-write-wins, immutable: never overwrite an existing
            // app_id. The in-memory registry already gated this, but the
            // ETag read-modify-write loop means a concurrent Lambda could
            // have committed a different owner since our cache load —
            // `or_insert` makes S3 the final arbiter.
            blob.items
                .entry(app_id.clone())
                .or_insert_with(|| app.clone());
            Ok(())
        })
        .await
    }

    async fn update_app(&self, app: &AppRecord) -> FoldDbResult<()> {
        let app_id = app.app_id.clone();
        let app = app.clone();
        self.update_blob::<AppRecord, _>(APPS_KEY, move |blob| {
            // Owner-authenticated mutation: the in-memory registry verified
            // signer == owner (and, for metadata updates, that display_name
            // is unchanged) before getting here. This path backs both
            // `update_app` (metadata) and `promote_app` (tier flip), so it
            // overwrites the mutable fields — `metadata` AND `tier` — while
            // preserving the immutable `owner_dev_pubkey` / `registered_at`.
            // Never create an entry here: an update/promote against an
            // unknown app_id is the caller's bug, already turned into a 404
            // by the in-memory layer. Preserving owner means even a
            // concurrent ETag loop can't replace a different owner's record.
            if let Some(existing) = blob.items.get_mut(&app_id) {
                existing.metadata = app.metadata.clone();
                existing.tier = app.tier;
            }
            Ok(())
        })
        .await
    }

    async fn load_all_apps(&self) -> FoldDbResult<HashMap<String, AppRecord>> {
        let (blob, _) = self.get_blob::<AppRecord>(APPS_KEY).await?;
        Ok(blob.items)
    }

    async fn save_declared_field(&self, record: &DeclaredFieldRecord) -> FoldDbResult<()> {
        let declaration_id = record.declaration_id.clone();
        let record = record.clone();
        self.update_blob::<DeclaredFieldRecord, _>(DECLARED_FIELDS_KEY, move |blob| {
            // Upsert under the ETag loop. Unlike `save_app`, this is NOT
            // first-write-wins: a re-declare legitimately adds fields or
            // grants. The declaration id is immutable and the owner is
            // verified before we get here, so the row can only grow — and a
            // concurrent Lambda writing the same declaration is writing the
            // same owner's data.
            blob.items.insert(declaration_id.clone(), record.clone());
            Ok(())
        })
        .await
    }

    async fn load_all_declared_fields(&self) -> FoldDbResult<Vec<DeclaredFieldRecord>> {
        let (blob, _) = self
            .get_blob::<DeclaredFieldRecord>(DECLARED_FIELDS_KEY)
            .await?;
        Ok(blob.items.into_values().collect())
    }
}
