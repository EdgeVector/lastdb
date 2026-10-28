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
        self.save_canonical_fields(&[(name.to_owned(), field.clone())])
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
                schema_service_core::field_name_policy::validate_canonical_insert(
                    &blob.items,
                    name,
                    field,
                )?;
            }
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
        let existing = self.load_app(&app.app_id).await?;
        if let Some(existing) = existing {
            if existing.owner_dev_pubkey == app.owner_dev_pubkey {
                return Ok(());
            }
            return Err(FoldDbError::AppOwnerConflict {
                current_owner_dev_pubkey: existing.owner_dev_pubkey,
            });
        }
        let key = format!("apps/{}.json", app.app_id);
        let body = serde_json::to_vec(app)
            .map_err(|e| FoldDbError::Serialization(format!("serialize app: {e}")))?;
        let result = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(body.clone()))
            .content_type("application/json")
            .if_none_match("*")
            .send()
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(e) if is_precondition_failed(&e) => {
                let existing: Option<AppRecord> = self.get_single_object(&key).await?;
                match existing {
                    Some(existing) if existing.owner_dev_pubkey == app.owner_dev_pubkey => Ok(()),
                    Some(existing) => Err(FoldDbError::AppOwnerConflict {
                        current_owner_dev_pubkey: existing.owner_dev_pubkey,
                    }),
                    None => Err(FoldDbError::Config(
                        "app create conflict but not found on re-read".to_string(),
                    )),
                }
            }
            Err(e) => Err(FoldDbError::S3(format!(
                "put S3 object '{key}': {}",
                short_error(&e)
            ))),
        }
    }

    async fn update_app(&self, app: &AppRecord) -> FoldDbResult<()> {
        let key = format!("apps/{}.json", app.app_id);
        let mut existing: Option<AppRecord> = self.get_single_object(&key).await?;
        if existing.is_none() {
            let (blob, _) = self.get_blob::<AppRecord>(APPS_KEY).await?;
            existing = blob.items.get(&app.app_id).cloned();
            if existing.is_some() {
                tracing::info!(
                    "update_app: migrating legacy app '{}' to per-record storage",
                    app.app_id
                );
            }
        }
        let Some(mut existing) = existing else {
            return Err(FoldDbError::Config(format!(
                "app '{}' not found for update",
                app.app_id
            )));
        };
        existing.metadata = app.metadata.clone();
        existing.tier = app.tier;
        self.put_single_object(&key, &existing, false).await
    }

    async fn load_all_apps(&self) -> FoldDbResult<HashMap<String, AppRecord>> {
        let mut result = self.list_and_load_apps().await?;
        let (blob, _) = self.get_blob::<AppRecord>(APPS_KEY).await?;
        if !blob.items.is_empty() {
            let legacy_count = blob.items.len();
            let per_record_count = result.len();
            for (app_id, legacy_app) in blob.items {
                result.entry(app_id).or_insert(legacy_app);
            }
            tracing::info!(
                "schema_service_s3: loaded {} per-record apps + {} legacy apps (total {})",
                per_record_count,
                legacy_count,
                result.len()
            );
        }
        Ok(result)
    }

    async fn load_app(&self, app_id: &str) -> FoldDbResult<Option<AppRecord>> {
        let key = format!("apps/{app_id}.json");
        let app = self.get_single_object::<AppRecord>(&key).await?;
        if app.is_some() {
            return Ok(app);
        }
        let (blob, _) = self.get_blob::<AppRecord>(APPS_KEY).await?;
        Ok(blob.items.get(app_id).cloned())
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

    async fn save_release(
        &self,
        record: &schema_service_core::app_release::ReleaseRecord,
    ) -> FoldDbResult<()> {
        let key = format!("releases/{}.json", record.release_id);
        self.put_release_preserving_revocation(&key, record).await
    }

    async fn load_all_releases(
        &self,
    ) -> FoldDbResult<HashMap<String, schema_service_core::app_release::ReleaseRecord>> {
        self.list_and_load_releases().await
    }

    async fn save_channel(
        &self,
        record: &schema_service_core::app_release::ChannelRecord,
    ) -> FoldDbResult<()> {
        let key = format!("channels/{}/{}.json", record.app_id, record.channel);
        self.put_channel_with_generation_check(&key, record).await
    }

    async fn load_all_channels(
        &self,
    ) -> FoldDbResult<HashMap<String, schema_service_core::app_release::ChannelRecord>> {
        self.list_and_load_channels().await
    }

    async fn save_compat_row(
        &self,
        record: &schema_service_core::app_release::CompatRow,
    ) -> FoldDbResult<()> {
        let key = format!(
            "compat/{}/{}.json",
            record.release_id, record.contract_version
        );
        self.put_single_object(&key, record, true)
            .await
            .map_err(|e| {
                if e.to_string().contains("PreconditionFailed") || e.to_string().contains("412") {
                    FoldDbError::CompatRowExists {
                        release_id: record.release_id.clone(),
                        contract_version: record.contract_version.clone(),
                    }
                } else {
                    e
                }
            })
    }

    async fn load_compat_rows_for_release(
        &self,
        release_id: &str,
    ) -> FoldDbResult<Vec<schema_service_core::app_release::CompatRow>> {
        self.list_and_load_compat_rows(release_id).await
    }
}
